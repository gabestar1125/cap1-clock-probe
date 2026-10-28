"""Evidence publication — the probe-proven commit-back pattern,
abstracted. A Publisher maps (branch, relpath) -> bytes with
write-once semantics for evidence files: re-publishing identical bytes
is an idempotent no-op; publishing different bytes at an evidence path
is an integrity error, never an overwrite. heartbeat.json is the sole
mutable path (it is a liveness signal, not evidence).

- DirPublisher: the remote is a directory tree (lab/offline mirror of
  the branch layout; used by every offline test).
- GitPublisher: the remote is a git repository; each branch is a real
  branch, created orphan on first publication, committed and pushed.
  Exercised offline against local bare repositories; on GitHub it is
  the rehearsal/live mechanism.
"""
import os
import subprocess

MUTABLE_PATHS = {"heartbeat.json"}


class PublicationError(Exception):
    pass


class IntegrityError(PublicationError):
    pass


def _is_mutable(relpath):
    return os.path.basename(relpath) in MUTABLE_PATHS


class DirPublisher:
    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self.bytes_published = 0

    def _full(self, branch, relpath):
        return os.path.join(self.root, branch, relpath)

    def publish(self, branch, relpath, data: bytes):
        full = self._full(branch, relpath)
        if os.path.exists(full) and not _is_mutable(relpath):
            with open(full, "rb") as f:
                if f.read() == data:
                    return "IDEMPOTENT"
            raise IntegrityError(
                f"write-once violation at {branch}/{relpath}")
        os.makedirs(os.path.dirname(full), exist_ok=True)
        tmp = full + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, full)
        self.bytes_published += len(data)
        return "PUBLISHED"

    def read(self, branch, relpath):
        full = self._full(branch, relpath)
        if not os.path.exists(full):
            return None
        with open(full, "rb") as f:
            return f.read()

    def exists(self, branch, relpath):
        return os.path.exists(self._full(branch, relpath))

    def list(self, branch, prefix=""):
        base = os.path.join(self.root, branch, prefix)
        out = []
        if not os.path.isdir(base):
            return out
        for root, _dirs, files in os.walk(base):
            for name in files:
                out.append(os.path.relpath(
                    os.path.join(root, name), os.path.join(self.root,
                                                           branch)))
        return sorted(out)


class GitPublisher:
    """Publishes onto real git branches from a dedicated clone the
    publisher owns outright (publisher_clone_dir). Single-writer per
    branch is a protocol property (the atomic claim), so a porcelain
    checkout/commit/push cycle per publication is safe. Exercised
    offline against local bare repositories; on GitHub the clone's
    origin is the repository HTTPS URL with the job token."""

    def __init__(self, clone_dir):
        self.repo = clone_dir
        self.bytes_published = 0
        self._env = dict(
            os.environ, GIT_AUTHOR_NAME="cap1-supervisor",
            GIT_AUTHOR_EMAIL="cap1@localhost",
            GIT_COMMITTER_NAME="cap1-supervisor",
            GIT_COMMITTER_EMAIL="cap1@localhost")
        self._branch = None

    @classmethod
    def clone(cls, origin, clone_dir):
        p = subprocess.run(["git", "clone", origin, clone_dir],
                           capture_output=True, text=True, timeout=300)
        if p.returncode != 0:
            raise PublicationError("clone: %s" % p.stderr.strip())
        return cls(clone_dir)

    def _git(self, *args, check=True):
        p = subprocess.run(["git", "-C", self.repo] + list(args),
                           capture_output=True, text=True, timeout=120,
                           env=self._env)
        if check and p.returncode != 0:
            raise PublicationError("git %s: %s" % (
                args[0], p.stderr.strip()))
        return p

    def _remote_tip(self, branch):
        p = self._git("ls-remote", "origin", "refs/heads/%s" % branch,
                      check=False)
        out = p.stdout.strip()
        return out.split()[0] if out else None

    def _checkout(self, branch):
        if self._branch == branch:
            return
        self._git("fetch", "origin", check=False)
        local = "pub/%s" % branch
        if self._remote_tip(branch) is not None:
            self._git("checkout", "-B", local,
                      "refs/remotes/origin/%s" % branch)
        else:
            self._git("checkout", "--orphan", local)
            self._git("rm", "-rf", "--quiet", ".", check=False)
        self._branch = branch

    def read(self, branch, relpath):
        tip = self._remote_tip(branch)
        if tip is None:
            return None
        self._git("fetch", "origin", "refs/heads/%s" % branch,
                  check=False)
        p = self._git("show", "%s:%s" % (tip, relpath), check=False)
        if p.returncode != 0:
            return None
        return p.stdout.encode()

    def exists(self, branch, relpath):
        return self.read(branch, relpath) is not None

    def list(self, branch, prefix=""):
        tip = self._remote_tip(branch)
        if tip is None:
            return []
        self._git("fetch", "origin", "refs/heads/%s" % branch,
                  check=False)
        p = self._git("ls-tree", "-r", "--name-only", tip)
        return sorted(x for x in p.stdout.splitlines()
                      if x.startswith(prefix))

    def publish(self, branch, relpath, data: bytes):
        existing = self.read(branch, relpath)
        if existing is not None and not _is_mutable(relpath):
            if existing == data:
                return "IDEMPOTENT"
            raise IntegrityError(
                f"write-once violation at {branch}/{relpath}")
        self._checkout(branch)
        full = os.path.join(self.repo, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(data)
        self._git("add", relpath)
        status = self._git("status", "--porcelain", check=False).stdout
        if status.strip():
            self._git("commit", "--quiet", "-m",
                      "cap1 publish %s" % relpath)
            push = self._git("push", "origin",
                             "HEAD:refs/heads/%s" % branch, check=False)
            if push.returncode != 0:
                raise PublicationError("push %s: %s" % (
                    branch, push.stderr.strip()))
        self.bytes_published += len(data)
        return "PUBLISHED"


class FailingPublisher:
    """Failure-injection wrapper: raises PublicationError on the next
    `fail_next` publish calls (or all calls while `always` is set)."""

    def __init__(self, inner):
        self.inner = inner
        self.fail_next = 0
        self.always = False
        self.attempts = 0

    @property
    def bytes_published(self):
        return self.inner.bytes_published

    def publish(self, branch, relpath, data):
        self.attempts += 1
        if self.always or self.fail_next > 0:
            if self.fail_next > 0:
                self.fail_next -= 1
            raise PublicationError("injected publication failure")
        return self.inner.publish(branch, relpath, data)

    def read(self, branch, relpath):
        return self.inner.read(branch, relpath)

    def exists(self, branch, relpath):
        return self.inner.exists(branch, relpath)

    def list(self, branch, prefix=""):
        return self.inner.list(branch, prefix)
