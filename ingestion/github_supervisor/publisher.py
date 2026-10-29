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
  A rejected push is retried a bounded number of times with resync
  (see GitPublisher.publish): publications are idempotent and
  content-addressed, so a resync-and-retry cannot corrupt evidence —
  it can only discover that the content already landed, that a
  write-once path was violated meanwhile, or land the same bytes on
  the current tip. Exercised offline against local bare repositories;
  on GitHub it is the rehearsal/live mechanism.
"""
import os
import subprocess
import time

MUTABLE_PATHS = {"heartbeat.json"}


class PublicationError(Exception):
    """Publication failure with durable-safe metadata (Fix B).

    attempts:   number of the push attempt in progress when the
                error was raised (1 for a publisher that tries once).
    exhausted:  True only when raised after the push-attempt loop
                was exhausted without success.
    last_error: the underlying error text (e.g. final push stderr);
                callers must sanitize before persisting it."""

    def __init__(self, message="", *, attempts=0, exhausted=False,
                 last_error=None):
        super().__init__(message)
        self.attempts = int(attempts)
        self.exhausted = bool(exhausted)
        self.last_error = str(last_error) if last_error is not None \
            else str(message)


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
            msg = f"write-once violation at {branch}/{relpath}"
            raise IntegrityError(msg, attempts=1, exhausted=False,
                                 last_error=msg)
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
    origin is the repository HTTPS URL with the job token.

    Push resilience: a single rejected push used to be fatal to the
    publishing segment (observed on GitHub, 2026-10-29 run #16: the
    server answered a handoff publication with "fatal error in
    commit_refs" and the uncaught PublicationError killed the job
    after it had captured its full window — no final.json, no
    successor dispatch). publish() therefore retries a rejected push
    a bounded number of times (max_push_attempts total attempts,
    push_backoff_s between them), resynchronising to the current
    remote tip and re-evaluating the write-once state before every
    attempt, so a push that secretly landed resolves as IDEMPOTENT
    and a write-once violation that landed meanwhile still raises
    IntegrityError instead of being overwritten."""

    def __init__(self, clone_dir, max_push_attempts=3,
                 push_backoff_s=(1.0, 3.0)):
        self.repo = clone_dir
        self.bytes_published = 0
        self.max_push_attempts = max(1, int(max_push_attempts))
        self.push_backoff_s = tuple(push_backoff_s)
        self._sleep = time.sleep      # injectable: tests zero/hook it
        self._env = dict(
            os.environ, GIT_AUTHOR_NAME="cap1-supervisor",
            GIT_AUTHOR_EMAIL="cap1@localhost",
            GIT_COMMITTER_NAME="cap1-supervisor",
            GIT_COMMITTER_EMAIL="cap1@localhost")
        self._branch = None

    @classmethod
    def clone(cls, origin, clone_dir, **kw):
        p = subprocess.run(["git", "clone", origin, clone_dir],
                           capture_output=True, text=True, timeout=300)
        if p.returncode != 0:
            raise PublicationError("clone: %s" % p.stderr.strip())
        return cls(clone_dir, **kw)

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

    def _checkout(self, branch, force=False):
        if self._branch == branch and not force:
            return
        self._git("fetch", "origin", check=False)
        local = "pub/%s" % branch
        if self._remote_tip(branch) is not None:
            self._git("checkout", "-B", local,
                      "refs/remotes/origin/%s" % branch)
        elif self._branch == branch:
            # Resync retry and the branch still does not exist
            # remotely: our local pub branch already carries the
            # commit an earlier attempt built — keep it; the retry
            # simply re-pushes it.
            return
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

    def _backoff_for(self, attempt):
        """Sleep duration before attempt `attempt` (>= 2)."""
        if not self.push_backoff_s:
            return 0.0
        idx = min(attempt - 2, len(self.push_backoff_s) - 1)
        return self.push_backoff_s[idx]

    def publish(self, branch, relpath, data: bytes):
        mutable = _is_mutable(relpath)
        last_err = ""
        for attempt in range(1, self.max_push_attempts + 1):
            if attempt > 1:
                self._sleep(self._backoff_for(attempt))
            try:
                # (Re-)evaluate against the CURRENT remote state
                # before every attempt: on attempt 1 this is the
                # original write-once pre-check; on a retry it
                # resolves the ambiguity of a rejected push —
                # identical bytes that landed anyway are IDEMPOTENT,
                # different bytes at a write-once path are an
                # IntegrityError, never an overwrite.
                existing = self.read(branch, relpath)
                if existing is not None and not mutable:
                    if existing == data:
                        return "IDEMPOTENT"
                    msg = (f"write-once violation at "
                           f"{branch}/{relpath}")
                    raise IntegrityError(msg, attempts=attempt,
                                         exhausted=False,
                                         last_error=msg)
                self._checkout(branch, force=attempt > 1)
                full = os.path.join(self.repo, relpath)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "wb") as f:
                    f.write(data)
                self._git("add", relpath)
                status = self._git("status", "--porcelain",
                                   check=False).stdout
                if status.strip():
                    self._git("commit", "--quiet", "-m",
                              "cap1 publish %s" % relpath)
                # Push even when nothing new was committed: after a
                # resync that kept an unpushed local commit (branch
                # not yet on the remote), HEAD itself is the payload.
                # An up-to-date push is a harmless no-op.
                push = self._git(
                    "push", "origin",
                    "HEAD:refs/heads/%s" % branch, check=False)
            except PublicationError as e:
                # Immediate raise path (write-once pre-check, or a
                # _checkout/_git failure): annotate truthfully with
                # the attempt in progress; NOT loop exhaustion.
                if not getattr(e, "attempts", 0):
                    e.attempts = attempt
                e.exhausted = False
                if not getattr(e, "last_error", None):
                    e.last_error = str(e)
                raise
            if push.returncode == 0:
                self.bytes_published += len(data)
                return "PUBLISHED"
            last_err = push.stderr.strip()
        raise PublicationError(
            "push %s after %d attempts: %s" % (
                branch, self.max_push_attempts, last_err),
            attempts=self.max_push_attempts, exhausted=True,
            last_error=last_err)


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
            raise PublicationError(
                "injected publication failure", attempts=1,
                exhausted=False,
                last_error="injected publication failure")
        return self.inner.publish(branch, relpath, data)

    def read(self, branch, relpath):
        return self.inner.read(branch, relpath)

    def exists(self, branch, relpath):
        return self.inner.exists(branch, relpath)

    def list(self, branch, prefix=""):
        return self.inner.list(branch, prefix)
