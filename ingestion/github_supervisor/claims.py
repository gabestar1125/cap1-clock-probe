"""Atomic segment claims — principal refinement (i).

The design's section 4.3 check 4 was a check-then-write on started.json:
read (absent?) then write. Two racing jobs can both pass the read before
either writes — a classic TOCTOU window. The claim primitive here
replaces it with a single atomic test-and-set at an authority:

- GitClaimStore: creating a git ref is one server-side ref transaction.
  Creating refs/heads/cap1-claims/seg-NN with a push of a unique claim
  commit succeeds for exactly one racer; the loser's push is rejected
  (the ref now exists pointing at a commit the loser's commit does not
  descend from). Takeover of a STALE claim is a compare-and-swap:
  `git push --force-with-lease=<ref>:<observed-old-sha>` — the update
  lands only if the ref still holds the exact value the recovery job
  based its staleness judgement on.
- DirClaimStore (offline/lab mirror of the same semantics):
  os.open(O_CREAT|O_EXCL) is a single atomic test-and-set in the
  filesystem. Takeover is performed under an atomic mkdir mutex with
  staleness re-verified inside the mutex, then an atomic rename — the
  second racer, entering the mutex after the first, sees a FRESH claim
  and refuses.

A claim record binds: segment index, run ID, manifest (or freeze) hash
it was dispatched against, and the claim time. Holding someone else's
live claim is never overridden: takeover requires staleness evidence
(heartbeat age) and is itself a chained, evidenced record.
"""
import json
import os
import subprocess
import time

from . import gh_common as G


class ClaimLost(Exception):
    """Another run holds (or won) this segment's claim."""


class ClaimHeld(Exception):
    """This run already holds the claim (idempotent self-claim)."""


def claim_doc(segment_index, run_id, against_sha256, now_s,
              takeover_of=None):
    doc = {"schema": "cap1-claim/1", "segment_index": segment_index,
           "run_id": run_id, "against_sha256": against_sha256,
           "claimed_at_local": G.iso_local_label(now_s)}
    if takeover_of is not None:
        doc["takeover_of"] = takeover_of
    return doc


class DirClaimStore:
    """Filesystem claim store (lab). Root layout mirrors branch names:
    <root>/cap1-claims/seg-NN.json is the live claim; takeover history
    is appended to seg-NN.takeovers.jsonl."""

    def __init__(self, root, namespace=None):
        # Namespace-isolated layout for a freeze carrying
        # evidence_namespace ("cap1-r2-claims"); the default (None)
        # is the legacy layout, byte-identical to before.
        name = "cap1-%s-claims" % namespace if namespace \
            else "cap1-claims"
        self.dir = os.path.join(root, name)
        os.makedirs(self.dir, exist_ok=True)

    def _path(self, segment_index):
        return os.path.join(self.dir, "seg-%02d.json" % segment_index)

    def read(self, segment_index):
        p = self._path(segment_index)
        if not os.path.exists(p):
            return None
        return G.read_json(p)

    def claim(self, segment_index, run_id, against_sha256, now_s):
        """Atomic test-and-set. Returns the claim doc on success;
        raises ClaimHeld if run_id already holds it, ClaimLost if
        another run does."""
        doc = claim_doc(segment_index, run_id, against_sha256, now_s)
        p = self._path(segment_index)
        # Atomic test-and-set WITH atomic content publication: the
        # doc is written+fsynced under a run-unique temp name, then
        # os.link() publishes it — link fails iff the claim already
        # exists, and any reader of an existing claim always sees
        # complete content. (The previous O_EXCL-create-then-write
        # left a window in which a concurrent loser read an empty
        # file and crashed instead of losing cleanly — found by the
        # multi-process race suite.)
        tmp = p + ".tmp-%s-%d" % (run_id, os.getpid())
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, p)
        except FileExistsError:
            existing = self.read(segment_index)
            if existing and existing.get("run_id") == run_id:
                raise ClaimHeld(segment_index)
            raise ClaimLost(
                f"seg {segment_index} held by "
                f"{existing.get('run_id') if existing else '?'}")
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return doc

    def takeover(self, segment_index, run_id, against_sha256, now_s,
                 stale_after_s, heartbeat_age_s):
        """CAS takeover of a stale claim. heartbeat_age_s is the
        caller's measured evidence; staleness is re-verified under the
        mutex against the same figure the caller supplies via the
        claim's own heartbeat sidecar (seg-NN.heartbeat_epoch)."""
        if heartbeat_age_s is None or heartbeat_age_s <= stale_after_s:
            raise ClaimLost("claim is not stale; takeover refused")
        mutex = self._path(segment_index) + ".takeover-mutex"
        try:
            os.mkdir(mutex)  # atomic; exactly one recovery proceeds
        except FileExistsError:
            raise ClaimLost("another takeover is in progress")
        try:
            current = self.read(segment_index)
            if current is None:
                return self.claim(segment_index, run_id, against_sha256,
                                  now_s)
            if current.get("run_id") == run_id:
                raise ClaimHeld(segment_index)
            last_hb = self._last_sign_of_life(segment_index, current)
            age = (now_s - last_hb) if last_hb is not None else None
            if age is None or age <= stale_after_s:
                raise ClaimLost(
                    "claim refreshed inside mutex; takeover refused")
            doc = claim_doc(segment_index, run_id, against_sha256, now_s,
                            takeover_of={"run_id": current["run_id"],
                                         "claim_sha256": G.doc_hash(
                                             current),
                                         "heartbeat_age_s": age})
            tmp = self._path(segment_index) + ".tmp-%s" % run_id
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, indent=1, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._path(segment_index))  # atomic rename
            with open(self._path(segment_index) + ".takeovers.jsonl",
                      "a", encoding="utf-8") as f:
                f.write(G.canon(doc) + "\n")
            return doc
        finally:
            os.rmdir(mutex)

    def heartbeat(self, segment_index, now_s):
        p = self._path(segment_index) + ".heartbeat_epoch"
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            f.write(repr(float(now_s)))
        os.replace(tmp, p)

    def _last_sign_of_life(self, segment_index, doc=None):
        """Epoch seconds of the claim holder's last sign of life:
        the heartbeat sidecar, else the claim's own timestamp (a
        job killed before its first heartbeat must still become
        recoverable once the claim is old enough — otherwise an
        early kill would brick the segment forever)."""
        p = self._path(segment_index) + ".heartbeat_epoch"
        try:
            return float(open(p).read().strip())
        except Exception:
            pass
        if doc is None:
            doc = self.read(segment_index)
        if doc and doc.get("claimed_at_local"):
            try:
                return G.parse_iso(
                    doc["claimed_at_local"][:19] + "Z") / 1000.0
            except Exception:
                return None
        return None

    def heartbeat_age(self, segment_index, now_s):
        last = self._last_sign_of_life(segment_index)
        return None if last is None else now_s - last


class GitClaimStore:
    """Git-backed claim store: the claim for segment NN is the ref
    refs/heads/cap1-claims/seg-NN pointing at a claim commit whose tree
    contains claim.json. All operations go through a local clone of the
    (bare or normal) repository at repo_dir; correctness rests on the
    server-side atomicity of a single ref update, identical for a local
    bare repository and for GitHub."""

    def __init__(self, repo_dir, namespace=None):
        self.repo = repo_dir
        # Freeze-driven evidence namespace (gh_common.
        # evidence_namespace): None = legacy refs, byte-identical
        # to before; "r2" = refs/heads/cap1-r2-claims/seg-NN, fully
        # disjoint from the legacy claim refs in the same repo.
        self.namespace = namespace or None

    def _git(self, *args, check=True):
        p = subprocess.run(["git", "-C", self.repo] + list(args),
                           capture_output=True, text=True, timeout=60)
        if check and p.returncode != 0:
            raise RuntimeError("git %s failed: %s" % (args[0],
                                                      p.stderr.strip()))
        return p

    def _ref(self, segment_index):
        if self.namespace:
            return "refs/heads/cap1-%s-claims/seg-%02d" % (
                self.namespace, segment_index)
        return "refs/heads/cap1-claims/seg-%02d" % segment_index

    def _remote_ref_sha(self, segment_index):
        p = self._git("ls-remote", "origin", self._ref(segment_index),
                      check=False)
        out = p.stdout.strip()
        return out.split()[0] if out else None

    def read(self, segment_index):
        sha = self._remote_ref_sha(segment_index)
        if sha is None:
            return None
        p = self._git("fetch", "origin", self._ref(segment_index),
                      check=False)
        show = self._git("show", "%s:claim.json" % sha, check=False)
        if show.returncode != 0:
            return {"_unreadable_claim_sha": sha}
        return json.loads(show.stdout)

    def _claim_commit(self, doc):
        """Create a commit object carrying claim.json (no checkout)."""
        env = dict(os.environ, GIT_AUTHOR_NAME="cap1-supervisor",
                   GIT_AUTHOR_EMAIL="cap1@localhost",
                   GIT_COMMITTER_NAME="cap1-supervisor",
                   GIT_COMMITTER_EMAIL="cap1@localhost")
        blob = subprocess.run(
            ["git", "-C", self.repo, "hash-object", "-w", "--stdin"],
            input=G.canon(doc), capture_output=True, text=True,
            env=env, timeout=30)
        blob_sha = blob.stdout.strip()
        tree = subprocess.run(
            ["git", "-C", self.repo, "mktree"],
            input="100644 blob %s\tclaim.json\n" % blob_sha,
            capture_output=True, text=True, env=env, timeout=30)
        tree_sha = tree.stdout.strip()
        commit = subprocess.run(
            ["git", "-C", self.repo, "commit-tree", tree_sha, "-m",
             "cap1 claim seg-%02d %s" % (doc["segment_index"],
                                         doc["run_id"])],
            capture_output=True, text=True, env=env, timeout=30)
        return commit.stdout.strip()

    def claim(self, segment_index, run_id, against_sha256, now_s):
        doc = claim_doc(segment_index, run_id, against_sha256, now_s)
        commit = self._claim_commit(doc)
        ref = self._ref(segment_index)
        p = self._git("push", "origin", "%s:%s" % (commit, ref),
                      check=False)
        if p.returncode == 0:
            return doc
        existing = self.read(segment_index)
        if existing and existing.get("run_id") == run_id:
            raise ClaimHeld(segment_index)
        raise ClaimLost("ref creation rejected: %s"
                        % p.stderr.strip().splitlines()[-1]
                        if p.stderr.strip() else "push rejected")

    def takeover(self, segment_index, run_id, against_sha256, now_s,
                 stale_after_s, heartbeat_age_s):
        if heartbeat_age_s is None or heartbeat_age_s <= stale_after_s:
            raise ClaimLost("claim is not stale; takeover refused")
        old_sha = self._remote_ref_sha(segment_index)
        if old_sha is None:
            return self.claim(segment_index, run_id, against_sha256,
                              now_s)
        current = self.read(segment_index)
        doc = claim_doc(segment_index, run_id, against_sha256, now_s,
                        takeover_of={"run_id": (current or {}).get(
                            "run_id"), "claim_commit": old_sha,
                            "heartbeat_age_s": heartbeat_age_s})
        commit = self._claim_commit(doc)
        ref = self._ref(segment_index)
        # Compare-and-swap: lands only if the ref still holds old_sha.
        p = self._git("push",
                      "--force-with-lease=%s:%s" % (ref, old_sha),
                      "origin", "%s:%s" % (commit, ref), check=False)
        if p.returncode == 0:
            return doc
        raise ClaimLost("takeover CAS rejected (ref moved): %s"
                        % p.stderr.strip().splitlines()[-1]
                        if p.stderr.strip() else "push rejected")

    def heartbeat(self, segment_index, now_s):
        """Git claims carry liveness in the segment branch's
        heartbeat.json (publisher's job); this store's staleness input
        is the caller-measured age of that record. No-op by design."""
        return None
