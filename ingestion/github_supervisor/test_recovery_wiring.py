"""Recovery-wiring regression suite (offline, synthetic).

Pins the fix for the unreachable-takeover defect: SegmentJob's
recovery branch ran only when job.recovery was set, and neither
entry point (seg_main.py, production/prod_main.py) ever set it —
the only setter in the codebase was the lab chain driver. Any
re-dispatch of an already-claimed segment therefore refused at
check 4 forever (observed on GitHub, 2026-10-28: seg-03's first
job died after claiming; every recovery dispatch refused).

T1 seg_main + git, fresh segment: completes and hands off,
   recovery never engaged (no recovery_started.json, claim has
   no takeover_of).
T2 seg_main + git against tonight's REAL seg-03 state (fixture
   copied from a mirror of the live repository: dead run's claim
   + started.json + one heartbeat, virtual clock): a recovery
   dispatch with the correct predecessor input TAKES OVER
   (recovery_started.json published, claim carries takeover_of),
   completes the segment, hands off.
T3 production builder (in-process, dir stores): recovery is
   auto-detected (False before any claim, True once a claim
   exists), and takeover of a claim whose heartbeat is FRESH in
   wall-clock terms is refused at check 4 — the live claim is
   never taken over.
T4 seg_main + git, claim held by a LIVE owner (a concurrent
   process keeps republishing heartbeat.json with advancing
   at_local, the real GitHub geometry): the recovery dispatch
   refuses at check 4 — duplicate prevention intact.

Results JSON to argv[1]; two runs byte-identical (counts and
stable details only).
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LAB = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, LAB)

_spec = importlib.util.spec_from_file_location(
    "ts", os.path.join(HERE, "test_supervisor.py"))
TS = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(TS)

from ingestion.github_supervisor import gh_common as G            # noqa: E402
from ingestion.github_supervisor import claims as claims_mod      # noqa: E402
from ingestion.github_supervisor import supervisor as sup         # noqa: E402
from ingestion.github_supervisor.dispatcher import (              # noqa: E402
    RecordingDispatch)
from ingestion.github_supervisor.publisher import (               # noqa: E402
    DirPublisher)
from ingestion.github_supervisor.venue_stub import (              # noqa: E402
    VirtualClock)
from ingestion.github_supervisor.clock_evidence import (          # noqa: E402
    synthetic_section_c, ScriptedProbeProvider)
from ingestion.github_supervisor.rehearsal_support import (       # noqa: E402
    synthetic_connector)
from ingestion.github_supervisor.production.prod_main import (    # noqa: E402
    build_segment_job)

SEG_MAIN = os.path.join(HERE, "seg_main.py")
FREEZE_PATH = os.path.join(
    HERE, "rehearsal", "repo", "ingestion", "github_supervisor",
    "rehearsal", "FREEZE.rehearsal.json")
MIRROR = "/tmp/cap1-mirror.git"
DEAD_RUN = "gh-40790946031-s3"
SEG3_INPUT = ("7a2d7588fea65f0623e8a9d0bb9a5b5a2a78d41bb2205de249766"
              "2d4d46174de")

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append({"test": name, "pass": bool(cond),
                    "detail": str(detail)})
    return cond


# ------------------------------------------------------------ git utils
def git(bare, *args, check_rc=True):
    p = subprocess.run(["git", "--git-dir=%s" % bare] + list(args),
                       capture_output=True, text=True, timeout=120)
    if check_rc and p.returncode != 0:
        raise RuntimeError("git %s: %s" % (args[0], p.stderr.strip()))
    return p


def git_show(bare, ref, path):
    p = git(bare, "show", "%s:%s" % (ref, path), check_rc=False)
    if p.returncode != 0:
        return None
    return p.stdout


def git_ref_exists(bare, ref):
    p = git(bare, "rev-parse", "--verify", "--quiet",
            "refs/heads/%s" % ref, check_rc=False)
    return p.returncode == 0


def init_bare(tmp):
    bare = os.path.join(tmp, "origin.git")
    subprocess.run(["git", "init", "--bare", bare],
                   check=True, capture_output=True, text=True)
    git(bare, "symbolic-ref", "HEAD", "refs/heads/main")
    seed = os.path.join(tmp, "seed")
    subprocess.run(["git", "clone", bare, seed], check=True,
                   capture_output=True, text=True)
    env = dict(os.environ, GIT_AUTHOR_NAME="cap1-test",
               GIT_AUTHOR_EMAIL="cap1@localhost",
               GIT_COMMITTER_NAME="cap1-test",
               GIT_COMMITTER_EMAIL="cap1@localhost")
    with open(os.path.join(seed, "README.md"), "w") as f:
        f.write("recovery-wiring fixture\n")
    for args in (["git", "-C", seed, "add", "README.md"],
                 ["git", "-C", seed, "commit", "-m", "seed"],
                 ["git", "-C", seed, "push", "origin", "main"]):
        subprocess.run(args, check=True, capture_output=True,
                       text=True, env=env)
    return bare


def mirror_fixture(tmp):
    """Tonight's real repository state, copied (pushes land in the
    copy; the mirror itself is only read). HEAD is pointed at main
    (the mirror's HEAD says master; the real repo's HEAD is main).
    """
    bare = os.path.join(tmp, "tonight.git")
    shutil.copytree(MIRROR, bare)
    git(bare, "symbolic-ref", "HEAD", "refs/heads/main")
    return bare


def load_rehearsal_freeze():
    with open(FREEZE_PATH, "rb") as f:
        raw = f.read()
    doc = json.loads(raw)
    import hashlib
    return doc, hashlib.sha256(raw).hexdigest()


def virtual_start(doc, k):
    """The workflow's own formula (cap1-supervisor-rehearsal.yml
    'Compute this segment's virtual start')."""
    a_s = G.parse_iso(doc["activation_A"]) / 1000.0
    return a_s + sum(doc["segment_plan"]["lengths_s"][:k - 1]) \
        - doc["phase"]["dispatch_lead_s"] - 60


def run_seg_main(tmp, origin, segment, run_id, dispatch_input,
                 vstart):
    argv = [sys.executable, SEG_MAIN,
            "--freeze", FREEZE_PATH,
            "--role", "segment", "--segment", str(segment),
            "--run-id", run_id,
            "--dispatch-input", dispatch_input,
            "--work-root", os.path.join(tmp, "work-%s" % run_id),
            "--backend", "git",
            "--repo-dir", os.path.join(tmp, "clone-%s" % run_id),
            "--origin", origin,
            "--dispatch", "recording",
            "--virtual-start", repr(float(vstart)),
            "--section-c", "synthetic:1.0",
            "--probe", "synthetic:5",
            "--image-version", "test-recovery"]
    p = subprocess.run(argv, capture_output=True, text=True,
                       timeout=1200)
    summary = None
    for line in p.stdout.splitlines():
        try:
            d = json.loads(line)
        except Exception:
            continue
        if "exit_reason" in d:
            summary = d
    return p, summary


# ------------------------------------------------------------ T1
def t1_fresh_segment():
    tmp = tempfile.mkdtemp()
    bare = init_bare(tmp)
    doc, fsha = load_rehearsal_freeze()
    p, s = run_seg_main(tmp, bare, 1, "t1-fresh", fsha,
                        virtual_start(doc, 1))
    check("t1.summary", s is not None, p.stdout[-200:] + p.stderr[-200:])
    if s is None:
        return
    check("t1.handed_off", s["exit_reason"] == "HANDED_OFF"
          and s["exit_code"] == 0, s)
    claim = git_show(bare, "cap1-claims/seg-01", "claim.json")
    claim = json.loads(claim) if claim else {}
    check("t1.claim_ours_no_takeover",
          claim.get("run_id") == "t1-fresh"
          and "takeover_of" not in claim, claim.get("run_id"))
    check("t1.no_recovery_started",
          git_show(bare, "cap1-segments/seg-01",
                   "recovery_started.json") is None)
    fin = git_show(bare, "cap1-segments/seg-01", "final.json")
    fin = json.loads(fin) if fin else {}
    check("t1.final_handed_off",
          fin.get("exit_reason") == "HANDED_OFF",
          fin.get("exit_reason"))


# ------------------------------------------------------------ T2
def t2_takeover_tonight_state():
    if not os.path.isdir(MIRROR):
        check("t2.mirror_present", False, "%s missing" % MIRROR)
        return
    tmp = tempfile.mkdtemp()
    bare = mirror_fixture(tmp)
    doc, _fsha = load_rehearsal_freeze()
    man = git_show(bare, "cap1-segments/seg-02", "manifest.json")
    check("t2.fixture_input_matches",
          man is not None
          and G.doc_hash(json.loads(man)) == SEG3_INPUT,
          "seg-02 manifest doc_hash anchors the fixture to "
          "tonight's recorded dispatch input")
    dead = git_show(bare, "cap1-claims/seg-03", "claim.json")
    dead = json.loads(dead) if dead else {}
    check("t2.fixture_dead_claim", dead.get("run_id") == DEAD_RUN,
          dead.get("run_id"))
    p, s = run_seg_main(tmp, bare, 3, "t2-recovery", SEG3_INPUT,
                        virtual_start(doc, 3))
    check("t2.summary", s is not None, p.stdout[-200:] + p.stderr[-200:])
    if s is None:
        return
    check("t2.handed_off", s["exit_reason"] == "HANDED_OFF"
          and s["exit_code"] == 0, s)
    rec = git_show(bare, "cap1-segments/seg-03",
                   "recovery_started.json")
    rec = json.loads(rec) if rec else {}
    check("t2.recovery_started_published",
          rec.get("run_id") == "t2-recovery", rec.get("run_id"))
    claim = git_show(bare, "cap1-claims/seg-03", "claim.json")
    claim = json.loads(claim) if claim else {}
    check("t2.claim_takeover_recorded",
          claim.get("run_id") == "t2-recovery"
          and (claim.get("takeover_of") or {}).get("run_id")
          == DEAD_RUN,
          "%s takeover_of=%s" % (claim.get("run_id"),
                                 claim.get("takeover_of")))
    fin = git_show(bare, "cap1-segments/seg-03", "final.json")
    fin = json.loads(fin) if fin else {}
    counts = (fin.get("record_counts") or {}).get("cohort") or {}
    check("t2.final_ok", fin.get("exit_reason") == "HANDED_OFF"
          and fin.get("crosscheck") == "OK",
          "exit=%s crosscheck=%s cohort_records=%s"
          % (fin.get("exit_reason"), fin.get("crosscheck"),
             {k: v for k, v in counts.items()
              if k != "tip_hash"}))


# ------------------------------------------------------------ T3
def drive_until(job, clock, pred, max_steps=20000):
    for _ in range(max_steps):
        if job.finished or pred(job):
            return job
        wake = job.step()
        if wake is None:
            break
        delay = wake - clock.now_s()
        if delay > 0:
            clock.sleep_s(min(delay, 3600))
    return job


def t3_prod_builder_fresh_refusal():
    tmp = tempfile.mkdtemp()
    fz, sha = TS.make_freeze(tmp)
    pub = DirPublisher(os.path.join(tmp, "remote"))
    claims = claims_mod.DirClaimStore(os.path.join(tmp, "claims"))
    disp = RecordingDispatch()
    start = fz["_a_ms"] / 1000.0 \
        - fz["phase"]["dispatch_lead_s"] - 60
    clock = VirtualClock(start)   # wall-clock TERMS: ages are real
    # elapsed seconds on one shared clock; freshness is what the
    # takeover rule measures, in production via the wall clock.

    def build(run_id, work):
        return build_segment_job(
            freeze=fz, segment_index=1, run_id=run_id,
            publisher=pub, claim_store=claims, dispatcher=disp,
            work_root=work, dispatch_input=sha,
            connector=synthetic_connector(fz, clock), clock=clock,
            section_c_provider=lambda: synthetic_section_c(1.0),
            probe_provider=ScriptedProbeProvider(
                {"state": "SYNCED", "uncertainty_ms": 5,
                 "source": "synthetic"}))

    job_a = build("t3-live", os.path.join(tmp, "workA"))
    check("t3.first_dispatch_not_recovery", job_a.recovery is False)
    drive_until(job_a, clock,
                lambda j: pub.read(sup.seg_branch(1),
                                   "heartbeat.json") is not None)
    held = claims.read(1)
    check("t3.live_claim_held",
          held is not None and held.get("run_id") == "t3-live",
          held and held.get("run_id"))
    job_b = build("t3-second", os.path.join(tmp, "workB"))
    check("t3.redispatch_auto_recovery", job_b.recovery is True)
    drive_until(job_b, clock, lambda j: j.finished)
    check("t3.fresh_takeover_refused_check4",
          job_b.exit_reason == "REFUSED:4", job_b.exit_reason)
    refused = pub.read(sup.seg_branch(1), "refused.json")
    refused = json.loads(refused) if refused else {}
    check("t3.refusal_is_takeover_refusal",
          refused.get("check_failed") == 4
          and "takeover refused" in refused.get("detail", ""),
          refused.get("detail"))
    still = claims.read(1)
    check("t3.claim_still_live_owner",
          still is not None and still.get("run_id") == "t3-live",
          still and still.get("run_id"))


# ------------------------------------------------------------ T4
def t4_git_live_owner_refusal():
    if not os.path.isdir(MIRROR):
        check("t4.mirror_present", False, "%s missing" % MIRROR)
        return
    tmp = tempfile.mkdtemp()
    bare = mirror_fixture(tmp)
    doc, _fsha = load_rehearsal_freeze()
    start = virtual_start(doc, 3)
    hb_raw = git_show(bare, "cap1-segments/seg-03", "heartbeat.json")
    hb_doc = json.loads(hb_raw)
    stop = threading.Event()
    errors = []

    def owner():
        """A LIVE owner: republishes heartbeat.json from its own
        clone with at_local advancing 400 virtual s per real s
        (the real job heartbeats every 300 virtual s)."""
        t0 = time.time()
        n = 0
        while not stop.is_set():
            n += 1
            try:
                cdir = os.path.join(tmp, "owner-%d" % n)
                subprocess.run(
                    ["git", "clone", "--quiet", "--branch",
                     "cap1-segments/seg-03", bare, cdir],
                    check=True, capture_output=True, text=True,
                    timeout=60)
                doc_hb = dict(hb_doc)
                doc_hb["at_local"] = G.iso_local_label(
                    start + (time.time() - t0) * 400.0)
                doc_hb["state"] = "ACTIVE"
                with open(os.path.join(cdir, "heartbeat.json"),
                          "w") as f:
                    f.write(G.canon(doc_hb))
                env = dict(os.environ,
                           GIT_AUTHOR_NAME="cap1-supervisor",
                           GIT_AUTHOR_EMAIL="cap1@localhost",
                           GIT_COMMITTER_NAME="cap1-supervisor",
                           GIT_COMMITTER_EMAIL="cap1@localhost")
                for args in (["git", "-C", cdir, "add",
                              "heartbeat.json"],
                             ["git", "-C", cdir, "commit", "--quiet",
                              "-m", "cap1 publish heartbeat.json"],
                             ["git", "-C", cdir, "push", "--quiet",
                              "origin", "cap1-segments/seg-03"]):
                    subprocess.run(args, check=True,
                                   capture_output=True, text=True,
                                   env=env, timeout=60)
                shutil.rmtree(cdir, ignore_errors=True)
            except Exception as e:                      # noqa: BLE001
                errors.append(str(e)[:120])
            stop.wait(0.7)

    th = threading.Thread(target=owner, daemon=True)
    th.start()
    try:
        time.sleep(1.2)     # owner establishes its heartbeat lead
        p, s = run_seg_main(tmp, bare, 3, "t4-dup", SEG3_INPUT,
                            start)
    finally:
        stop.set()
        th.join(timeout=30)
    check("t4.summary", s is not None,
          "owner_errors=%s" % (errors[:1],))
    if s is None:
        return
    check("t4.refused_check4", s["exit_reason"] == "REFUSED:4",
          s)
    claim = git_show(bare, "cap1-claims/seg-03", "claim.json")
    claim = json.loads(claim) if claim else {}
    check("t4.claim_still_live_owner",
          claim.get("run_id") == DEAD_RUN
          and "takeover_of" not in claim, claim.get("run_id"))
    check("t4.no_recovery_started",
          git_show(bare, "cap1-segments/seg-03",
                   "recovery_started.json") is None)


def main():
    t1_fresh_segment()
    t2_takeover_tonight_state()
    t3_prod_builder_fresh_refusal()
    t4_git_live_owner_refusal()
    out = {"suite": "test_recovery_wiring",
           "passed": sum(1 for r in RESULTS if r["pass"]),
           "total": len(RESULTS), "results": RESULTS}
    with open(sys.argv[1], "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    print(json.dumps({"passed": out["passed"],
                      "total": out["total"]}))
    for r in RESULTS:
        if not r["pass"]:
            print("FAIL", r["test"], r["detail"])


if __name__ == "__main__":
    main()
