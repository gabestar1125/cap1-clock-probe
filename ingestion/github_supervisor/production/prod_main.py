"""Production (live) segment/seal entry point — the activation-step
wiring the accepted supervisor build deferred (seg_main.py wires ONLY
the synthetic stub and says so in its own comment).

What is different from seg_main, and ONLY this:
- the connector handed to supervisor.SegmentJob is the REAL venue
  connector: the frozen transport's built-in HTTPS connector
  (cap1_transport._default_connector — http.client.HTTPSConnection to
  the allowlisted host, the frozen fixed header set; the frozen
  transport has no API for auth material, non-GET methods, redirects,
  or non-allowlisted hosts/paths), wrapped in the same request
  instrumentation the rehearsal used so the live run's request log is
  measured, not asserted;
- the section-C provider is the real daemon capture
  (clock_evidence.capture_section_c) and the per-capture probe is
  clock_evidence.ChronyProbeProvider — the production forms seg_main
  selects with --section-c chrony / --probe chrony;
- the clock is the wall clock (no virtual clock exists here);
- before any job is constructed, the ACTIVATION GUARD runs: the
  freeze must load under gh_common.load_freeze (schema, window
  tiling, code hashes matching the frozen files on disk), must NOT
  be synthetic, must carry the production constants (series, segment
  plan, phase, budgets, thresholds — DAT-3), must carry DAT
  tv_pass/clock_pass, and the dispatch input must equal the freeze
  file's canonical SHA-256. Any failure prints
  {"activation": "BLOCKED", "reason": ...} and exits 4 — mirroring
  the frozen tick collector's freeze guard (cap1_run.main) — having
  made zero venue requests: the guard performs file reads and hash
  checks only, and no connector is invoked before it passes.

Everything downstream of the guard is the accepted supervisor
unchanged: SegmentJob builds HttpTransport(connector) ->
OwnershipFilter -> TransportPacer and drives the frozen
cap1_run.Tick through the runner's own injection seams; the frozen
Governor inside Tick enforces 10 requests/min, concurrency 1 (the
pacer + the single-threaded tick), <=2 retries, and the 5,000 total
budget carried across segments in the governor state seed.

Test seams (offline suites only; the CLI path never sets them):
main() and the builders accept injected connector/clock/providers,
exactly as cap1_run.Tick accepts injected transport/probe/time
(correction C7 of the frozen runner).
"""
import argparse
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_GS = os.path.dirname(_HERE)                    # github_supervisor/
_ING_DIR_PARENT = os.path.dirname(_GS)          # ingestion/
_LAB = os.path.dirname(_ING_DIR_PARENT)         # kalshi-integration-lab/
if _LAB not in sys.path:
    sys.path.insert(0, _LAB)

from ingestion import cap1_transport as ct  # noqa: E402
from ingestion.github_supervisor import gh_common as G  # noqa: E402
from ingestion.github_supervisor import claims as claims_mod  # noqa
from ingestion.github_supervisor import supervisor as sup  # noqa
from ingestion.github_supervisor import clock_evidence as ce  # noqa
from ingestion.github_supervisor.dispatcher import (  # noqa: E402
    RecordingDispatch, GitHubDispatch)
from ingestion.github_supervisor.publisher import (  # noqa: E402
    DirPublisher, GitPublisher)
from ingestion.github_supervisor.venue_stub import (  # noqa: E402
    InstrumentedConnector, WallClock)

PROD_SERIES = ("KXBTC15M", "KXETH15M", "KXSOL15M")
BLOCKED_EXIT = 4        # mirrors cap1_run.main()'s freeze-guard exit
LIVE_WORKFLOW_FILE = "cap1-supervisor-live.yml"


# ------------------------------------------------------------ §C path
def live_section_c_provider():
    """The real section-C record: daemon evidence captured on this
    host at segment start (30 s drift window), exactly the provider
    seg_main selects with --section-c chrony."""
    return ce.capture_section_c(30.0)


# ------------------------------------------------------------ connector
class _FileSink:
    """Request-log sink (the rehearsal's, verbatim behaviour): one
    JSON line per connector call, timestamps from the run's clock."""

    def __init__(self, path, clock):
        self.path = path
        self.clock = clock

    def append(self, entry):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, sort_keys=True) + "\n")


def build_live_connector(clock, log_path=None):
    """The REAL venue connector for a production job: the frozen
    transport's built-in HTTPS connector, instrumented. Constructing
    it performs no I/O; the first call happens inside the frozen
    Governor's discipline, downstream of the supervisor's claim and
    section-C gate."""
    log_path = log_path or os.environ.get(
        "CAP1_REQUEST_LOG", "cap1_request_log.jsonl")
    return InstrumentedConnector(
        ct._default_connector, clock.now_s, _FileSink(log_path, clock))


# ------------------------------------------------------------ guard
def activation_guard(freeze_path, dispatch_input, ingestion_dir):
    """Validate an ACTIVATION freeze before anything is built.

    Returns (freeze, None) on success, (None, reason) on refusal.
    File reads and hash checks only — zero venue requests are
    possible before this passes, and no connector exists yet when
    it runs."""
    try:
        fz = G.load_freeze(freeze_path, ingestion_dir)
    except Exception as e:
        return None, "freeze rejected: %s" % e
    if fz.get("synthetic"):
        return None, ("freeze is SYNTHETIC (rehearsal/test) — the "
                      "production entry refuses it")
    if dispatch_input != fz["_sha256"]:
        return None, "dispatch input != freeze canonical hash"
    dat = fz.get("dat") or {}
    if dat.get("tv_pass") is not True or dat.get("clock_pass") is not True:
        return None, "freeze DAT flags not PASS (tv_pass/clock_pass)"
    if tuple(fz["series"]) != PROD_SERIES:
        return None, "freeze series != production series %s" % (
            list(PROD_SERIES),)
    if list(fz["segment_plan"]["lengths_s"]) != \
            list(G.PROD_SEGMENT_LENGTHS_S):
        return None, "freeze segment plan != production plan"
    if fz["phase"] != G.PROD_PHASE:
        return None, "freeze phase != production phase constants"
    if fz["budgets"] != G.PROD_BUDGETS:
        return None, "freeze budgets != frozen production budgets"
    if fz["thresholds"] != G.PROD_THRESHOLDS:
        return None, "freeze thresholds != production thresholds"
    return fz, None


# ------------------------------------------------------------ builders
def build_segment_job(*, freeze, segment_index, run_id, publisher,
                      claim_store, dispatcher, work_root,
                      dispatch_input, connector=None, clock=None,
                      section_c_provider=None, probe_provider=None,
                      image_version="live", events=None):
    """Construct a production SegmentJob. Defaults are the live
    wiring (real connector, wall clock, real section-C/Chrony
    providers); every default is substitutable for offline tests."""
    clock = clock if clock is not None else WallClock()
    if connector is None:
        connector = build_live_connector(clock)
    job = sup.SegmentJob(
        freeze=freeze, segment_index=segment_index, run_id=run_id,
        publisher=publisher, claim_store=claim_store,
        dispatcher=dispatcher, connector=connector, clock=clock,
        work_root=work_root,
        section_c_provider=section_c_provider
        if section_c_provider is not None else live_section_c_provider,
        probe_provider=probe_provider
        if probe_provider is not None else ce.ChronyProbeProvider(),
        dispatch_input=dispatch_input,
        image_version=image_version,
        events=events if events is not None else [])
    # Recovery auto-detection (fix for the unreachable-takeover
    # defect): a prior claim in the store makes this dispatch a
    # recovery, wiring SegmentJob._step_claim's takeover branch.
    # A first-ever dispatch (no prior claim) keeps recovery=False
    # and the takeover path unreachable. Read-only arbitration
    # only — the claim step still refuses takeover of a LIVE claim.
    # The production clock is the wall clock: NO timing offset is
    # applied here (the virtual-clock staleness offset exists only
    # in the rehearsal entry, seg_main).
    try:
        prior_claim = claim_store.read(segment_index)
    except Exception:
        prior_claim = None
    if prior_claim is not None:
        job.recovery = True
    return job


def build_seal_job(*, freeze, run_id, publisher, claim_store,
                   work_root, events=None):
    return sup.SealJob(freeze=freeze, run_id=run_id,
                       publisher=publisher, claim_store=claim_store,
                       work_root=work_root,
                       events=events if events is not None else [])


def drive_segment(job, clock):
    """The production step loop (seg_main's, unchanged semantics):
    step, then sleep on the run's clock until the next wake."""
    while not job.finished:
        wake = job.step()
        if wake is None:
            break
        delay = wake - clock.now_s()
        if delay > 0:
            clock.sleep_s(min(delay, 3600))
    print(json.dumps({"segment": job.k,
                      "exit_reason": job.exit_reason,
                      "exit_code": job.exit_code}))
    return job.exit_code


# ------------------------------------------------------------ CLI
def main(argv=None, *, _connector=None, _clock=None,
         _section_c_provider=None, _probe_provider=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--freeze", required=True)
    ap.add_argument("--role", default="segment")
    ap.add_argument("--segment", type=int, default=1)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--dispatch-input", required=True)
    ap.add_argument("--work-root", required=True)
    ap.add_argument("--backend", default="git")
    ap.add_argument("--remote-dir")
    ap.add_argument("--claims-dir")
    ap.add_argument("--repo-dir")
    ap.add_argument("--origin")
    ap.add_argument("--dispatch", default="github")
    ap.add_argument("--github-repo")
    ap.add_argument("--workflow-file", default=LIVE_WORKFLOW_FILE)
    ap.add_argument("--image-version", default="live-1")
    args = ap.parse_args(argv)

    # ACTIVATION GUARD — before any job, publisher, or connector
    # exists. Zero venue requests on refusal.
    freeze, err = activation_guard(args.freeze, args.dispatch_input,
                                   _ING_DIR_PARENT)
    if freeze is None:
        print(json.dumps({"activation": "BLOCKED", "reason": err}))
        return BLOCKED_EXIT

    # The production freeze carries no evidence_namespace, so ns
    # is None here and every claim ref below is the legacy ref,
    # byte-identical to before this field existed.
    ns = G.evidence_namespace(freeze)
    if args.backend == "git":
        publisher = GitPublisher.clone(args.origin, args.repo_dir) \
            if not os.path.exists(os.path.join(args.repo_dir, ".git")) \
            else GitPublisher(args.repo_dir)
        claim_store = claims_mod.GitClaimStore(args.repo_dir,
                                               namespace=ns)
    else:
        publisher = DirPublisher(args.remote_dir)
        claim_store = claims_mod.DirClaimStore(args.claims_dir,
                                               namespace=ns)

    if args.dispatch == "github":
        dispatcher = GitHubDispatch(args.github_repo,
                                    args.workflow_file)
    else:
        dispatcher = RecordingDispatch()

    clock = _clock if _clock is not None else WallClock()

    if args.role == "seal":
        job = build_seal_job(freeze=freeze, run_id=args.run_id,
                             publisher=publisher,
                             claim_store=claim_store,
                             work_root=args.work_root)
        return job.run(args.dispatch_input, now_s=clock.now_s())

    job = build_segment_job(
        freeze=freeze, segment_index=args.segment,
        run_id=args.run_id, publisher=publisher,
        claim_store=claim_store, dispatcher=dispatcher,
        work_root=args.work_root, dispatch_input=args.dispatch_input,
        connector=_connector, clock=clock,
        section_c_provider=_section_c_provider,
        probe_provider=_probe_provider,
        image_version=args.image_version)
    return drive_segment(job, clock)


if __name__ == "__main__":
    sys.exit(main())
