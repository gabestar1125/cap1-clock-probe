"""Segment/seal entry point — the process a GitHub Actions job runs
(and the empirical suite runs as a subprocess).

Modes:
- --backend dir (default): DirPublisher + DirClaimStore (lab).
- --backend git --repo-dir DIR --origin URL: GitPublisher (dedicated
  clone) + GitClaimStore (the rehearsal/production form).
- --dispatch recording (default; prints dispatch requests as JSON
  lines) or --dispatch github (workflow_dispatch via GITHUB_TOKEN;
  PV-1 territory, rehearsal only in this build).
- Clock: --wall (default) or --virtual-start EPOCH_S (accelerated
  synthetic schedule; sleeps advance the virtual clock instantly).
- Clock evidence: --section-c synthetic:<bound_ms> or chrony;
  --probe synthetic:<uncertainty_ms> or chrony. Synthetic providers
  are labelled synthetic in every record they touch.
"""
import argparse
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_LAB = os.path.dirname(os.path.dirname(_HERE))
if _LAB not in sys.path:
    sys.path.insert(0, _LAB)

from ingestion.github_supervisor import gh_common as G  # noqa: E402
from ingestion.github_supervisor import claims as claims_mod  # noqa
from ingestion.github_supervisor import supervisor as sup  # noqa
from ingestion.github_supervisor import clock_evidence as ce  # noqa
from ingestion.github_supervisor.dispatcher import (  # noqa: E402
    RecordingDispatch, GitHubDispatch)
from ingestion.github_supervisor.publisher import (  # noqa: E402
    DirPublisher, GitPublisher)
from ingestion.github_supervisor.venue_stub import (  # noqa: E402
    VirtualClock, WallClock)


class PrintDispatch(RecordingDispatch):
    def dispatch(self, **kw):
        print(json.dumps({"dispatch": kw}), flush=True)
        return super().dispatch(**kw)


def detect_prior_claim(claim_store, segment_index):
    """Recovery auto-detection (fix for the unreachable-takeover
    defect): a segment is a RECOVERY dispatch iff the claim store
    already holds a claim for it. Read-only arbitration only — the
    supervisor's claim step still decides (a live claim's takeover
    is refused there; a first-ever dispatch, with no prior claim,
    keeps recovery=False and the takeover path unreachable).
    Returns the claim doc, or None. Any read failure is treated as
    no claim: the claim step itself remains the arbiter."""
    read = getattr(claim_store, "read", None)
    if read is None:
        return None
    try:
        return read(segment_index)
    except Exception:
        return None


def last_sign_of_life_s(publisher, segment_index, claim_doc):
    """Epoch seconds of the previous instance's last sign of life:
    the segment branch's heartbeat.json at_local, falling back to
    the claim's claimed_at_local — the supervisor's own fallback
    order in SegmentJob._claim_heartbeat_age. None if neither
    parses."""
    at_local = None
    try:
        raw = publisher.read(sup.seg_branch(segment_index),
                             "heartbeat.json")
    except Exception:
        raw = None
    if raw:
        try:
            at_local = json.loads(raw).get("at_local")
        except Exception:
            at_local = None
    if not at_local and claim_doc:
        at_local = claim_doc.get("claimed_at_local")
    if not at_local:
        return None
    try:
        return G.parse_iso(at_local[:19] + "Z") / 1000.0
    except Exception:
        return None


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--freeze", required=True)
    ap.add_argument("--role", default="segment")
    ap.add_argument("--segment", type=int, default=1)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--dispatch-input", required=True)
    ap.add_argument("--work-root", required=True)
    ap.add_argument("--backend", default="dir")
    ap.add_argument("--remote-dir")
    ap.add_argument("--claims-dir")
    ap.add_argument("--repo-dir")
    ap.add_argument("--origin")
    ap.add_argument("--dispatch", default="recording")
    ap.add_argument("--github-repo")
    ap.add_argument("--workflow-file",
                    default="cap1-supervisor-rehearsal.yml")
    ap.add_argument("--virtual-start", type=float)
    ap.add_argument("--section-c", default="synthetic:1.0")
    ap.add_argument("--probe", default="synthetic:5")
    ap.add_argument("--image-version", default="unknown")
    args = ap.parse_args(argv)

    ing_dir = os.path.join(_LAB, "ingestion")
    freeze = G.load_freeze(args.freeze, ing_dir)

    if args.backend == "git":
        publisher = GitPublisher.clone(args.origin, args.repo_dir) \
            if not os.path.exists(os.path.join(args.repo_dir, ".git")) \
            else GitPublisher(args.repo_dir)
        claim_store = claims_mod.GitClaimStore(args.repo_dir)
    else:
        publisher = DirPublisher(args.remote_dir)
        claim_store = claims_mod.DirClaimStore(args.claims_dir)

    if args.dispatch == "github":
        dispatcher = GitHubDispatch(args.github_repo,
                                    args.workflow_file)
    else:
        dispatcher = PrintDispatch()

    # Recovery auto-detection, evaluated BEFORE the job is built:
    # a prior claim in the store makes this dispatch a recovery.
    prior_claim = None
    if args.role != "seal":
        prior_claim = detect_prior_claim(claim_store, args.segment)

    clock_start = args.virtual_start
    if args.virtual_start is not None and prior_claim is not None:
        # Rehearsal virtual-clock staleness (seg_main only; the
        # production entry is wall-clock and takes no offset): the
        # workflow formula pins EVERY instance of a segment to the
        # same virtual start, so a recovery instance would measure
        # the dead instance's heartbeat age as ~0 and the takeover
        # would refuse "not stale" forever. Resume the timeline
        # just past the staleness gap instead: the recovery
        # instance's clock starts at the dead instance's last sign
        # of life + claim_stale_s + 60 — the earliest virtual
        # instant at which the claim is genuinely stale, so the
        # measured age is the true gap the frozen rule requires,
        # neither fabricated larger nor smaller in effect (any
        # later start measures an even older heartbeat; the
        # supervisor's slot accounting keys off absolute slot
        # times, so the missed interval is accounted, not hidden).
        last = last_sign_of_life_s(publisher, args.segment,
                                   prior_claim)
        if last is not None:
            stale_s = freeze["phase"].get("claim_stale_s", 900)
            clock_start = max(clock_start, last + stale_s + 60)

    clock = VirtualClock(clock_start) \
        if args.virtual_start is not None else WallClock()

    if args.section_c.startswith("synthetic:"):
        bound = float(args.section_c.split(":", 1)[1])
        section_c_provider = lambda: ce.synthetic_section_c(bound)  # noqa
    else:
        section_c_provider = lambda: ce.capture_section_c(30.0)  # noqa
    if args.probe.startswith("synthetic:"):
        unc = int(args.probe.split(":", 1)[1])
        probe_provider = ce.ScriptedProbeProvider(
            {"state": "SYNCED", "uncertainty_ms": unc,
             "source": "synthetic"})
    else:
        probe_provider = ce.ChronyProbeProvider()

    if args.role == "seal":
        job = sup.SealJob(freeze=freeze, run_id=args.run_id,
                          publisher=publisher, claim_store=claim_store,
                          work_root=args.work_root, events=[])
        return job.run(args.dispatch_input, now_s=clock.now_s())

    # The connector is supplied by the caller's environment: in the
    # rehearsal it is ALWAYS the synthetic stub (zero venue requests);
    # the live connector is wired only by the future activation step,
    # which is outside this build.
    from ingestion.github_supervisor.rehearsal_support import (
        synthetic_connector)
    connector = synthetic_connector(freeze, clock)
    job = sup.SegmentJob(
        freeze=freeze, segment_index=args.segment, run_id=args.run_id,
        publisher=publisher, claim_store=claim_store,
        dispatcher=dispatcher, connector=connector, clock=clock,
        work_root=args.work_root,
        section_c_provider=section_c_provider,
        probe_provider=probe_provider,
        dispatch_input=args.dispatch_input,
        image_version=args.image_version, events=[])
    if prior_claim is not None:
        # Wire the takeover path (SegmentJob._step_claim): with a
        # prior claim present this dispatch may take over a STALE
        # claim; a live claim still refuses there.
        job.recovery = True
    while not job.finished:
        wake = job.step()
        if wake is None:
            break
        delay = wake - clock.now_s()
        if delay > 0:
            clock.sleep_s(min(delay, 3600))
    print(json.dumps({"segment": args.segment,
                      "exit_reason": job.exit_reason,
                      "exit_code": job.exit_code}))
    return job.exit_code


if __name__ == "__main__":
    sys.exit(main())
