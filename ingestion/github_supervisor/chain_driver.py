"""Lab chain driver — interleaves SegmentJobs with true overlap
semantics, acts on recorded dispatches by starting successor jobs,
and supports kill/recovery injection for the failure suites.

Clock model: each job gets its OWN VirtualClock sharing one epoch —
exactly the production assumption (independent processes whose clocks
are synchronised within the section-C bound). A job's sleeps advance
only its own frame; the stub venue and instrumentation timestamp by
the currently-stepping job's frame (harness.clock is a mux over the
per-job clocks, so test code can wire the venue to harness.clock).
Everything is synthetic; no network is touched.
"""
import json
import os

from . import gh_common as G
from . import claims as claims_mod
from . import supervisor as sup
from .dispatcher import RecordingDispatch
from .publisher import DirPublisher
from .venue_stub import VirtualClock


class _ClockMux:
    """now_s()/sleep_s() delegating to the active job's clock."""

    def __init__(self, harness):
        self.harness = harness

    def now_s(self):
        active = self.harness._active_clock
        return active.now_s() if active is not None else \
            self.harness.start_s

    def sleep_s(self, s):
        active = self.harness._active_clock
        if active is not None:
            active.sleep_s(s)


class HarnessDispatch(RecordingDispatch):
    def __init__(self, harness):
        super().__init__()
        self.harness = harness

    def dispatch(self, *, segment_index, manifest_sha256, role="segment"):
        res = super().dispatch(segment_index=segment_index,
                               manifest_sha256=manifest_sha256,
                               role=role)
        self.harness.on_dispatch(segment_index, manifest_sha256, role)
        return res


class ChainHarness:
    def __init__(self, *, freeze, root, venue_connector,
                 section_c_provider, probe_provider,
                 publisher=None, claim_store=None, clock_start_s=None):
        self.fz = freeze
        self.root = root
        os.makedirs(root, exist_ok=True)
        self.remote_dir = os.path.join(root, "remote")
        self.publisher = publisher or DirPublisher(self.remote_dir)
        self.claims = claim_store or claims_mod.DirClaimStore(
            os.path.join(root, "claims"),
            namespace=G.evidence_namespace(freeze))
        self.start_s = clock_start_s if clock_start_s is not None \
            else freeze["_a_ms"] / 1000.0 \
            - freeze["phase"]["dispatch_lead_s"] - 3600
        self.clock = _ClockMux(self)
        self._active_clock = None
        self.job_clocks = {}
        self.venue_connector = venue_connector
        self.section_c_provider = section_c_provider
        self.probe_provider = probe_provider
        self.events = []
        self.jobs = {}
        self.wakes = {}
        self.pending_dispatch = []
        self.dispatcher = HarnessDispatch(self)
        self.run_counter = 0
        self.recovery_count = {}
        self.seal_job = None
        self.seal_result = None
        raw = G.canon({k: v for k, v in freeze.items()
                       if not k.startswith("_")}).encode()
        self.publisher.publish("main", "FREEZE.json", raw)

    # ------------------------------------------------------------ jobs
    def _new_run_id(self):
        self.run_counter += 1
        return "run-%04d" % self.run_counter

    def start_segment(self, k, dispatch_input, *, run_id=None,
                      dispatcher=None, recovery=False, start_s=None):
        clock = VirtualClock(
            start_s if start_s is not None else self.clock.now_s())
        self.job_clocks[k] = clock
        run_id = run_id or self._new_run_id()
        job = sup.SegmentJob(
            freeze=self.fz, segment_index=k,
            run_id=run_id,
            publisher=self.publisher, claim_store=self.claims,
            dispatcher=dispatcher or self.dispatcher,
            connector=self.venue_connector, clock=clock,
            # production-faithful: every job gets a FRESH workspace
            # (a killed job's workspace does not survive); recovery
            # resumes from published chunks, not from local files
            work_root=os.path.join(self.root, "work", run_id),
            section_c_provider=self.section_c_provider,
            probe_provider=self.probe_provider,
            dispatch_input=dispatch_input, events=self.events)
        job.recovery = recovery
        self.jobs[k] = job
        self.wakes[k] = clock.now_s()
        return job

    def on_dispatch(self, segment_index, manifest_sha256, role):
        self.pending_dispatch.append(
            {"segment_index": segment_index,
             "manifest_sha256": manifest_sha256, "role": role,
             "at_s": self.clock.now_s()})

    def _act_on_dispatches(self):
        while self.pending_dispatch:
            d = self.pending_dispatch.pop(0)
            if d["role"] == "seal":
                seal_run = self._new_run_id()
                self.seal_job = sup.SealJob(
                    freeze=self.fz, run_id=seal_run,
                    publisher=self.publisher, claim_store=self.claims,
                    work_root=os.path.join(self.root, "work", seal_run),
                    events=self.events)
                self.seal_result = self.seal_job.run(
                    d["manifest_sha256"], now_s=d["at_s"])
            elif d["segment_index"] not in self.jobs:
                self.start_segment(d["segment_index"],
                                   d["manifest_sha256"],
                                   start_s=d["at_s"])

    # ---------------------------------------------------------- driver
    def run(self, max_steps=2_000_000):
        steps = 0
        while steps < max_steps:
            self._act_on_dispatches()
            active = [(self.wakes[k], k) for k, j in self.jobs.items()
                      if not j.finished]
            if not active:
                break
            _w, k = min(active)
            job = self.jobs[k]
            clock = self.job_clocks[k]
            clock.advance_to(self.wakes[k])
            self._active_clock = clock
            wake = job.step()
            steps += 1
            self.wakes[k] = wake if wake is not None \
                else float("inf")
            self._active_clock = None
        self._act_on_dispatches()
        return self.summary()

    def kill(self, k):
        """Simulate a platform kill: the job simply never steps again
        (no final, heartbeat goes stale in every other frame)."""
        job = self.jobs.pop(k)
        self.wakes.pop(k, None)
        self.events.append({"event": "KILLED", "segment": k,
                            "run_id": job.run_id})
        return job

    def recover(self, k, dispatch_input):
        """Recovery dispatch of the same segment index (design sec.
        8), counted per UTC day against the frozen restart budget;
        the harness refuses beyond it (the production count is kept
        by the watchdog/principal path, sec. 7.4 — the feasibility
        suite exercises the mapping here)."""
        import datetime
        day = datetime.datetime.fromtimestamp(
            self.clock.now_s(), datetime.timezone.utc).strftime(
            "%Y-%m-%d")
        self.recovery_count[day] = self.recovery_count.get(day, 0) + 1
        if self.recovery_count[day] > \
                self.fz["budgets"]["max_recovery_per_day"]:
            self.events.append({"event": "RECOVERY_REFUSED_OVER_LIMIT",
                                "segment": k, "day": day})
            return None
        start = self.job_clocks[k].now_s() if k in self.job_clocks \
            else None
        return self.start_segment(k, dispatch_input, recovery=True,
                                  start_s=start)

    # --------------------------------------------------------- summary
    def summary(self):
        finals = {}
        attempts = {}
        ns = G.evidence_namespace(self.fz)
        for k in range(1, len(self.fz["segment_plan"]["lengths_s"])
                       + 1):
            # D2: finals[k] is the AUTHORITATIVE final only
            # (final.json — the completing instance's clean record,
            # or None when no instance completed cleanly); per-
            # attempt terminal records are surfaced separately
            # under attempts[k], never folded into finals[k].
            raw = self.publisher.read(sup.seg_branch(k, ns),
                                      "final.json")
            finals[k] = json.loads(raw) if raw else None
            attempts[k] = sup.enumerate_attempt_records(
                self.publisher, sup.seg_branch(k, ns))
        return {"finals": finals,
                "attempts": attempts,
                "events": self.events,
                "seal_verdict": (self.seal_job.verdict
                                 if self.seal_job else None),
                "recovery_count": self.recovery_count,
                "jobs": {k: {"exit_reason": j.exit_reason,
                             "exit_code": j.exit_code}
                         for k, j in self.jobs.items()}}
