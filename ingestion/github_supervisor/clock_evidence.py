"""Section-C clock evidence for segments — reuses the proven probe
(ingestion/github_probe/probe_clock.py) by importing its capture,
parse, and derivation functions unchanged.

Two products:
- capture_section_c(): the full section-C record at a segment start
  (daemon evidence + derivation over recorded numbers). Segment 1's
  record is the activation gate (derived bound <= 100 ms); later
  segments must derive <= 250 ms to capture at all.
- ChronyProbeProvider: the per-capture clock_status_fn the frozen
  engine requires. Its uncertainty formula is EXACTLY the frozen
  runner's (cap1_run.clock_probe): |system offset| + root dispersion +
  root delay/2, ceiling to ms, UNKNOWN unless chrony reports a normal
  leap status. Implemented here from the same chronyc CSV fields so
  the supervisor never has to import the runner for a probe.
- ScriptedProbeProvider: deterministic synthetic provider for lab and
  rehearsal (labelled synthetic wherever its readings are recorded).
"""
import importlib.util
import math
import os
import subprocess
import time

_PROBE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir,
    "github_probe", "probe_clock.py")


def _load_probe():
    spec = importlib.util.spec_from_file_location("cap1_probe_clock",
                                                  _PROBE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


probe = _load_probe()

SECTION_C_SCHEMA = "cap1-section-c/1"

# --- Bounded clock-readiness wait (live4 seg-02 remediation) --------
# capture_section_c historically sampled the daemon exactly once,
# immediately. On a cold runner whose chrony has not yet locked
# (live4 segment 2: the daemon sample fell ~1 s into the capture on a
# fresh runner — stratum 0, leap "Not synchronised") that single
# sample derived IMPOSSIBLE and the segment aborted CLOCK_UNPROVEN,
# even though the dispatch lead leaves ~1800 s before the segment's
# window and a fresh runner's daemon typically converges in tens of
# seconds. The wait below polls the daemon's own tracking report
# until it reports synchronised — leap status Normal, the same
# condition the capture records and derive() requires — bounded at
# 300 s. It changes NO threshold, NO derivation and NO gate-evidence
# record: on bound exhaustion it returns and the capture below runs
# exactly as before, sampling the still-unsynchronised daemon and
# deriving IMPOSSIBLE, so an abort is produced by the unchanged code
# path with the unchanged record shape. Poll interval 5 s: a chronyc
# fork/exec costs milliseconds, detection latency is <= 5 s
# (negligible against the 300 s bound and the >1300 s of worst-case
# slack in the dispatch lead), and the daemon is not hammered while
# it converges.
#
# Timing integrity (review closure, Nov 9): the deadline is computed
# on time.monotonic() (elapsed, never wall time), and every poll is
# a run_capture call with an explicit per-call timeout
# (CLOCK_WAIT_POLL_TIMEOUT_S), so a stalled chronyc cannot stretch
# the wait: the loop re-checks the deadline after every poll, hence
# elapsed <= bound_s + one poll timeout. The wait's telemetry
# (waited / polls / outcome) is kept in _LAST_READINESS and the
# supervisor publishes it as a SEPARATE durable diagnostic record
# (cap1-clock-readiness/1, see supervisor._publish_clock_readiness);
# it is not part of the section-C record, whose schema is unchanged.
CLOCK_WAIT_BOUND_S = 300.0
CLOCK_WAIT_POLL_S = 5.0
CLOCK_WAIT_POLL_TIMEOUT_S = 10.0

READINESS_SCHEMA = "cap1-clock-readiness/1"
_LAST_READINESS = None


def _daemon_synchronised():
    """One readiness poll: True iff `chronyc tracking` parses and
    reports leap status Normal. Any failure to obtain or parse the
    report is 'not ready', never an exception. The capture is
    timeout-bounded (run_capture kills a stalled chronyc at
    CLOCK_WAIT_POLL_TIMEOUT_S and reports TIMEOUT), so one poll
    costs at most that many seconds."""
    cap = probe.run_capture(["chronyc", "tracking"],
                            timeout=CLOCK_WAIT_POLL_TIMEOUT_S)
    chrony = probe.parse_chronyc_tracking(cap.get("stdout") or "")
    return bool(chrony) and chrony.get("leap_status") == "Normal"


def wait_for_clock_ready(bound_s=CLOCK_WAIT_BOUND_S,
                         poll_s=CLOCK_WAIT_POLL_S):
    """Poll until the daemon reports synchronised, bounded by
    bound_s on a monotonic elapsed clock
    (elapsed <= bound_s + CLOCK_WAIT_POLL_TIMEOUT_S: the deadline
    is re-checked after each poll, and no poll can run longer than
    its subprocess timeout). Never raises: on exhaustion the caller
    proceeds to the unchanged capture, whose derivation decides the
    outcome exactly as it does today. Records its telemetry in
    _LAST_READINESS and returns it as a dict:
    {"waited_s", "polls", "outcome"} with outcome "converged" or
    "exhausted"."""
    global _LAST_READINESS
    t0 = time.monotonic()
    polls = 0
    while True:
        polls += 1
        if _daemon_synchronised():
            tele = {"waited_s": round(time.monotonic() - t0, 3),
                    "polls": polls, "outcome": "converged",
                    "bound_s": bound_s, "poll_interval_s": poll_s,
                    "poll_timeout_s": CLOCK_WAIT_POLL_TIMEOUT_S}
            _LAST_READINESS = tele
            return tele
        waited = time.monotonic() - t0
        if waited >= bound_s:
            tele = {"waited_s": round(waited, 3), "polls": polls,
                    "outcome": "exhausted", "bound_s": bound_s,
                    "poll_interval_s": poll_s,
                    "poll_timeout_s": CLOCK_WAIT_POLL_TIMEOUT_S}
            _LAST_READINESS = tele
            return tele
        time.sleep(min(poll_s, bound_s - waited))


def last_readiness():
    """Telemetry of the most recent wait_for_clock_ready call IN
    THIS PROCESS, or None if the wait has not run. The supervisor
    publishes it inside the same clock-gate step that invoked the
    section-C provider, so attribution is to that step's capture;
    processes that never call capture_section_c (synthetic-provider
    lab paths) see None and publish nothing."""
    return dict(_LAST_READINESS) if _LAST_READINESS is not None \
        else None


def capture_section_c(drift_window_s=30.0, runner_env=None):
    """Full section-C-shaped record from this host's daemon, following
    the probe's method exactly (captures verbatim, derivation over
    recorded inputs only, IMPOSSIBLE — never approximated — when the
    daemon cannot support it)."""
    # Review fix (F4): never let a previous capture's telemetry leak
    # into this capture's diagnostic publication.
    global _LAST_READINESS
    _LAST_READINESS = None
    wait_for_clock_ready()
    caps = {}
    for name, argv in (
        ("timedatectl_status", ["timedatectl", "status"]),
        ("timedatectl_timesync_status", ["timedatectl",
                                         "timesync-status"]),
        ("chronyc_tracking", ["chronyc", "tracking"]),
        ("chronyc_sources", ["chronyc", "sources", "-v"]),
    ):
        caps[name] = probe.run_capture(argv)
    granularity = probe.measure_granularity_ms()
    drift = probe.measure_drift(drift_window_s)
    chrony = probe.parse_chronyc_tracking(
        caps["chronyc_tracking"].get("stdout") or "")
    tdc = probe.parse_timedatectl(
        caps["timedatectl_status"].get("stdout") or "",
        caps["timedatectl_timesync_status"].get("stdout") or "")
    daemon = None
    if chrony is not None and ("system_time_offset_s" in chrony or
                               "root_dispersion_s" in chrony):
        daemon = {"kind": "chrony",
                  "synchronised":
                      chrony.get("leap_status") == "Normal",
                  "offset_s": chrony.get("system_time_offset_s"),
                  "root_dispersion_s": chrony.get("root_dispersion_s"),
                  "root_delay_s": chrony.get("root_delay_s"),
                  "update_interval_s": chrony.get("update_interval_s"),
                  "stratum": chrony.get("stratum"),
                  "leap_status": chrony.get("leap_status")}
    elif tdc:
        daemon = {"kind": "systemd-timesyncd",
                  "synchronised": tdc.get("system_clock_synchronized"),
                  "offset_s": None, "root_dispersion_s": None,
                  "update_interval_s": None,
                  "server": tdc.get("server")}
    derivation = probe.derive(daemon, granularity, drift)
    return {"schema": SECTION_C_SCHEMA,
            "captured_at_local": probe.now_local_iso(),
            "captures": caps,
            "granularity_ms": granularity,
            "drift": drift,
            "daemon_evidence": daemon,
            "derivation": derivation,
            "runner_env": runner_env or {}}


def summarize_section_c(rec):
    """The manifest-carried summary (design section 5.1, clock field)."""
    d = (rec or {}).get("daemon_evidence") or {}
    der = (rec or {}).get("derivation") or {}
    return {"daemon": d.get("kind"), "stratum": d.get("stratum"),
            "leap_status": d.get("leap_status"),
            "synchronised": d.get("synchronised"),
            "offset_ms": (abs(d["offset_s"]) * 1000.0
                          if d.get("offset_s") is not None else None),
            "root_dispersion_ms": (
                d["root_dispersion_s"] * 1000.0
                if d.get("root_dispersion_s") is not None else None),
            "derivation_status": der.get("status"),
            "derived_bound_ms": der.get("bound_ms")}


class ChronyProbeProvider:
    """Per-capture clock status from live chrony (production form)."""

    def __call__(self):
        try:
            out = subprocess.run(["chronyc", "-c", "tracking"],
                                 capture_output=True, text=True,
                                 timeout=5)
            if out.returncode != 0:
                raise ValueError("chronyc failed")
            f = out.stdout.strip().split(",")
            if f[13].strip() != "Normal":
                raise ValueError("leap status %s" % f[13])
            bound_s = abs(float(f[4])) + float(f[11]) + \
                float(f[10]) / 2.0
            return {"state": "SYNCED",
                    "uncertainty_ms": max(1, math.ceil(bound_s * 1000)),
                    "source": "chrony"}
        except Exception:
            return {"state": "UNKNOWN", "uncertainty_ms": None,
                    "source": "none"}


class ScriptedProbeProvider:
    """Deterministic synthetic provider: cycles a fixed list of
    clock-status dicts (or a single constant)."""

    def __init__(self, sequence):
        if isinstance(sequence, dict):
            sequence = [sequence]
        self.seq = list(sequence)
        self.i = 0

    def __call__(self):
        v = self.seq[min(self.i, len(self.seq) - 1)]
        self.i += 1
        return dict(v)


def synthetic_section_c(bound_ms, *, offset_ms=1.0, dispersion_ms=2.0):
    """A synthetic section-C record with a chosen derived bound, for
    lab/rehearsal gate tests. Labelled synthetic in its own body."""
    return {"schema": SECTION_C_SCHEMA, "synthetic": True,
            "captured_at_local": "SYNTHETIC",
            "captures": {},
            "granularity_ms": 0.001,
            "drift": {"drift_ppm": 0.0, "window_s": 0.0},
            "daemon_evidence": {
                "kind": "synthetic", "synchronised": True,
                "offset_s": offset_ms / 1000.0,
                "root_dispersion_s": dispersion_ms / 1000.0,
                "root_delay_s": 0.0, "update_interval_s": 8.0,
                "stratum": 1, "leap_status": "Normal"},
            "derivation": {
                "status": "DERIVED",
                "formula": "|offset| + root_dispersion + "
                           "drift_allowance + granularity",
                "bound_ms": bound_ms,
                "passes_activation_100ms": bound_ms <= 100.0,
                "passes_per_observation_250ms": bound_ms <= 250.0},
            "runner_env": {}}
