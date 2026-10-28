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


def capture_section_c(drift_window_s=30.0, runner_env=None):
    """Full section-C-shaped record from this host's daemon, following
    the probe's method exactly (captures verbatim, derivation over
    recorded inputs only, IMPOSSIBLE — never approximated — when the
    daemon cannot support it)."""
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
