#!/usr/bin/env python3
"""CAP-1 GitHub Actions clock-evidence probe.

Purpose: settle, empirically, whether a GitHub-hosted runner can produce a
section-C-shaped clock-readiness evidence record (frozen spec: sync source and
health identified; the daemon's own offset estimate and uncertainty / root
dispersion figures with LOCAL timestamps; a derivation over recorded numbers
whose bound must be <= 100 ms for activation, with the frozen per-observation
maximum of 250 ms).

Design rules (inherited from the CAP-1 evidence discipline):
- Pure standard library. No network calls except the explicitly labelled
  UDP/123 diagnostic (which is NOT daemon evidence and never feeds the
  derivation).
- Every captured timestamp is LOCAL (this host's wall clock) unless a field
  name says otherwise. Raw command outputs are preserved verbatim.
- Fail honest: if the daemon is absent, unsynchronised, or does not expose an
  offset estimate AND a dispersion figure, the derivation is recorded as
  IMPOSSIBLE with the reason, never approximated.
- The script never raises on a missing tool; absence is itself evidence.

Output: one JSON document (path from --out, default clock_probe_evidence.json).
"""

import argparse
import json
import os
import platform
import re
import socket
import struct
import subprocess
import sys
import time

PROBE_VERSION = "cap1-gha-clock-probe/1"
ACTIVATION_BOUND_MS = 100.0          # frozen section-C activation threshold
PER_OBSERVATION_MAX_MS = 250.0       # frozen CAP1_CONFIG_V1 maximum
NTP_SERVERS = ["time.cloudflare.com", "time.google.com", "pool.ntp.org"]
NTP_EPOCH_DELTA = 2208988800  # seconds between 1900-01-01 and 1970-01-01


def now_local_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + (
        ".%03dZ" % int((time.time() % 1) * 1000))


def run_capture(argv, timeout=15):
    """Run a read-only command; return a record with verbatim output."""
    rec = {"argv": argv, "captured_at_local": now_local_iso()}
    try:
        p = subprocess.run(argv, capture_output=True, text=True,
                           timeout=timeout)
        rec["exit_code"] = p.returncode
        rec["stdout"] = p.stdout
        rec["stderr"] = p.stderr
    except FileNotFoundError:
        rec["error"] = "TOOL_ABSENT"
    except subprocess.TimeoutExpired:
        rec["error"] = "TIMEOUT"
    except Exception as e:  # absence/failure is evidence, never fatal
        rec["error"] = "%s: %s" % (type(e).__name__, e)
    return rec


def measure_granularity_ms(samples=2000):
    """Smallest nonzero wall-clock step observable from Python, in ms."""
    best = None
    prev = time.time_ns()
    for _ in range(samples):
        cur = time.time_ns()
        d = cur - prev
        if d > 0:
            if best is None or d < best:
                best = d
            prev = cur
    return (best / 1e6) if best is not None else None


def measure_drift(window_s=30.0):
    """Wall-vs-monotonic drift over the window; returns ppm and endpoints."""
    w0, m0 = time.time(), time.monotonic()
    time.sleep(window_s)
    w1, m1 = time.time(), time.monotonic()
    wall = w1 - w0
    mono = m1 - m0
    ppm = ((wall - mono) / mono * 1e6) if mono > 0 else None
    return {"window_s": window_s, "wall_elapsed_s": wall,
            "monotonic_elapsed_s": mono, "drift_ppm": ppm,
            "started_at_local": None, "ended_at_local": now_local_iso()}


def ntp_udp_diagnostic(server, timeout=3.0):
    """One raw NTP client exchange over UDP/123.

    DIAGNOSTIC ONLY: establishes whether outbound UDP/123 works from this
    host at all. The Cristian offset computed here is not daemon evidence
    and is never used in the section-C derivation.
    """
    rec = {"server": server, "checked_at_local": now_local_iso()}
    try:
        addr = socket.gethostbyname(server)
    except Exception as e:
        rec["result"] = "DNS_FAIL: %s" % e
        return rec
    rec["resolved_ip"] = addr
    pkt = bytearray(48)
    pkt[0] = 0x1B  # LI=0, VN=3, Mode=3 (client)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        t0 = time.time()
        s.sendto(bytes(pkt), (addr, 123))
        data, _ = s.recvfrom(512)
        t3 = time.time()
        rec["result"] = "RESPONSE"
        rec["rtt_ms"] = (t3 - t0) * 1000.0
        if len(data) >= 48:
            sec, frac = struct.unpack("!II", data[40:48])
            server_tx = (sec - NTP_EPOCH_DELTA) + frac / 2**32
            rec["cristian_offset_ms_diagnostic_only"] = (
                (server_tx - (t0 + t3) / 2.0) * 1000.0)
            rec["stratum"] = data[1]
        else:
            rec["note"] = "short response (%d bytes)" % len(data)
    except socket.timeout:
        rec["result"] = "NO_RESPONSE_TIMEOUT"
    except PermissionError as e:
        rec["result"] = "SEND_DENIED: %s" % e
    except Exception as e:
        rec["result"] = "ERROR: %s: %s" % (type(e).__name__, e)
    finally:
        s.close()
    return rec


def _float_after(text, label):
    m = re.search(re.escape(label) + r"\s*:\s*(-?[0-9]+(?:\.[0-9]+)?)", text)
    return float(m.group(1)) if m else None


def parse_chronyc_tracking(stdout):
    """Best-effort parse of `chronyc tracking`; raw output stays primary."""
    if not stdout:
        return None
    out = {}
    m = re.search(r"System time\s*:\s*([0-9.]+) seconds (slow|fast)", stdout)
    if m:
        val = float(m.group(1)) * (1 if m.group(2) == "fast" else -1)
        out["system_time_offset_s"] = val
    out["root_dispersion_s"] = _float_after(stdout, "Root dispersion")
    out["root_delay_s"] = _float_after(stdout, "Root delay")
    out["update_interval_s"] = _float_after(stdout, "Update interval")
    m = re.search(r"Leap status\s*:\s*(.+)", stdout)
    if m:
        out["leap_status"] = m.group(1).strip()
    m = re.search(r"Stratum\s*:\s*([0-9]+)", stdout)
    if m:
        out["stratum"] = int(m.group(1))
    return out


def parse_timedatectl(status_out, timesync_out):
    out = {}
    if status_out:
        m = re.search(r"System clock synchronized:\s*(\w+)", status_out)
        if m:
            out["system_clock_synchronized"] = (m.group(1) == "yes")
        m = re.search(r"NTP service:\s*(.+)", status_out)
        if m:
            out["ntp_service"] = m.group(1).strip()
    if timesync_out:
        # systemd >= 250 prints an "Offset:" line in timesync-status when the
        # daemon has a current estimate; absence is recorded, not assumed.
        off = _float_after(timesync_out, "Offset")
        if off is not None:
            out["timesync_offset_raw"] = off
        m = re.search(r"Server:\s*(.+)", timesync_out)
        if m:
            out["server"] = m.group(1).strip()
        m = re.search(r"Poll interval:\s*(.+)", timesync_out)
        if m:
            out["poll_interval"] = m.group(1).strip()
    return out


def derive(daemon, granularity_ms, drift):
    """Section-C derivation over recorded inputs only.

    bound = |daemon offset| + root dispersion + drift allowance + granularity.
    Drift allowance = |measured drift ppm| x update interval when the daemon
    reports one; otherwise the derivation cannot be certified.
    """
    d = {"formula": "|offset| + root_dispersion + drift_allowance + granularity",
         "activation_threshold_ms": ACTIVATION_BOUND_MS,
         "per_observation_max_ms": PER_OBSERVATION_MAX_MS}
    if daemon is None:
        d["status"] = "IMPOSSIBLE"
        d["reason"] = "no daemon evidence captured"
        return d
    if daemon.get("synchronised") is not True:
        d["status"] = "IMPOSSIBLE"
        d["reason"] = "daemon not synchronised (or sync state unproven)"
        return d
    off = daemon.get("offset_s")
    disp = daemon.get("root_dispersion_s")
    if off is None or disp is None:
        d["status"] = "IMPOSSIBLE"
        d["reason"] = ("daemon does not expose both an offset estimate and a "
                       "root-dispersion figure in captured output")
        return d
    upd = daemon.get("update_interval_s")
    ppm = drift.get("drift_ppm")
    if upd is None or ppm is None:
        d["status"] = "IMPOSSIBLE"
        d["reason"] = "drift allowance unbounded (no update interval or drift measure)"
        return d
    drift_allow_ms = abs(ppm) * 1e-6 * upd * 1000.0
    gran = granularity_ms if granularity_ms is not None else 0.0
    bound = abs(off) * 1000.0 + disp * 1000.0 + drift_allow_ms + gran
    d.update({
        "status": "DERIVED",
        "offset_ms": abs(off) * 1000.0,
        "root_dispersion_ms": disp * 1000.0,
        "drift_allowance_ms": drift_allow_ms,
        "granularity_ms": gran,
        "bound_ms": bound,
        "passes_activation_100ms": bound <= ACTIVATION_BOUND_MS,
        "passes_per_observation_250ms": bound <= PER_OBSERVATION_MAX_MS,
    })
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="clock_probe_evidence.json")
    ap.add_argument("--drift-window", type=float, default=30.0)
    args = ap.parse_args()

    ev = {"probe": PROBE_VERSION, "started_at_local": now_local_iso(),
          "timestamps": "all timestamps LOCAL host wall clock unless named"}

    ev["environment"] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": sys.version,
        "runner_env": {k: os.environ.get(k) for k in
                       ("RUNNER_OS", "RUNNER_ARCH", "RUNNER_NAME",
                        "ImageOS", "ImageVersion", "GITHUB_RUN_ID",
                        "GITHUB_WORKFLOW") if os.environ.get(k)},
    }

    caps = {}
    for name, argv in (
        ("uname", ["uname", "-a"]),
        ("os_release", ["cat", "/etc/os-release"]),
        ("timedatectl_status", ["timedatectl", "status"]),
        ("timedatectl_timesync_status", ["timedatectl", "timesync-status"]),
        ("chronyc_tracking", ["chronyc", "tracking"]),
        ("chronyc_sources", ["chronyc", "sources", "-v"]),
        ("systemctl_chrony", ["systemctl", "status", "chrony",
                              "--no-pager"]),
        ("systemctl_timesyncd", ["systemctl", "status",
                                 "systemd-timesyncd", "--no-pager"]),
    ):
        caps[name] = run_capture(argv)
    ev["captures_start"] = caps

    ev["granularity_ms"] = measure_granularity_ms()
    ev["udp123_diagnostic"] = [ntp_udp_diagnostic(s) for s in NTP_SERVERS]
    drift = measure_drift(args.drift_window)
    ev["drift"] = drift

    # Re-capture the daemon state after the drift window (stability check).
    ev["captures_end"] = {
        "timedatectl_status": run_capture(["timedatectl", "status"]),
        "timedatectl_timesync_status": run_capture(
            ["timedatectl", "timesync-status"]),
        "chronyc_tracking": run_capture(["chronyc", "tracking"]),
    }

    # Daemon evidence assembly: chrony preferred (it exposes root
    # dispersion); timesyncd used only insofar as its captured output
    # actually carries the required figures.
    daemon = None
    chrony = parse_chronyc_tracking(
        caps["chronyc_tracking"].get("stdout") or "")
    tdc = parse_timedatectl(caps["timedatectl_status"].get("stdout") or "",
                            caps["timedatectl_timesync_status"].get(
                                "stdout") or "")
    if chrony is not None and ("system_time_offset_s" in chrony or
                               "root_dispersion_s" in chrony):
        daemon = {
            "kind": "chrony",
            "synchronised": (chrony.get("leap_status") == "Normal"),
            "offset_s": chrony.get("system_time_offset_s"),
            "root_dispersion_s": chrony.get("root_dispersion_s"),
            "update_interval_s": chrony.get("update_interval_s"),
            "stratum": chrony.get("stratum"),
        }
    elif tdc:
        daemon = {
            "kind": "systemd-timesyncd",
            "synchronised": tdc.get("system_clock_synchronized"),
            "offset_s": None,          # timesync-status exposes no signed
            "root_dispersion_s": None,  # offset/dispersion pair in general
            "update_interval_s": None,
            "server": tdc.get("server"),
        }
    ev["daemon_evidence"] = daemon
    ev["derivation"] = derive(daemon, ev["granularity_ms"], drift)
    ev["finished_at_local"] = now_local_iso()

    with open(args.out, "w") as f:
        json.dump(ev, f, indent=2, sort_keys=True)
    print("probe complete -> %s" % args.out)
    print("daemon:", json.dumps(daemon))
    print("derivation:", json.dumps(ev["derivation"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
