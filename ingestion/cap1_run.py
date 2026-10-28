"""CAP-1 tick collector (G-COLL-FWD Rev 3) — CORRECTED (patch candidate,
NOT APPLIED to the lab tree; see PATCH_NOTES.md).

State-driven: each invocation is one tick — discover (segment cadence),
capture due slots into the segregated cohort / ops chains, enforce
stops, exit. ACTIVATION GUARD: without a window-freeze record whose
TV PASS, clock-readiness PASS, and code/config hashes match the files
on disk, exits 4 before any I/O. A STOP, once recorded, is terminal:
later ticks exit 5 (no auto-restart).

Corrections vs 590cf67b (all confined to the CaptureEngine seam):
C1 Engine constructed with its real signature (chain, raw, transport,
   clock, clock_status_fn, bound_ms); clock is a now_ms() object.
C2 capture(market, slot, at_ms) replaces the nonexistent capture_slot;
   the full market dict is passed.
C3 Discovery records carry series_ticker (the engine requires it).
C4 Transport adapter implements the engine's contract
   transport("/markets", {"ticker": ...}) -> (status, raw) over the
   governor + allowlisted HTTP transport, with the slot deadline.
C5 Governor StopConditions escape the engine's broad Exception handler
   via a BaseException shim and become terminal STOPs (never GAPs).
C6 Clock status is probed per capture (clock_status_fn), not once/tick.
C7 Tick accepts injected transport/probe/time for offline E2E testing.
C8 Chain verification is construction-time in cap1.py (no verify()
   method): corrupt stores are construction errors -> terminal
   state-only STOP; seal re-verifies by fresh construction.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion import cap1, cap1_transport as ct, cap1_verdict

LAB = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ING = os.path.join(LAB, "ingestion")
WINDOW_DIR = os.path.join(LAB, "cap1_window")
FREEZE_PATH = os.path.join(WINDOW_DIR, "FREEZE.json")
STATE_PATH = os.path.join(WINDOW_DIR, "state.json")
STORES = {"cohort": os.path.join(WINDOW_DIR, "cohort_chain.json"),
          "ops": os.path.join(WINDOW_DIR, "ops_chain.json")}
RAW_DIRS = {"cohort": os.path.join(WINDOW_DIR, "raw_cohort"),
            "ops": os.path.join(WINDOW_DIR, "raw_ops")}
SERIES = ("KXBTC15M", "KXETH15M", "KXSOL15M")
COHORT_LO, COHORT_HI = "2026-10-28T00:00:00Z", "2026-10-30T00:00:00Z"
HARD_END = "2026-10-29T23:59:59Z"
STORAGE_CAP = 500 * 1024 * 1024
LATENESS_MS = 180_000
COHORT_SLOTS = ("OPEN", "T-600", "T-330", "T-90", "POST-CLOSE")
OPS_SLOTS = ("OPEN", "T-330", "T-90")
SLOT_OFFSETS = {"OPEN": 5_000, "T-600": -600_000, "T-330": -330_000,
                "T-90": -90_000, "POST-CLOSE": 60_000}


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def ms_of(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp() * 1000)


def iso_of(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def load_freeze():
    if not os.path.exists(FREEZE_PATH):
        return None, "no freeze record"
    fz = json.load(open(FREEZE_PATH))
    checks = {"tv_pass": fz.get("tv_pass") is True,
              "clock_pass": fz.get("clock_pass") is True,
              "transport_hash": fz.get("transport_sha256") == sha(
                  os.path.join(ING, "cap1_transport.py")),
              "cap1_hash": fz.get("cap1_sha256") == sha(
                  os.path.join(ING, "cap1.py")),
              "config_hash": fz.get("config_sha256") == sha(
                  os.path.join(ING, "cap1_config_v1.json"))}
    bad = [k for k, v in checks.items() if not v]
    return (fz, None) if not bad else (None, f"freeze failed: {bad}")


def clock_probe():
    """Per-observation uncertainty from live chrony tracking:
    |system offset| + root dispersion + root delay/2, ceiling to ms.
    UNKNOWN (fail closed) unless chrony reports a normal leap status."""
    import math
    try:
        out = subprocess.run(["chronyc", "-c", "tracking"],
                             capture_output=True, text=True, timeout=5)
        if out.returncode != 0:
            raise ValueError("chronyc failed")
        f = out.stdout.strip().split(",")
        if f[13].strip() != "Normal":
            raise ValueError(f"leap status {f[13]}")
        bound_s = abs(float(f[4])) + float(f[11]) + float(f[10]) / 2.0
        return {"state": "SYNCED",
                "uncertainty_ms": max(1, math.ceil(bound_s * 1000)),
                "source": "chrony"}
    except Exception:
        return {"state": "UNKNOWN", "uncertainty_ms": None,
                "source": "none"}


def dir_bytes(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


class StopSignal(BaseException):
    """C5: carries a governor StopCondition across the engine's broad
    `except Exception` so a mandatory stop can never become a GAP."""

    def __init__(self, cond):
        super().__init__(cond.code)
        self.cond = cond


class EngineClock:
    """C1: the engine's clock role — successive now_ms() readings."""

    def __init__(self, now_fn):
        self._now_fn = now_fn

    def now_ms(self):
        return int(self._now_fn() * 1000)


class Tick:
    def __init__(self, fz, transport=None, probe_fn=None,
                 now_fn=time.time, sleep_fn=time.sleep):
        self.fz = fz
        self._now_fn, self._sleep_fn = now_fn, sleep_fn
        self.state = json.load(open(STATE_PATH)) if os.path.exists(
            STATE_PATH) else {"gov": None, "last_discovery_ms": 0,
                              "restarts": {}, "stopped": None,
                              "sealed": False, "last_tick_status": "ok"}
        # C8: ChainStore/RawStore verify AT CONSTRUCTION (no verify()
        # method exists); a corrupt store raises ValueError here and is
        # carried as a construction error -> terminal state-only STOP.
        self.chains, self.raw, self.chain_errors = {}, {}, []
        for k, p in STORES.items():
            try:
                self.chains[k] = cap1.ChainStore(p)
            except Exception as e:
                self.chain_errors.append(f"{k}: {e}")
        for k, d in RAW_DIRS.items():
            try:
                self.raw[k] = cap1.RawStore(d)
            except Exception as e:
                self.chain_errors.append(f"raw-{k}: {e}")
        self.gov = ct.Governor(now_fn, sleep_fn, state=self.state["gov"])
        self.transport = transport if transport is not None \
            else ct.HttpTransport()
        self.probe_fn = probe_fn if probe_fn is not None else clock_probe
        self.bound_ms = cap1.load_config()[0]["max_clock_uncertainty_ms"]
        self._deadline_s = None
        self.errors = []

    def _now_ms(self):
        return int(self._now_fn() * 1000)

    def save(self):
        self.state["gov"] = self.gov.state
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f)
        os.replace(tmp, STATE_PATH)

    def segment(self, market):
        c = ms_of(market["close_time"])
        return "cohort" if ms_of(COHORT_LO) <= c < ms_of(COHORT_HI) else "ops"

    def stop(self, code, msg):
        self.state["stopped"] = {"code": code, "msg": msg,
                                 "at": iso_of(self._now_ms())}
        if "ops" in self.chains:  # a corrupt chain is never appended to
            self.chains["ops"].append({"kind": "STOP", "code": code,
                                       "msg": msg,
                                       "recorded_at_local": iso_of(
                                           self._now_ms())})
        self.save()

    def discover(self, now):
        cadence = 900_000 if ms_of(COHORT_LO) - 900_000 <= now < ms_of(
            COHORT_HI) else 1_800_000
        if now - self.state["last_discovery_ms"] < cadence:
            return
        found = {"cohort": [], "ops": []}
        for series in SERIES:
            pages = self.gov.get_all_pages(
                self.transport, "/trade-api/v2/markets",
                {"series_ticker": series, "status": "open"})
            if pages and pages[0] == "GAP":
                self.errors.append(f"discovery GAP {series}: {pages[1]}")
                continue
            for raw in pages:
                for m in json.loads(raw.decode())["markets"]:
                    mk = {"ticker": m["ticker"],
                          "series_ticker": series,
                          "open_time": m["open_time"],
                          "close_time": m["close_time"]}
                    found[self.segment(mk)].append(mk)
        for seg, mkts in found.items():
            if mkts:
                self.chains[seg].append({
                    "kind": "DISCOVERY", "markets": mkts,
                    "recorded_at_local": iso_of(now)})
        self.state["last_discovery_ms"] = now

    def known_markets(self):
        out = {}
        for seg, chain in self.chains.items():
            for r in chain.records:
                if r.get("kind") == "DISCOVERY":
                    for m in r["markets"]:
                        out[m["ticker"]] = (seg, m)
        return out

    def captured(self, chain, ticker):
        done = set()
        for r in chain.records:
            if r.get("kind") == "OBSERVATION" and r["ticker"] == ticker:
                done.add(r["slot"])
            if r.get("kind") == "GAP" and r["ticker"] == ticker:
                done.add(r["slot"])
        return done

    def _engine_transport(self, endpoint, params):
        """C4: the engine's transport contract over governor + HTTP."""
        if endpoint != "/markets":
            raise RuntimeError(f"unexpected engine endpoint {endpoint}")
        try:
            res = self.gov.fetch(self.transport, "/trade-api/v2/markets",
                                 params, deadline_s=self._deadline_s)
        except ct.StopCondition as e:
            raise StopSignal(e)
        if res[0] == "GAP":
            raise RuntimeError(res[1])
        return res

    def capture_due(self, now):
        engines = {}
        for ticker, (seg, m) in sorted(self.known_markets().items()):
            chain, raw = self.chains[seg], self.raw[seg]
            open_ms, close_ms = ms_of(m["open_time"]), ms_of(m["close_time"])
            slots = COHORT_SLOTS if seg == "cohort" else OPS_SLOTS
            done = self.captured(chain, ticker)
            for slot in slots:
                if slot in done:
                    continue
                base = open_ms if slot == "OPEN" else close_ms
                due = base + SLOT_OFFSETS[slot]
                if now < due:
                    continue
                if now > due + LATENESS_MS:
                    chain.append({"kind": "GAP", "ticker": ticker,
                                  "slot": slot, "reason": "MISSED_SLOT",
                                  "recorded_at_local": iso_of(now)})
                    continue
                if seg not in engines:
                    engines[seg] = cap1.CaptureEngine(
                        chain, raw, self._engine_transport,
                        EngineClock(self._now_fn), self.probe_fn,
                        self.bound_ms)
                self._deadline_s = (due + LATENESS_MS) / 1000.0
                engines[seg].capture(m, slot, due)

    def run(self):
        now = self._now_ms()
        if self.state.get("stopped"):
            return 5
        if self.state.get("sealed"):
            return 0
        if now > ms_of(HARD_END):
            return self.seal()
        if self.chain_errors:
            self.stop("CHAIN_BREAK", "; ".join(self.chain_errors))
            return 5
        if dir_bytes(WINDOW_DIR) > STORAGE_CAP:
            self.stop("BUDGET_BREACH", "storage cap")
            return 5
        try:
            self.discover(now)
            self.capture_due(now)
        except StopSignal as e:
            self.stop(e.cond.code, str(e.cond))
            return 5
        except ct.StopCondition as e:
            self.stop(e.code, str(e))
            return 5
        self.state["last_tick_status"] = "error" if self.errors else "ok"
        self.save()
        return 0

    def seal(self):
        tips = {}
        for seg, p in STORES.items():
            try:  # fresh construction = independent re-verification
                tips[seg] = {"ok": True, "tip": cap1.ChainStore(p).tip}
            except Exception as e:
                tips[seg] = {"ok": False, "tip": f"UNVERIFIABLE: {e}"}
        v = cap1_verdict.verdict(STORES["cohort"], COHORT_LO, COHORT_HI)
        seal = {"sealed_at": iso_of(self._now_ms()),
                "tips": tips, "pt1l": v}
        with open(os.path.join(WINDOW_DIR, "SEAL.json"), "w") as f:
            json.dump(seal, f, indent=1, sort_keys=True)
        self.state["sealed"] = True
        self.save()
        print(json.dumps({"sealed": True, "pt1l_verdict": v["verdict"]}))
        return 0


def main():
    fz, err = load_freeze()
    if fz is None:
        print(json.dumps({"activation": "BLOCKED", "reason": err}))
        return 4
    return Tick(fz).run()


if __name__ == "__main__":
    sys.exit(main())
