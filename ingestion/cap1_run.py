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

Step 3 dev-area changes (Design L + Capture Discipline; see
step3/CD5_RESOLUTION_NOTE_2026-11-08.md for every timing constant):
L1 Discovery polls status=unopened (was status=open) at the preserved
   900 s cadence; confirmation is positive-only (listing appearance),
   monotone, and deduplicated by ticker at append time.
L2 Imminent-close sweep: status=open checks at open+15/+75/+195 s
   for grid opens whose close this segment owns and whose series are
   not yet confirmed; evaluated at the head of capture_due so every
   tick mode (FULL and CAPTURE_ONLY) sees it.
L3 CD-6: every discovery/sweep poll attempt is appended to the ops
   chain as a DISCOVERY_ATTEMPT record (success/empty/failed). The
   cadence advance on a failed poll is PRESERVED (Design L sec.
   3.2.4: the next round runs 900 s later at worst); the frozen
   defect corrected here is the silence, not the advance.
CD-3 captured() is validity-aware for the OPEN slot only: an INVALID
   observation or a TRANSPORT_FAILURE GAP no longer completes OPEN;
   the OPEN ladder retries at open+20/+45/+90/+150 s (extra
   slot="OPEN" observations with true receipts) until the first
   VALID observation or window expiry at open+185 s (MISSED_SLOT).
   All other slots keep the frozen completion semantics verbatim.
CD-1 Tick.next_wake_ms() exposes the earliest pending capture/sweep
   instant so the supervisor can wake at slot instants instead of
   waiting for the 120 s tick grid (see supervisor.py Step 3 note).
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
# --- Step 3: Design L + CD constants (CD5_RESOLUTION_NOTE_2026-11-08) ---
GRID_MS = 900_000                     # market life; close = open + 900 s
DISCOVERY_STATUS = "unopened"         # Design L discovery filter
SWEEP_STATUS = "open"                 # imminent-close sweep filter
SWEEP_OFFSETS_MS = (15_000, 75_000, 195_000)   # sweep rounds after open
# CD-3 ladder: rung 0 is the frozen OPEN primary (open+5 s); rungs
# 1..4 are the retry ladder. All rungs are slot="OPEN" observations.
OPEN_LADDER_OFFSETS_MS = (5_000, 20_000, 45_000, 90_000, 150_000)
# CD-5 operational target: no FIRST capture is scheduled after
# open+540 s. Structurally, OPEN ladder dues (<=150 s) and T-600
# (+300 s) are the only first-capture-capable dispatches and both
# sit inside it; the frozen T-330 fallback (due +570 s, receipt
# bound +575 s) is the note's named exception, not a first capture.
FIRST_CAPTURE_CUTOFF_MS = 540_000


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def ms_of(s):
    # Routed through cap1.parse_iso_ms (round-2 correction): the
    # runner parses venue-supplied open/close stamps here, and the
    # old strict whole-second strptime was the same crash class as
    # the round-1 updated_time defect. parse_iso_ms is a strict
    # superset on canonical stamps (identical outputs, battery-
    # proven in test_capture_path_live.py) and applies the
    # documented fractional-truncation semantics otherwise.
    return cap1.parse_iso_ms(s)


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
                              "sealed": False, "last_tick_status": "ok",
                              "sweeps_done": []}
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

    def _record_poll_attempt(self, now, channel, series, outcome,
                             n_markets, error=None):
        """CD-6: every poll attempt is append-only evidence in the
        ops chain (the segment's operational chain; STOP records
        already live there). Outcome classes: success (n>0 markets
        listed), empty (poll succeeded, nothing listed), failed
        (governor GAP after the existing retry discipline)."""
        chain = self.chains.get("ops")
        if chain is None:
            return
        chain.append({"kind": "DISCOVERY_ATTEMPT", "channel": channel,
                      "series": series, "outcome": outcome,
                      "n_markets": n_markets, "error": error,
                      "recorded_at_local": iso_of(now)})

    def _poll_series(self, now, channel, status, series_list):
        """One listing poll round over `series_list` via the governor
        (every page is a governed, counted request; pagination is
        consumed completely). Positive confirmation only: a market
        is appended as DISCOVERY only when listed here and not
        already known (monotone, deduplicated by ticker against the
        chains and within this round, across pages and reordered
        responses). Returns the newly confirmed market dicts."""
        seen = set(self.known_markets())
        found = {"cohort": [], "ops": []}
        for series in series_list:
            pages = self.gov.get_all_pages(
                self.transport, "/trade-api/v2/markets",
                {"series_ticker": series, "status": status})
            if pages and pages[0] == "GAP":
                self._record_poll_attempt(now, channel, series,
                                          "failed", 0, str(pages[1]))
                self.errors.append(f"discovery GAP {series}: {pages[1]}")
                continue
            listed = []
            for raw in pages:
                for m in json.loads(raw.decode())["markets"]:
                    listed.append({"ticker": m["ticker"],
                                   "series_ticker": series,
                                   "open_time": m["open_time"],
                                   "close_time": m["close_time"]})
            self._record_poll_attempt(
                now, channel, series,
                "success" if listed else "empty", len(listed))
            for mk in listed:
                if mk["ticker"] in seen:
                    continue
                seen.add(mk["ticker"])
                found[self.segment(mk)].append(mk)
        new = []
        for seg, mkts in found.items():
            if mkts:
                self.chains[seg].append({
                    "kind": "DISCOVERY", "markets": mkts,
                    "recorded_at_local": iso_of(now)})
                new.extend(mkts)
        return new

    def discover(self, now):
        cadence = 900_000 if ms_of(COHORT_LO) - 900_000 <= now < ms_of(
            COHORT_HI) else 1_800_000
        if now - self.state["last_discovery_ms"] < cadence:
            return
        # Design L: the seam now polls status=unopened at the
        # preserved cadence. Ownership filtering / harvest / segment
        # handoff are transport-side (supervisor OwnershipFilter)
        # and unchanged; tick alignment of discovery itself is
        # unchanged (only capture dispatch is de-quantized, CD-1).
        self._poll_series(now, "unopened", DISCOVERY_STATUS, SERIES)
        self.state["last_discovery_ms"] = now

    def sweep(self, now):
        """Design L imminent-close sweep (lead-independent net):
        status=open checks at open+15/+75/+195 s for the grid open
        whose close this segment owns, polling only series with no
        confirmed market opening at that instant. Each due round
        fires at most once (state cache only — confirmations live
        in the chains, so a lost cache can only re-poll, and dedup
        makes a re-poll harmless). Rounds already past when first
        evaluated collapse into a single poll round. Never-created
        closes simply never appear: no DISCOVERY, no GAP, no cohort
        entry is ever produced for them here."""
        open_ms = (now // GRID_MS) * GRID_MS
        close_ms = open_ms + GRID_MS
        # Owner-scoped: only sweep a close this segment owns
        # (COHORT_LO/HI are re-pointed to the segment's close bounds
        # by the supervisor; a successor's close is covered by the
        # harvest channel and by the owner's own sweep in its
        # lead-in, exactly as its discovery is).
        if not ms_of(COHORT_LO) <= close_ms < ms_of(COHORT_HI):
            return
        due = [o for o in SWEEP_OFFSETS_MS if now >= open_ms + o]
        if not due:
            return
        done = set(self.state.get("sweeps_done", []))
        pending = [o for o in due if f"{open_ms}:{o}" not in done]
        if not pending:
            return
        confirmed = {m["series_ticker"]
                     for _seg, m in self.known_markets().values()
                     if ms_of(m["open_time"]) == open_ms}
        missing = [s for s in SERIES if s not in confirmed]
        if missing:
            self._poll_series(now, "open_sweep", SWEEP_STATUS, missing)
        for o in pending:
            done.add(f"{open_ms}:{o}")
        self.state["sweeps_done"] = sorted(done)[-96:]

    def known_markets(self):
        out = {}
        for seg, chain in self.chains.items():
            for r in chain.records:
                if r.get("kind") == "DISCOVERY":
                    for m in r["markets"]:
                        out[m["ticker"]] = (seg, m)
        return out

    def captured(self, chain, ticker):
        """Completed slots. Step 3 (CD-3 / CD5 note sec. 3): for the
        OPEN slot only, completion is judged on VALID observations —
        an INVALID observation or a TRANSPORT_FAILURE GAP leaves
        OPEN pending (the ladder retries it) until a VALID
        observation lands or the window expires (MISSED_SLOT GAP).
        Every other slot keeps the frozen semantics verbatim: any
        OBSERVATION or any GAP under the slot label completes it.
        Record semantics and pit_eligible are untouched."""
        done = set()
        for r in chain.records:
            if r.get("ticker") != ticker:
                continue
            if r.get("kind") == "OBSERVATION":
                if r["slot"] == "OPEN":
                    if r.get("validity") == "VALID":
                        done.add("OPEN")
                else:
                    done.add(r["slot"])
            elif r.get("kind") == "GAP":
                if r["slot"] == "OPEN":
                    if r.get("reason") == "MISSED_SLOT":
                        done.add("OPEN")
                else:
                    done.add(r["slot"])
        return done

    def _open_attempts(self, chain, ticker):
        """OPEN-slot attempts so far (any OBSERVATION under OPEN,
        plus TRANSPORT_FAILURE GAPs under OPEN) — the ladder rung
        index. Derived purely from chain records, so a restart
        re-derives the identical ladder position."""
        n = 0
        for r in chain.records:
            if r.get("ticker") != ticker or r.get("slot") != "OPEN":
                continue
            if r.get("kind") == "OBSERVATION":
                n += 1
            elif r.get("kind") == "GAP" and \
                    r.get("reason") == "TRANSPORT_FAILURE":
                n += 1
        return n

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

    def _capture(self, engines, seg, m, slot, due):
        if seg not in engines:
            engines[seg] = cap1.CaptureEngine(
                self.chains[seg], self.raw[seg],
                self._engine_transport, EngineClock(self._now_fn),
                self.probe_fn, self.bound_ms)
        self._deadline_s = (due + LATENESS_MS) / 1000.0
        engines[seg].capture(m, slot, due)

    def capture_due(self, now):
        # The imminent-close sweep is evaluated here, at the head of
        # the capture path, so it fires in every tick mode (the
        # supervisor's CAPTURE_ONLY ticks call capture_due directly;
        # FULL ticks reach it via Tick.run). A market the sweep
        # confirms is captured below in this same call (its OPEN
        # slot is already due; CD-1 dispatches in the same wake).
        self.sweep(now)
        engines = {}
        for ticker, (seg, m) in sorted(self.known_markets().items()):
            chain, raw = self.chains[seg], self.raw[seg]
            open_ms, close_ms = ms_of(m["open_time"]), ms_of(m["close_time"])
            slots = COHORT_SLOTS if seg == "cohort" else OPS_SLOTS
            done = self.captured(chain, ticker)
            for slot in slots:
                if slot in done:
                    continue
                if slot == "OPEN":
                    # CD-2/CD-3: primary at open+5 s, then ladder
                    # rungs at +20/+45/+90/+150 s while no VALID
                    # observation is held; all rungs are slot="OPEN"
                    # observations with true receipts. The window is
                    # the frozen [due, due+LATENESS_MS] of the
                    # primary (expiry open+185 s).
                    attempts = self._open_attempts(chain, ticker)
                    window_end = open_ms + SLOT_OFFSETS["OPEN"] \
                        + LATENESS_MS
                    if attempts >= len(OPEN_LADDER_OFFSETS_MS):
                        if now > window_end:
                            chain.append({
                                "kind": "GAP", "ticker": ticker,
                                "slot": "OPEN", "reason": "MISSED_SLOT",
                                "recorded_at_local": iso_of(now)})
                        continue
                    due = open_ms + OPEN_LADDER_OFFSETS_MS[attempts]
                    if now < due:
                        continue
                    if now > window_end:
                        chain.append({
                            "kind": "GAP", "ticker": ticker,
                            "slot": "OPEN", "reason": "MISSED_SLOT",
                            "recorded_at_local": iso_of(now)})
                        continue
                    self._capture(engines, seg, m, "OPEN", due)
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
                self._capture(engines, seg, m, slot, due)

    def next_wake_ms(self, now):
        """CD-1: the earliest instant at which capture_due would do
        work (a pending slot's due, the next OPEN ladder rung, an
        OPEN window expiry that must write MISSED_SLOT, or a due
        sweep round), or `now` if work is already due, or None if
        nothing is pending. Pure function of the chains + state, so
        a restart re-derives the identical wake. The supervisor
        sleeps until min(this, its existing tick/heartbeat/chunk
        wakes): discovery and heartbeats keep their cadence; only
        capture dispatch is de-quantized (jitter is then processing
        time, not tick phase)."""
        cands = []
        for ticker, (seg, m) in sorted(self.known_markets().items()):
            chain = self.chains[seg]
            open_ms, close_ms = ms_of(m["open_time"]), \
                ms_of(m["close_time"])
            slots = COHORT_SLOTS if seg == "cohort" else OPS_SLOTS
            done = self.captured(chain, ticker)
            for slot in slots:
                if slot in done:
                    continue
                if slot == "OPEN":
                    attempts = self._open_attempts(chain, ticker)
                    window_end = open_ms + SLOT_OFFSETS["OPEN"] \
                        + LATENESS_MS
                    if attempts < len(OPEN_LADDER_OFFSETS_MS):
                        due = open_ms + \
                            OPEN_LADDER_OFFSETS_MS[attempts]
                        if due <= now <= window_end:
                            return now
                        if due > now:
                            cands.append(due)
                        elif now > window_end:
                            return now  # MISSED_SLOT write is due
                    else:
                        if now > window_end:
                            return now
                        cands.append(window_end + 1)
                    continue
                due = close_ms + SLOT_OFFSETS[slot]
                if due > now:
                    cands.append(due)
                else:
                    return now  # due (or overdue for MISSED_SLOT)
        # Sweep rounds for the current and next grid opens (owned
        # closes only, mirroring sweep()'s owner scope).
        done_sweeps = set(self.state.get("sweeps_done", []))
        cur_open = (now // GRID_MS) * GRID_MS
        for open_ms in (cur_open, cur_open + GRID_MS):
            close_ms = open_ms + GRID_MS
            if not ms_of(COHORT_LO) <= close_ms < ms_of(COHORT_HI):
                continue
            confirmed = {mm["series_ticker"]
                         for _s, mm in self.known_markets().values()
                         if ms_of(mm["open_time"]) == open_ms}
            if len(confirmed) >= len(SERIES):
                continue
            for o in SWEEP_OFFSETS_MS:
                t = open_ms + o
                if t > now and f"{open_ms}:{o}" not in done_sweeps:
                    cands.append(t)
        return min(cands) if cands else None

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
