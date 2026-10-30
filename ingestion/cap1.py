"""CAP-1 — forward strike-capture component (LAB implementation, G-CAP1-BUILD).

Implements the Rev 2 design: existence-based discovery (offline form),
market-anchored capture slots, complete raw-response preservation with
LOCAL/OBSERVED provenance envelopes, hash-chained evidence stores, GAP
and INVALID handling, amendment detection with terminal quarantine, and
health summaries. Eligibility applies the frozen CAP1_CONFIG_V1
clock-uncertainty rule. This module performs NO network I/O: transport
is an injected callable (fixtures/simulator in the lab).
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.clock import fmt_ts

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "cap1_config_v1.json")
SLOTS = ("OPEN", "T-600", "T-330", "T-90", "POST-CLOSE")
WATCHED_PARAMS = ("floor_strike", "open_time", "close_time", "rules_primary")


def _canon(o) -> str:
    return json.dumps(o, sort_keys=True, separators=(",", ":"))


def _chain_hash(prev_hash, record) -> str:
    return hashlib.sha256((prev_hash + _canon(record)).encode()).hexdigest()


def parse_iso_ms(s):
    """Parse a venue ISO-8601 UTC timestamp to epoch milliseconds.

    Accepted shapes (and nothing else):
      YYYY-MM-DDTHH:MM:SSZ
      YYYY-MM-DDTHH:MM:SS.<frac>Z    (<frac> = one or more ASCII digits)

    The fractional part is a decimal fraction of one second. The
    result is the whole-second epoch-millisecond value PLUS the
    fraction truncated (floored) to whole milliseconds: ".5" ->
    +500 ms, ".392551" -> +392 ms, ".0000001" -> +0 ms, ".9999999" ->
    +999 ms; it can never round up into the next second. Whole-second
    inputs return exactly what the previous strptime-based
    implementation returned (a strict superset of the old grammar —
    the venue's live `updated_time` carries microseconds, which the
    old whole-second-only grammar rejected with ValueError and killed
    the 2026-10-30 live activation at its first capture tick).

    Anything malformed raises ValueError: a missing or lowercase "Z",
    a numeric UTC offset, an empty or non-digit fraction, out-of-range
    date/time fields, extra or missing characters, whitespace, or a
    non-string input.
    """
    if not isinstance(s, str):
        raise ValueError("timestamp must be a string: %r" % (s,))
    frac_ms = 0
    body = s
    if len(s) >= 21 and s[19] == ".":
        if not s.endswith("Z"):
            raise ValueError("malformed timestamp: %r" % (s,))
        frac = s[20:-1]
        if not frac or not (frac.isascii() and frac.isdigit()):
            raise ValueError("malformed timestamp fraction: %r" % (s,))
        frac_ms = int((frac + "000")[:3])
        body = s[:19] + "Z"
    if len(body) != 20 or body[4] != "-" or body[7] != "-" \
            or body[10] != "T" or body[13] != ":" or body[16] != ":" \
            or body[19] != "Z":
        raise ValueError("malformed timestamp: %r" % (s,))
    fields = (body[0:4], body[5:7], body[8:10], body[11:13],
              body[14:16], body[17:19])
    if not all(f.isascii() and f.isdigit() for f in fields):
        raise ValueError("malformed timestamp: %r" % (s,))
    year, mon, day, hh, mm, ss = (int(f) for f in fields)
    # datetime construction is the range check: month 13, day 32,
    # hour 24, second 60, year 0, ... all raise ValueError here.
    dt = datetime(year, mon, day, hh, mm, ss, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000) + frac_ms


def load_config(path=CONFIG_PATH):
    raw = open(path, "rb").read()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


class ChainStore:
    """One hash-chained JSONL evidence store (single writer)."""

    def __init__(self, path):
        self.path = path
        self.records = []
        self.tip = "GENESIS"
        if os.path.exists(path):
            self._load_and_verify()

    def _load_and_verify(self):
        prev = "GENESIS"
        with open(self.path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    env = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"chain corrupt at line {i}: {e}")
                if env.get("prev_hash") != prev or \
                        _chain_hash(prev, env["record"]) != env.get("hash"):
                    raise ValueError(f"chain break at line {i}")
                prev = env["hash"]
                self.records.append(env["record"])
        self.tip = prev

    def append(self, record):
        env = {"seq": len(self.records) + 1, "prev_hash": self.tip,
               "record": record}
        env["hash"] = _chain_hash(self.tip, record)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(env, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.records.append(record)
        self.tip = env["hash"]
        return env["hash"]


class RawStore:
    """Complete raw response bytes, base64-carried, separately chained."""

    def __init__(self, path):
        self.path = path
        self.payloads = {}
        self.tip = "GENESIS"
        if os.path.exists(path):
            prev = "GENESIS"
            with open(path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    env = json.loads(line)
                    rec = env["record"]
                    if env.get("prev_hash") != prev or \
                            _chain_hash(prev, rec) != env.get("hash"):
                        raise ValueError(f"raw chain break at line {i}")
                    import base64
                    self.payloads[rec["payload_sha256"]] = base64.b64decode(
                        rec["payload_b64"])
                    prev = env["hash"]
            self.tip = prev

    def put(self, payload: bytes) -> str:
        import base64
        digest = hashlib.sha256(payload).hexdigest()
        if digest in self.payloads:
            return digest
        rec = {"payload_sha256": digest,
               "payload_b64": base64.b64encode(payload).decode()}
        env = {"seq": len(self.payloads) + 1, "prev_hash": self.tip,
               "record": rec}
        env["hash"] = _chain_hash(self.tip, rec)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(env, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.payloads[digest] = payload
        self.tip = env["hash"]
        return digest


def build_observation(*, ticker, series, slot, endpoint, params, status,
                      raw: bytes, send_ms, receipt_ms, clock_status,
                      bound_ms, payload_sha256):
    """Assemble one observation record with its provenance envelope and
    validity assessment (design sec. 3D)."""
    obs = {"kind": "OBSERVATION", "ticker": ticker, "series": series,
           "slot": slot, "endpoint": endpoint, "params": params,
           "http_status": status, "payload_sha256": payload_sha256,
           "send_local_ms": send_ms, "receipt_local_ms": receipt_ms,
           "venue_observed": None, "clock_status": clock_status,
           "market": None, "validity": "VALID", "invalid_reason": None}
    if hashlib.sha256(raw).hexdigest() != payload_sha256:
        obs["validity"], obs["invalid_reason"] = "INVALID", "PAYLOAD_HASH_MISMATCH"
        return obs
    try:
        payload = json.loads(raw.decode("utf-8"))
        market = payload["markets"][0] if "markets" in payload else payload
    except Exception:
        obs["validity"], obs["invalid_reason"] = "INVALID", "UNPARSEABLE"
        return obs
    obs["market"] = {k: market.get(k) for k in
                     ("ticker", "series_ticker", "status", "open_time",
                      "close_time", "floor_strike", "strike_type",
                      "rules_primary", "updated_time")}
    if market.get("updated_time"):
        obs["venue_observed"] = {"updated_time": market["updated_time"]}
    if receipt_ms < send_ms:
        obs["validity"], obs["invalid_reason"] = "INVALID", "TIMESTAMP_ORDER"
    elif clock_status.get("state") != "SYNCED":
        obs["validity"], obs["invalid_reason"] = "INVALID", "CLOCK_UNKNOWN"
    elif clock_status.get("uncertainty_ms") is None:
        obs["validity"], obs["invalid_reason"] = "INVALID", "CLOCK_UNMEASURED"
    elif clock_status["uncertainty_ms"] > bound_ms:
        obs["validity"], obs["invalid_reason"] = "INVALID", "CLOCK_EXCESSIVE"
    elif obs["venue_observed"] is not None:
        venue_ms = parse_iso_ms(obs["venue_observed"]["updated_time"])
        if venue_ms - receipt_ms > clock_status["uncertainty_ms"]:
            obs["validity"], obs["invalid_reason"] = \
                "INVALID", "VENUE_TS_INCONSISTENT"
    if obs["validity"] == "VALID" and market.get("floor_strike") is None:
        obs["validity"], obs["invalid_reason"] = "INVALID", "MISSING_FIELD"
    return obs


def slot_times(market):
    open_ms = parse_iso_ms(market["open_time"])
    close_ms = parse_iso_ms(market["close_time"])
    return {"OPEN": open_ms + 5000, "T-600": close_ms - 600_000,
            "T-330": close_ms - 330_000, "T-90": close_ms - 90_000,
            "POST-CLOSE": close_ms + 60_000}


class CaptureEngine:
    """Drives captures over an injected transport + injected clock.
    transport(endpoint, params) -> (status, raw_bytes). clock.now_ms()
    yields successive LOCAL instants (send, then receipt)."""

    def __init__(self, chain: ChainStore, raw: RawStore, transport, clock,
                 clock_status_fn, bound_ms):
        self.chain, self.raw = chain, raw
        self.transport, self.clock = transport, clock
        self.clock_status_fn, self.bound_ms = clock_status_fn, bound_ms

    def capture(self, market, slot, at_ms):
        # The documented single-market filter is "tickers" (plural).
        # The singular "ticker" parameter is IGNORED by the venue,
        # which returns its default 100-market page; build_observation
        # would then consume markets[0] — the wrong market under this
        # market's label (proven live, Phase D 2026-10-30).
        params = {"tickers": market["ticker"]}
        send_ms = self.clock.now_ms()
        try:
            status, raw = self.transport("/markets", params)
        except Exception:
            self.chain.append({"kind": "GAP", "ticker": market["ticker"],
                               "series": market["series_ticker"],
                               "slot": slot, "scheduled_ms": at_ms,
                               "reason": "TRANSPORT_FAILURE"})
            return None
        receipt_ms = self.clock.now_ms()
        digest = self.raw.put(raw)
        obs = build_observation(
            ticker=market["ticker"], series=market["series_ticker"],
            slot=slot, endpoint="/markets", params=params, status=status,
            raw=raw, send_ms=send_ms, receipt_ms=receipt_ms,
            clock_status=self.clock_status_fn(), bound_ms=self.bound_ms,
            payload_sha256=digest)
        self.chain.append(obs)
        return obs

    def run_market(self, market):
        times = slot_times(market)
        out = []
        for slot in SLOTS:
            out.append(self.capture(market, slot, times[slot]))
        detect_amendments(self.chain, market["ticker"])
        return out


def detect_amendments(chain: ChainStore, ticker):
    """Compare successive VALID observations; any watched-parameter change
    appends an AMENDMENT event + QUARANTINE record (terminal, sec. 3E)."""
    if is_quarantined(chain, ticker):
        return []  # already terminal; no further events, no resume
    seen = {}
    events = []
    for rec in chain.records:
        if rec.get("kind") != "OBSERVATION" or rec["ticker"] != ticker \
                or rec["validity"] != "VALID":
            continue
        m = rec["market"]
        for param in WATCHED_PARAMS:
            value = m.get(param)
            if param in seen and seen[param] != value:
                chain.append({"kind": "AMENDMENT", "ticker": ticker,
                              "parameter": param, "prior": seen[param],
                              "new": value, "detected_in_slot": rec["slot"]})
                chain.append({"kind": "QUARANTINE", "ticker": ticker,
                              "reason": f"AMENDMENT:{param}"})
                events.append(param)
                return events
            seen[param] = value
    return events


def is_quarantined(chain: ChainStore, ticker) -> bool:
    return any(r.get("kind") == "QUARANTINE" and r["ticker"] == ticker
               for r in chain.records)


def pit_eligible(chain: ChainStore, ticker, decision_ms, bound_ms):
    """Design sec. B + frozen config rule: a VALID, non-quarantined,
    strike-consistent observation with receipt + uncertainty <= decision
    instant (and uncertainty <= bound) makes the market eligible."""
    if is_quarantined(chain, ticker):
        return {"eligible": False, "reason": "QUARANTINED"}
    strikes = set()
    best = None
    for rec in chain.records:
        if rec.get("kind") != "OBSERVATION" or rec["ticker"] != ticker \
                or rec["validity"] != "VALID":
            continue
        unc = rec["clock_status"].get("uncertainty_ms")
        if unc is None or unc > bound_ms:
            continue
        if rec["receipt_local_ms"] + unc <= decision_ms:
            strikes.add(rec["market"]["floor_strike"])
            best = rec
    if best is None:
        return {"eligible": False, "reason": "NO_VALID_OBSERVATION_IN_TIME"}
    if len(strikes) > 1:
        return {"eligible": False, "reason": "CONTRADICTORY_STRIKES"}
    return {"eligible": True, "reason": None,
            "strike": best["market"]["floor_strike"],
            "receipt_local_ms": best["receipt_local_ms"]}


def health_summary(chain: ChainStore):
    counts = {}
    for r in chain.records:
        key = r.get("kind")
        if key == "OBSERVATION":
            key = f"OBSERVATION_{r['validity']}"
        counts[key] = counts.get(key, 0) + 1
    counts["tip_hash"] = chain.tip
    return counts
