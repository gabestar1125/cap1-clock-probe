"""Synthetic venue + request instrumentation (lab/rehearsal only).

The stub serves Kalshi-shaped responses for fixture markets through
the frozen HttpTransport's connector seam — the exact seam the frozen
E2E suite uses. It makes ZERO network calls. Fixture universes are
generated on the frozen 900-second close grid, skipping the census
Thursday hole closes (the venue never creates those markets), so the
composed reader's NEVER_CREATED accounting is genuinely exercised.

Instrumentation records every connector call with in/out timestamps
(from an injected clock) so rate and concurrency are MEASURED from
request intervals, never asserted.
"""
import hashlib
import json
import time
import urllib.parse
from datetime import datetime, timezone

from . import gh_common as G

# Census-exact Thursday hole (SHARED_MARKET_CALENDAR_SPEC): crypto
# series markets are never created with these UTC close times.
THURSDAY_HOLE_CLOSES = {(7, 15), (7, 30), (7, 45), (8, 0), (8, 15),
                        (8, 30), (8, 45), (9, 0)}
GRID_S = 900


def is_thursday_hole_close(epoch_ms):
    dt = datetime.fromtimestamp(epoch_ms / 1000, timezone.utc)
    return dt.weekday() == 3 and (dt.hour, dt.minute) in \
        THURSDAY_HOLE_CLOSES


def grid_markets(series_list, window_lo_ms, window_hi_ms, *,
                 strike_base=100.0):
    """Deterministic fixture universe: for each series, a market
    closing at every grid instant in (lo - 900s, hi + 900s] (so lead-in
    and tail markets exist), except Thursday-hole closes. Strike is a
    deterministic function of the ticker."""
    out = {}
    first = (window_lo_ms // (GRID_S * 1000)) * GRID_S * 1000
    t = first - GRID_S * 1000
    while t <= window_hi_ms + GRID_S * 1000:
        if not is_thursday_hole_close(t):
            for series in series_list:
                close_iso = G.iso_of_ms(t)
                open_iso = G.iso_of_ms(t - GRID_S * 1000)
                ticker = "%s-%s" % (series, close_iso.replace(
                    "-", "").replace(":", "").replace("T", "-"))
                digest = hashlib.sha256(ticker.encode()).digest()
                strike = strike_base + (digest[0] % 50) * 0.5
                out[ticker] = {
                    "ticker": ticker, "series_ticker": series,
                    "status": "active", "open_time": open_iso,
                    "close_time": close_iso, "floor_strike": strike,
                    "strike_type": "greater_or_equal",
                    "rules_primary": "Synthetic fixture market.",
                    "updated_time": open_iso}
        t += GRID_S * 1000
    return out


class StubVenue:
    """Connector callable: (method, host, path, headers, timeout) ->
    (status, headers, raw). Behaviour script:
      latency_s: float, or callable(call_no) -> float (seconds the
        call occupies; virtual clocks are advanced by the caller's
        harness, real clocks via time.sleep)
      failures: callable(call_no, kind, key) -> None | ("status", code)
        | "raise"   (kind: "listing" | "market")
    A `clock` (object with now_s()) may be attached; when latency is
    consumed in virtual mode the harness advances it externally — this
    connector only reports the latency it applied via last_latency_s.
    """

    def __init__(self, markets, now_fn, latency_s=0.0, failures=None,
                 sleep_fn=None, list_lookahead_s=0):
        self.markets = markets
        self._now = now_fn
        self.latency_s = latency_s
        self.failures = failures
        self._sleep = sleep_fn
        # Markets become listable this long before their open_time.
        # The frozen E2E fixture used 0; a real venue lists markets
        # when they are created, well before they open, and the
        # frozen discovery cadence (900 s) plus 180 s slot lateness
        # cannot otherwise ever capture an OPEN slot — the rehearsal
        # uses a 3600 s lookahead and says so in its freeze label.
        self.list_lookahead_s = list_lookahead_s
        self.calls = 0
        self.last_latency_s = 0.0

    def _latency(self):
        lat = self.latency_s(self.calls) if callable(self.latency_s) \
            else self.latency_s
        self.last_latency_s = lat
        if lat and self._sleep is not None:
            self._sleep(lat)
        return lat

    def __call__(self, method, host, path, headers, timeout):
        self.calls += 1
        q = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
        if "ticker" in q:
            kind, key = "market", q["ticker"][0]
        else:
            kind, key = "listing", q.get("series_ticker", ["?"])[0]
        self._latency()
        if self.failures is not None:
            verdict = self.failures(self.calls, kind, key)
            if verdict == "raise":
                raise ConnectionError("injected transport failure")
            if isinstance(verdict, tuple) and verdict[0] == "status":
                return verdict[1], {}, json.dumps(
                    {"error": "injected"}).encode()
        if kind == "market":
            m = self.markets[key]
            return 200, {}, json.dumps({"markets": [m]}).encode()
        horizon = G.iso_of_ms(int(self._now() * 1000)
                              + self.list_lookahead_s * 1000)
        listed = [m for m in self.markets.values()
                  if m["series_ticker"] == key
                  and m["open_time"] <= horizon]
        listed.sort(key=lambda m: m["close_time"])
        return 200, {}, json.dumps({"markets": listed,
                                    "cursor": None}).encode()


class InstrumentedConnector:
    """Wraps a connector; appends {seq, kind, key, t_in, t_out} to a
    sink list for every call (timestamps from the injected clock)."""

    def __init__(self, inner, now_fn, sink):
        self.inner = inner
        self._now = now_fn
        self.sink = sink

    def __call__(self, method, host, path, headers, timeout):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
        kind = "market" if "ticker" in q else "listing"
        key = q.get("ticker", q.get("series_ticker", ["?"]))[0]
        t_in = self._now()
        try:
            res = self.inner(method, host, path, headers, timeout)
            status = res[0]
        except Exception:
            self.sink.append({"kind": kind, "key": key, "t_in": t_in,
                              "t_out": self._now(),
                              "status": "EXC"})
            raise
        self.sink.append({"kind": kind, "key": key, "t_in": t_in,
                          "t_out": self._now(), "status": status})
        return res


def max_rolling_releases(log, window_s=60.0):
    """Worst number of request STARTS in any rolling window."""
    starts = sorted(e["t_in"] for e in log)
    worst, j = 0, 0
    for i, t in enumerate(starts):
        while starts[j] <= t - window_s:
            j += 1
        worst = max(worst, i - j + 1)
    return worst


def max_concurrency(log):
    """Worst number of simultaneously in-flight requests, from the
    recorded [t_in, t_out] intervals (an interval ending exactly when
    another starts does not overlap it)."""
    events = []
    for e in log:
        events.append((e["t_in"], 1))
        events.append((e["t_out"], -1))
    events.sort(key=lambda x: (x[0], x[1]))  # ends before starts at ties
    cur = worst = 0
    for _t, d in events:
        cur += d
        worst = max(worst, cur)
    return worst


class WallClock:
    def now_s(self):
        return time.time()

    def sleep_s(self, s):
        time.sleep(s)


class VirtualClock:
    """Deterministic accelerated clock for the lab chain driver."""

    def __init__(self, start_s):
        self.t = float(start_s)

    def now_s(self):
        return self.t

    def sleep_s(self, s):
        self.t += s

    def advance_to(self, t):
        if t > self.t:
            self.t = float(t)
