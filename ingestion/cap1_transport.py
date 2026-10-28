"""CAP-1 live transport (G-COLL-FWD) — public-read-only, allowlisted.

GET-only by construction (no method parameter exists), fixed header set
(no auth material can be attached — there is no API for it), redirects
refused, hosts/paths allowlisted before any I/O. The HTTP connector is
injected: offline verification runs entirely on fixtures.
Governor: 10 requests/minute rolling, total-cap enforcement, bounded
retries, 429 stop tracking, deadline-aware deferral.
"""
import http.client
import json
import time
import urllib.parse

ALLOWED_HOSTS = {"api.elections.kalshi.com"}
ALLOWED_PATH_PREFIXES = ("/trade-api/v2/markets",)
BASE_HOST = "api.elections.kalshi.com"
BASE_PATH = "/trade-api/v2"
FIXED_HEADERS = {"Accept": "application/json",
                 "User-Agent": "cap1-observer/1.0"}


class TransportRefusal(Exception):
    def __init__(self, code, msg=""):
        super().__init__(f"{code}: {msg}")
        self.code = code


class TransportError(Exception):
    pass


class StopCondition(Exception):
    def __init__(self, code, msg=""):
        super().__init__(f"{code}: {msg}")
        self.code = code


def _default_connector(method, host, path_with_query, headers, timeout):
    conn = http.client.HTTPSConnection(host, timeout=timeout)
    try:
        conn.request(method, path_with_query, headers=headers)
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


class HttpTransport:
    def __init__(self, connector=None, timeout=10):
        self._connector = connector or _default_connector
        self._timeout = timeout

    def get(self, path, params=None):
        """The only operation. `path` is a full API path; allowlist is
        enforced before the connector is invoked."""
        if not any(path == p or path.startswith(p + "/") or
                   path.startswith(p + "?") for p in ALLOWED_PATH_PREFIXES):
            raise TransportRefusal("PATH_NOT_ALLOWLISTED", path)
        query = urllib.parse.urlencode(params or {})
        full = path + (("?" + query) if query else "")
        status, headers, raw = self._connector(
            "GET", BASE_HOST, full, dict(FIXED_HEADERS), self._timeout)
        if 300 <= status < 400:
            raise TransportRefusal("REDIRECT_REFUSED", f"{status} -> "
                                   f"{headers.get('Location', '?')}")
        return status, raw


class Governor:
    """Rolling-minute rate limit + total cap + retry/429 discipline.
    now_s/sleep_s are injected (live: time.time/time.sleep)."""

    def __init__(self, now_s, sleep_s, per_minute=10, total_cap=5000,
                 state=None):
        self._now, self._sleep = now_s, sleep_s
        self.per_minute, self.total_cap = per_minute, total_cap
        self.state = state if state is not None else {
            "total_requests": 0, "request_times": [], "rate_limit_events": []}

    def _prune(self):
        cutoff = self._now() - 60.0
        self.state["request_times"] = [
            t for t in self.state["request_times"] if t > cutoff]

    def acquire(self, deadline_s=None):
        """Wait (or refuse) until a request slot is available. Returns
        True when the caller may fire; raises StopCondition at the total
        cap; returns False when the deadline cannot be met (caller GAPs)."""
        if self.state["total_requests"] >= self.total_cap:
            raise StopCondition("REQUEST_CAP",
                                f"total {self.state['total_requests']}")
        self._prune()
        if len(self.state["request_times"]) >= self.per_minute:
            wait = self.state["request_times"][0] + 60.0 - self._now()
            if deadline_s is not None and self._now() + wait > deadline_s:
                return False
            self._sleep(max(wait, 0.0))
            self._prune()
        self.state["request_times"].append(self._now())
        self.state["total_requests"] += 1
        return True

    def note_response(self, status):
        if status == 429:
            now = self._now()
            recent = [t for t in self.state["rate_limit_events"]
                      if now - t <= 600.0]
            recent.append(now)
            self.state["rate_limit_events"] = recent
            if len(recent) >= 2:
                raise StopCondition("RATE_LIMITED",
                                    "second 429 within 10 minutes")

    def fetch(self, transport, path, params=None, deadline_s=None,
              backoff=(5.0, 15.0)):
        """One logical fetch: governed, ≤2 retries on transport errors /
        5xx only. 4xx returns as a response (preserved raw by caller).
        Returns (status, raw) or ('GAP', reason)."""
        attempts = 0
        while True:
            if not self.acquire(deadline_s=deadline_s):
                return ("GAP", "DEADLINE_UNMET")
            try:
                status, raw = transport.get(path, params)
            except TransportRefusal:
                raise
            except Exception as e:
                status, raw = None, str(e).encode()
                err = TransportError(str(e))
            if status is not None:
                self.note_response(status)
                if status < 500:
                    return (status, raw)
                err = TransportError(f"HTTP {status}")
            attempts += 1
            if attempts > len(backoff):
                return ("GAP", f"RETRIES_EXHAUSTED:{err}")
            if deadline_s is not None and \
                    self._now() + backoff[attempts - 1] > deadline_s:
                return ("GAP", "DEADLINE_UNMET")
            self._sleep(backoff[attempts - 1])

    def get_all_pages(self, transport, path, params=None, deadline_s=None):
        """Cursor pagination; every page is a governed, counted fetch."""
        pages, cursor = [], None
        while True:
            p = dict(params or {})
            if cursor:
                p["cursor"] = cursor
            res = self.fetch(transport, path, p, deadline_s=deadline_s)
            if res[0] == "GAP":
                return ("GAP", res[1])
            status, raw = res
            pages.append(raw)
            try:
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                return pages
            cursor = body.get("cursor")
            if not cursor:
                return pages
