"""Injected clock interface (Phase 2 hardening requirement 2).

Every layer-generated timestamp comes from a Clock handed to the component
at construction. SystemClock is the ONLY place in the layer that reads the
wall clock; ScriptedClock serves tests and raises on exhaustion rather
than ever falling back to wall-clock time. Source observation/publication
timestamps are data — they are parsed and validated here, never generated.
"""
from datetime import datetime, timezone


class ClockExhausted(Exception):
    pass


class ClockRegression(Exception):
    pass


def fmt_ts(dt) -> str:
    return dt.astimezone(timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(s):
    """Strict ISO-8601 UTC parse. Raises ValueError on anything else."""
    if not isinstance(s, str) or not s:
        raise ValueError("missing timestamp")
    txt = s.strip()
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    dt = datetime.fromisoformat(txt)
    if dt.tzinfo is None:
        raise ValueError("timestamp lacks timezone")
    return dt.astimezone(timezone.utc)


class Clock:
    def now_utc(self):
        raise NotImplementedError


class SystemClock(Clock):
    """Production-style clock. The layer's single wall-clock call site."""

    def now_utc(self):
        return datetime.now(timezone.utc)


class ScriptedClock(Clock):
    """Deterministic test clock: returns the scripted instants in order;
    exhaustion is a hard failure (never a wall-clock fallback)."""

    def __init__(self, instants):
        self._instants = list(instants)
        self.history = []

    def now_utc(self):
        if not self._instants:
            raise ClockExhausted("scripted clock exhausted")
        dt = self._instants.pop(0)
        self.history.append(dt)
        return dt
