"""Synthetic connector factory for seg_main (rehearsal/empirical).

Builds a StubVenue over the freeze window's grid universe and wraps
it in an InstrumentedConnector that appends every call (in/out
timestamps from the run's clock) to a JSONL request log — the file
the rate/concurrency analysis measures. Path from env
CAP1_REQUEST_LOG (default: ./cap1_request_log.jsonl). Latency and
failure injection from env, for rehearsal scenarios:
  CAP1_STUB_LATENCY_S   float seconds each call occupies
  CAP1_STUB_FAIL_MODE   "" | "storm" (first two attempts of every
                        market fetch return HTTP 500)
This module is the ONLY connector seg_main wires in this build: the
synthetic stub. Zero venue requests are possible through it.
"""
import json
import os

from .venue_stub import StubVenue, InstrumentedConnector, grid_markets


def synthetic_connector(freeze, clock, log_path=None):
    fixture_path = os.environ.get("CAP1_FIXTURE_JSON")
    if fixture_path:
        with open(fixture_path) as f:
            markets = json.load(f)
    else:
        markets = grid_markets(freeze["series"], freeze["_a_ms"],
                               freeze["_end_ms"])
    latency = float(os.environ.get("CAP1_STUB_LATENCY_S", "0"))
    lookahead = int(os.environ.get("CAP1_STUB_LOOKAHEAD_S", "3600"))
    fail_mode = os.environ.get("CAP1_STUB_FAIL_MODE", "")
    attempts = {}

    def failures(call_no, kind, key):
        if fail_mode == "storm" and kind == "market":
            n = attempts.get(key, 0) + 1
            attempts[key] = n
            if n <= 2:
                return ("status", 500)
        return None

    venue = StubVenue(markets, clock.now_s, latency_s=latency,
                      failures=failures if fail_mode else None,
                      sleep_fn=clock.sleep_s,
                      list_lookahead_s=lookahead)
    log_path = log_path or os.environ.get(
        "CAP1_REQUEST_LOG", "cap1_request_log.jsonl")
    sink = _FileSink(log_path, clock)
    return InstrumentedConnector(venue, clock.now_s, sink)


class _FileSink:
    def __init__(self, path, clock):
        self.path = path
        self.clock = clock

    def append(self, entry):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, sort_keys=True) + "\n")
