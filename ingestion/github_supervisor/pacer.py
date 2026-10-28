"""TransportPacer — the supervisor's rate/concurrency mechanism for
segment overlaps (principal refinement (ii); proof in
RATE_CONCURRENCY_PROOF.md).

The frozen Governor enforces 10 requests/minute and the frozen runner
is sequential — but only INSIDE one job. During an overlap two jobs
exist, each with its own governor, so without a cross-job mechanism the
pair could in principle sum past 10/min and reach concurrency 2. The
pacer is a transport wrapper (the frozen runner's own injection seam:
Tick(transport=...)) that restores the frozen bounds across the pair
during Phase B by CONSTRUCTION:

- Per-job Phase-B cap: the pacer admits at most `phase_b_cap` (5)
  venue-directed releases in any rolling 60 s window of ITS job. The
  partner job's pacer admits at most 5 in any rolling 60 s. Any 60 s
  window therefore sees at most 5 + 5 = 10 releases combined,
  regardless of alignment.
- Minute-parity release windows: the outgoing job may release only in
  even minutes (minute_index % 2 == 0), the incoming only in odd
  minutes (roles assigned by the segment run record). Releases are
  additionally guarded so a released request — bounded by the frozen
  transport's 10 s timeout plus a safety margin — completes inside its
  release minute. Venue requests from the two jobs therefore occupy
  disjoint minutes, and in-flight intervals (each < 60 s and contained
  in one minute) can never overlap: concurrency <= 1 at every instant.
- Outside Phase B (single active job) the pacer is a pass-through and
  the frozen governor's own limits govern.

The pacer counts RELEASES (calls into the wrapped transport). The
governor above it counts acquisitions, a superset in time (an
acquisition precedes its release), so governor accounting stays
conservative. One disclosed semantics note: because pacing happens
inside the transport stack, an observation's send_local_ms is stamped
by the engine just before the paced call — the eligibility-critical
receipt_local_ms is exact, and receipt >= send always (the engine's
TIMESTAMP_ORDER check is unaffected).
"""
import time

# Frozen transport timeout (cap1_transport.HttpTransport default).
TRANSPORT_TIMEOUT_S = 10.0
COMPLETION_GUARD_S = TRANSPORT_TIMEOUT_S + 5.0   # release deadline in
# the release minute: second_of_minute + guard <= 60 always holds.


class TransportPacer:
    """Wraps an object with .get(path, params) -> (status, raw)."""

    def __init__(self, inner, now_fn=time.time, sleep_fn=time.sleep,
                 phase_b_cap=5):
        self.inner = inner
        self._now, self._sleep = now_fn, sleep_fn
        self.phase_b_cap = phase_b_cap
        self.mode = "NORMAL"          # NORMAL | PHASE_B
        self.parity = None            # 0 = even minutes, 1 = odd
        self._releases = []           # release times (phase B only)
        self.total_releases = 0
        self.max_hold_s = 0.0

    def set_phase_b(self, parity):
        self.mode = "PHASE_B"
        self.parity = parity

    def set_normal(self):
        self.mode = "NORMAL"
        self.parity = None

    def _wait_for_window(self):
        """Sleep until this job may release a request under the
        Phase-B rules; returns the release instant."""
        while True:
            now = self._now()
            minute = int(now // 60)
            if minute % 2 != self.parity:
                self._sleep(60.0 - (now % 60.0) + 0.001)
                continue
            if now % 60.0 + COMPLETION_GUARD_S > 60.0:
                # Too late in this minute: a released request might not
                # complete inside it. Wait for the next own minute.
                self._sleep(120.0 - (now % 60.0) + 0.001)
                continue
            cutoff = now - 60.0
            self._releases = [t for t in self._releases if t > cutoff]
            if len(self._releases) >= self.phase_b_cap:
                wait = self._releases[0] + 60.0 - now
                self._sleep(max(wait, 0.001))
                continue
            return now

    def get(self, path, params=None):
        if self.mode == "PHASE_B":
            start = self._now()
            release = self._wait_for_window()
            self.max_hold_s = max(self.max_hold_s, release - start)
            self._releases.append(release)
        self.total_releases += 1
        return self.inner.get(path, params)
