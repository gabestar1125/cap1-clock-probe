# cap1-clock-probe

Empirical probe for the CAP-1 research programme: can a GitHub-hosted runner
produce a section-C-shaped clock-readiness evidence record — a synchronised
clock daemon's own offset estimate and root-dispersion figure, from which a
bound of <= 100 ms can be derived over recorded numbers?

- `probe_clock.py` — pure-stdlib probe; captures daemon state verbatim,
  tests outbound UDP/123 (diagnostic only), measures drift and granularity,
  and attempts the derivation. It fails honestly: absent or unsynchronised
  daemon evidence is recorded as IMPOSSIBLE, never approximated.
- `.github/workflows/cap1-clock-probe.yml` — runs the probe on
  ubuntu-24.04 and ubuntu-24.04-arm; each leg commits its evidence JSON to
  `results/<runner>.json` on branch `probe-results/<runner>`.
- No credentials, no API keys, and no third-party service calls are involved.
