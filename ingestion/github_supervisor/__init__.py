"""CAP-1 GitHub Actions supervisor (G-CAP1-GHA-BUILD, 2026-10-28).

Adaptation layer that runs the FROZEN CAP-1 collector (ingestion.cap1,
ingestion.cap1_transport, ingestion.cap1_run, ingestion.cap1_verdict —
hash-pinned, never modified by this package) across chained GitHub
Actions jobs: 10 ownership segments, hash-chained handoff manifests,
atomic segment claims, phased overlaps with a provable rate/concurrency
bound, and a composed independent verdict.

This package performs NO venue I/O of its own: every venue request is
made by the frozen collector through an injected transport. All lab and
rehearsal traffic is synthetic (stub venue). No live collection is
authorized by this code's existence.
"""
