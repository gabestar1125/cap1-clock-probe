"""CAP-1 GitHub supervisor — PRODUCTION (live) wiring.

The activation-step counterpart of the rehearsal package: prod_main
wires the supervisor's SegmentJob/SealJob to the REAL venue connector
(the frozen cap1_transport's built-in HTTPS connector) behind the
activation freeze guard. Built 2026-10-28 under the activation
authorization (A = 2026-11-03T12:00:00Z). Nothing in this package is
imported by the rehearsal path; seg_main.py is untouched.
"""
