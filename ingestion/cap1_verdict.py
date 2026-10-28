"""Independent PT-1L verdict reader (G-COLL-FWD sec. 8).

Reads ONLY the sealed cohort evidence stores (never the capture writer's
state) and applies the Rev 3 thresholds exactly:
- >=98% of discovered cohort markets: VALID strike observation with
  receipt + uncertainty <= T-300 (conservative rule, frozen 250 ms bound).
- 100% of cohort markets: VALID evidence for BOTH T-300 and T-60.
- Zero provenance defects among relied-upon observations.
- Full accounting: observed / invalid / gap / never-created / quarantined.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion import cap1

PROVENANCE_DEFECTS = {"PAYLOAD_HASH_MISMATCH", "TIMESTAMP_ORDER",
                      "VENUE_TS_INCONSISTENT", "CLOCK_UNKNOWN",
                      "CLOCK_UNMEASURED", "CLOCK_EXCESSIVE"}
BOUND_MS = 250


def _ms(iso):
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp() * 1000)


def verdict(cohort_chain_path, cohort_lo_iso, cohort_hi_iso):
    chain = cap1.ChainStore(cohort_chain_path)
    lo, hi = _ms(cohort_lo_iso), _ms(cohort_hi_iso)
    markets = {}
    for rec in chain.records:
        if rec.get("kind") == "DISCOVERY":
            for m in rec.get("markets", []):
                markets[m["ticker"]] = m
    cohort = {t: m for t, m in markets.items()
              if lo <= _ms(m["close_time"]) < hi}
    rows, defects = {}, 0
    ok300 = okboth = 0
    for ticker, m in sorted(cohort.items()):
        close = _ms(m["close_time"])
        e300 = cap1.pit_eligible(chain, ticker, close - 300_000, BOUND_MS)
        e60 = cap1.pit_eligible(chain, ticker, close - 60_000, BOUND_MS)
        relied = [r for r in chain.records
                  if r.get("kind") == "OBSERVATION"
                  and r["ticker"] == ticker and r["validity"] == "INVALID"
                  and r.get("invalid_reason") in PROVENANCE_DEFECTS]
        defects += len(relied)
        rows[ticker] = {"t300": e300["eligible"], "t60": e60["eligible"],
                        "quarantined": cap1.is_quarantined(chain, ticker)}
        ok300 += e300["eligible"]
        okboth += (e300["eligible"] and e60["eligible"])
    n = len(cohort)
    gaps = sum(1 for r in chain.records if r.get("kind") == "GAP")
    invalid = sum(1 for r in chain.records
                  if r.get("kind") == "OBSERVATION"
                  and r["validity"] == "INVALID")
    quarantined = sum(1 for r in chain.records
                      if r.get("kind") == "QUARANTINE")
    pct300 = (ok300 / n) if n else 0.0
    passed = n > 0 and pct300 >= 0.98 and okboth == n and defects == 0
    return {"cohort_markets": n, "eligible_t300": ok300,
            "pct_t300": round(pct300, 4), "eligible_both": okboth,
            "provenance_defects_relied": defects, "gap_records": gaps,
            "invalid_observations": invalid, "quarantined": quarantined,
            "verdict": "PASS" if passed else "FAIL",
            "chain_tip": chain.tip}
