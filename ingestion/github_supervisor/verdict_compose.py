"""Composed PT-1L verdict over the segmented evidence shape
(design section 13.2) — an EXTENSION reader. The frozen
ingestion.cap1_verdict is not modified; this module applies the same
thresholds to the composition of per-segment stores linked by the
hash-chained handoff manifests, and adds the boundary accounting of
CAP1_BOUNDARY_ACCOUNTING_SPEC (approved denominator rule: discovered
cohort minus BOUNDARY_INELIGIBLE, count disclosed, >5 = accounting
FAIL) and the census grid accounting (Thursday-hole closes are
NEVER_CREATED, never GAPs).

Thresholds are read from the freeze record (which DAT-3 pins to the
frozen values for any real window).
"""
import os
from datetime import datetime, timezone

from ingestion import cap1

from . import gh_common as G
from .venue_stub import GRID_S, is_thursday_hole_close

PROVENANCE_DEFECTS = {"PAYLOAD_HASH_MISMATCH", "TIMESTAMP_ORDER",
                      "VENUE_TS_INCONSISTENT", "CLOCK_UNKNOWN",
                      "CLOCK_UNMEASURED", "CLOCK_EXCESSIVE"}
BOUND_MS = 250


def _verify_chain(path):
    """Constructing a ChainStore verifies it (frozen C8 semantics)."""
    try:
        return cap1.ChainStore(path), None
    except Exception as e:
        return None, str(e)


def compose(freeze, segments):
    """segments: [{index, cohort_chain_path, manifest, final}] in
    segment order. Returns the composed verdict dict."""
    a_ms, end_ms = freeze["_a_ms"], freeze["_end_ms"]
    bounds = G.segment_bounds(freeze)
    thr = freeze["thresholds"]
    fail = []

    # --- manifest chain + tiling -------------------------------------
    manifest_ok = True
    prev = freeze["_sha256"]
    for seg in segments:
        m = seg.get("manifest")
        if m is None:
            if seg["index"] < len(segments):
                manifest_ok = False
                fail.append(f"seg {seg['index']}: manifest missing")
            continue
        if m.get("predecessor_manifest_sha256") != prev:
            manifest_ok = False
            fail.append(f"seg {seg['index']}: manifest chain break")
        if m.get("freeze_sha256") != freeze["_sha256"]:
            manifest_ok = False
            fail.append(f"seg {seg['index']}: freeze hash mismatch")
        own = m.get("ownership", {})
        want = bounds[seg["index"] - 1]
        if (own.get("lo_ms"), own.get("hi_ms")) != want:
            manifest_ok = False
            fail.append(f"seg {seg['index']}: ownership tiling mismatch")
        prev = G.doc_hash(m)

    # --- per-segment chains + market composition ---------------------
    markets = {}          # ticker -> (owner_index, market dict)
    chains = {}
    boundary_excluded = {}
    boundary_ineligible = set()
    gap_records = 0
    invalid_obs = 0
    quarantined = 0
    raw_gaps = []         # (segment_index, record) for reclassification
    for seg in segments:
        chain, err = _verify_chain(seg["cohort_chain_path"])
        if chain is None:
            fail.append(f"seg {seg['index']}: chain unverifiable: {err}")
            chains[seg["index"]] = None
            continue
        chains[seg["index"]] = chain
        for rec in chain.records:
            kind = rec.get("kind")
            if kind == "DISCOVERY":
                for m in rec.get("markets", []):
                    if m["ticker"] in markets and \
                            markets[m["ticker"]][0] != seg["index"]:
                        fail.append(
                            f"{m['ticker']}: discovered by two owners")
                    markets.setdefault(m["ticker"],
                                       (seg["index"], m))
            elif kind == "BOUNDARY_EXCLUDED":
                boundary_excluded[rec["reason"]] = \
                    boundary_excluded.get(rec["reason"], 0) + 1
            elif kind == "BOUNDARY_INELIGIBLE":
                boundary_ineligible.add(rec["ticker"])
            elif kind == "GAP":
                gap_records += 1
                raw_gaps.append((seg["index"], rec))
            elif kind == "OBSERVATION" and rec["validity"] == "INVALID":
                invalid_obs += 1
            elif kind == "QUARANTINE":
                quarantined += 1

    # Boundary reclassification (spec section 4 assigns the categories
    # to the reader layer): a segment-1 GAP for a slot whose scheduled
    # due precedes A, on a market already open at A, is a
    # BOUNDARY_EXCLUDED / SLOT_BEFORE_ACTIVATION slot, never a GAP.
    for seg_idx, rec in raw_gaps:
        if seg_idx != 1:
            continue
        owner = markets.get(rec.get("ticker"))
        if owner is None:
            continue
        m = owner[1]
        if cap1.parse_iso_ms(m["open_time"]) >= a_ms:
            continue
        due = cap1.slot_times(m).get(rec.get("slot"))
        if due is not None and due < a_ms:
            gap_records -= 1
            boundary_excluded["SLOT_BEFORE_ACTIVATION"] = \
                boundary_excluded.get("SLOT_BEFORE_ACTIVATION", 0) + 1

    cohort = {t: v for t, v in markets.items()
              if a_ms <= cap1.parse_iso_ms(v[1]["close_time"]) < end_ms}

    # --- eligibility (frozen rule, per-owner chain) -------------------
    ok300 = okboth = defects = 0
    rows = {}
    for ticker, (owner_idx, m) in sorted(cohort.items()):
        chain = chains.get(owner_idx)
        close = cap1.parse_iso_ms(m["close_time"])
        if chain is None:
            e300 = e60 = {"eligible": False}
        else:
            e300 = cap1.pit_eligible(chain, ticker, close - 300_000,
                                     BOUND_MS)
            e60 = cap1.pit_eligible(chain, ticker, close - 60_000,
                                    BOUND_MS)
            defects += sum(1 for r in chain.records
                           if r.get("kind") == "OBSERVATION"
                           and r["ticker"] == ticker
                           and r["validity"] == "INVALID"
                           and r.get("invalid_reason")
                           in PROVENANCE_DEFECTS)
        ineligible = ticker in boundary_ineligible
        rows[ticker] = {"owner_segment": owner_idx,
                        "t300": e300["eligible"], "t60": e60["eligible"],
                        "boundary_ineligible": ineligible,
                        "quarantined": cap1.is_quarantined(chain, ticker)
                        if chain is not None else False}
        if not ineligible:
            ok300 += e300["eligible"]
            okboth += (e300["eligible"] and e60["eligible"])

    discovered = len(cohort)
    n_ineligible = sum(1 for t in cohort if t in boundary_ineligible)
    denominator = discovered - n_ineligible
    if n_ineligible > G.DENOMINATOR_MAX_INELIGIBLE:
        fail.append(f"BOUNDARY_INELIGIBLE {n_ineligible} > "
                    f"{G.DENOMINATOR_MAX_INELIGIBLE}: accounting FAIL")
    pct300 = (ok300 / denominator) if denominator else 0.0

    # --- D1: coverage terms itemized (the r1 wart) -------------------
    # Each coverage term that fails is named in fail_reasons with its
    # exact numbers, not left implicit in the boolean `passed`.
    if denominator > 0 and pct300 < thr["t300_min_pct"]:
        fail.append(f"T-300 coverage {ok300}/{denominator} = "
                    f"{pct300:.4f} below threshold "
                    f"{thr['t300_min_pct']}")
    if okboth != denominator:
        fail.append(f"eligible_both {okboth} != denominator "
                    f"{denominator}")
    if defects > 0:
        fail.append(f"provenance defects relied upon: {defects}")

    # --- D1: authoritative-final gating ------------------------------
    # Every segment must carry an AUTHORITATIVE final (segments[i]
    # ["final"], read by the seal from final.json — per-attempt
    # records under attempts/ are transparency only and never
    # substitute). The authoritative final must be CLEAN: HANDED_OFF
    # for segments 1..N-1, WINDOW_END for segment N. A segment whose
    # only terminal record is a non-clean attempt (the r3 seg-05
    # shape: final.json occupied by an aborted takeover) therefore
    # FAILS here, itemized. Legacy segment dicts without an
    # "attempts" key are read exactly as before, plus this check.
    for seg in segments:
        f = seg.get("final")
        if f is None:
            fail.append(f"seg {seg['index']}: authoritative final "
                        f"missing")
            continue
        want_clean = "WINDOW_END" if seg["index"] == len(segments) \
            else "HANDED_OFF"
        if f.get("exit_reason") != want_clean:
            fail.append(f"seg {seg['index']}: authoritative final "
                        f"not clean: {f.get('exit_reason')} "
                        f"(run {f.get('run_id')})")

    # --- end-boundary exclusions (computed, deterministic) ------------
    end_excluded = sum(
        1 for _t, (_o, m) in cohort.items()
        if cap1.parse_iso_ms(m["close_time"]) + 60_000 >= end_ms)

    # --- grid accounting (census) --------------------------------------
    grid_expected = grid_hole = grid_absent_other = 0
    discovered_closes = {(m["series_ticker"],
                          cap1.parse_iso_ms(m["close_time"]))
                         for _o, m in cohort.values()}
    t = (a_ms // (GRID_S * 1000)) * GRID_S * 1000
    if t < a_ms:
        t += GRID_S * 1000
    while t < end_ms:
        for series in freeze["series"]:
            grid_expected += 1
            if is_thursday_hole_close(t):
                grid_hole += 1
            elif (series, t) not in discovered_closes:
                grid_absent_other += 1
        t += GRID_S * 1000

    # --- budget reconciliation -----------------------------------------
    # Governor counters are seeded globally, so the chain telescopes:
    # each segment's spend is (its final global total) - (its effective
    # seed). Segments without a final contribute their worst-case
    # bound instead: spend attested at manifest time + tail reserve.
    actual_total = 0
    bound_total = 0
    finals_missing = []
    for seg in segments:
        f = seg.get("final")
        m = seg.get("manifest") or {}
        if f is None:
            finals_missing.append(seg["index"])
            req = m.get("requests", {})
            bound_total += (req.get("used_global_attested", 0)
                            - req.get("seed_global", 0)) \
                + req.get("tail_reserve", 0)
            continue
        actual_total += f["requests"]["segment_actual"]
        bound_total += f["requests"]["segment_actual"]
    cap_total = freeze["budgets"]["total_requests"]
    budget_ok = bound_total <= cap_total
    if not budget_ok:
        fail.append(f"request budget bound {bound_total} > {cap_total}")

    passed = (denominator > 0 and pct300 >= thr["t300_min_pct"]
              and okboth == denominator and defects == 0
              and manifest_ok and budget_ok and not fail)
    return {
        "verdict": "PASS" if passed else "FAIL",
        "fail_reasons": fail,
        "manifest_chain_ok": manifest_ok,
        "discovered_cohort": discovered,
        "boundary_ineligible": n_ineligible,
        "denominator": denominator,
        "eligible_t300": ok300,
        "pct_t300": round(pct300, 4),
        "eligible_both": okboth,
        "provenance_defects_relied": defects,
        "gap_records": gap_records,
        "invalid_observations": invalid_obs,
        "quarantined": quarantined,
        "boundary_excluded": boundary_excluded,
        "boundary_excluded_end_post_close": end_excluded,
        "grid": {"expected_closes": grid_expected,
                 "never_created_thursday_hole": grid_hole,
                 "undiscovered_grid_other": grid_absent_other},
        "budget": {"segment_actual_sum": actual_total,
                   "bound_sum": bound_total, "cap": cap_total,
                   "finals_missing": finals_missing},
        "rows": rows,
    }
