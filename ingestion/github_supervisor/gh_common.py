"""Shared primitives for the CAP-1 GitHub supervisor: canonical
serialization (identical rule to ingestion.cap1), time helpers, the
freeze-record schema (window bounds ALWAYS read from the freeze record,
never constants — amendment section 2.5), and segment-plan derivation.
"""
import hashlib
import json
import os
from datetime import datetime, timezone

FREEZE_SCHEMA = "cap1-freeze/1"
MANIFEST_SCHEMA = "cap1-handoff/1"
CHUNK_SCHEMA = "cap1-chunk/1"

# Frozen production values (design sections 3, 6, 7). These are the
# values a REAL freeze record must carry (DAT-3 verifies them); the
# supervisor itself reads every operative number from the freeze record
# so synthetic freezes can exercise the same code with small numbers.
PROD_SEGMENT_LENGTHS_S = [18000] * 9 + [10800]          # 9x5h + 3h = 48h
PROD_PHASE = {"dispatch_lead_s": 1800, "phase_b_s": 900, "tail_s": 300,
              "chunk_interval_s": 1800, "heartbeat_s": 300,
              "tick_interval_s": 120, "redispatch_offsets_s": [360, 720],
              "final_grace_s": 900, "claim_stale_s": 900}
PROD_THRESHOLDS = {"t300_min_pct": 0.98,
                   "activation_clock_bound_ms": 100,
                   "per_observation_max_ms": 250,
                   "consecutive_over_max_stop": 3}
PROD_BUDGETS = {"total_requests": 5000, "per_minute": 10,
                "concurrency": 1, "max_retries": 2,
                "storage_cap_bytes": 500 * 1024 * 1024,
                "max_recovery_per_day": 3}
DENOMINATOR_RULE = "EXCLUDE_BOUNDARY_INELIGIBLE"
DENOMINATOR_MAX_INELIGIBLE = 5


def canon(o) -> str:
    """Canonical JSON — the lab's chain rule (cap1._canon)."""
    return json.dumps(o, sort_keys=True, separators=(",", ":"))


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(path) -> str:
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def doc_hash(doc) -> str:
    """Hash of a JSON document over its canonical serialization."""
    return sha256_bytes(canon(doc).encode())


def parse_iso(s) -> int:
    """'YYYY-MM-DDTHH:MM:SSZ' -> epoch ms (the frozen parser's format)."""
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def iso_of_ms(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def iso_local_label(epoch_s) -> str:
    """Probe-style LOCAL timestamp label for supervisor records."""
    return datetime.fromtimestamp(epoch_s, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S") + (".%03dZ" % int((epoch_s % 1) * 1000))


class FreezeError(ValueError):
    pass


def load_freeze(path, ingestion_dir):
    """Load and validate a freeze record. Refuses (FreezeError) unless:
    schema matches; hard_end - A equals the segment-plan total; the
    code hashes match the frozen files on disk (the amendment's
    mismatched-constants prohibition); thresholds/budgets are present.
    Returns the freeze dict. Synthetic freezes are accepted and their
    `synthetic: true` marker is preserved — it is evidence, and the
    supervisor records it in every attestation it produces."""
    with open(path, "rb") as f:
        raw = f.read()
    fz = json.loads(raw)
    if fz.get("schema") != FREEZE_SCHEMA:
        raise FreezeError(f"schema {fz.get('schema')!r} != {FREEZE_SCHEMA}")
    a_ms = parse_iso(fz["activation_A"])
    end_ms = parse_iso(fz["hard_end"])
    lengths = fz["segment_plan"]["lengths_s"]
    if sum(lengths) * 1000 != end_ms - a_ms:
        raise FreezeError("segment plan does not tile [A, hard_end)")
    for key, fname in (("cap1_sha256", "cap1.py"),
                       ("transport_sha256", "cap1_transport.py"),
                       ("config_sha256", "cap1_config_v1.json")):
        actual = sha256_file(os.path.join(ingestion_dir, fname))
        if fz["code_hashes"].get(key) != actual:
            raise FreezeError(f"code hash mismatch for {fname}: freeze "
                              f"{fz['code_hashes'].get(key)} != disk "
                              f"{actual}")
    for section in ("thresholds", "budgets", "phase", "series"):
        if section not in fz:
            raise FreezeError(f"freeze missing section {section}")
    fz["_sha256"] = sha256_bytes(raw)
    fz["_a_ms"] = a_ms
    fz["_end_ms"] = end_ms
    return fz


def evidence_namespace(fz):
    """The freeze's OPTIONAL evidence namespace. Absent or empty
    (the production case, and every freeze written before this
    field existed) returns None, and every evidence branch name
    derived from it is byte-identical to the legacy names. A
    present namespace (e.g. "r2" for the clean rehearsal re-run)
    isolates that run's evidence branches and claim refs from any
    other run sharing the repository, without touching the other
    run's records."""
    ns = (fz or {}).get("evidence_namespace")
    if isinstance(ns, str) and ns.strip():
        return ns.strip()
    return None


def segment_bounds(fz):
    """[(B_k_ms, B_k+1_ms)] for k = 1..N from the freeze's plan."""
    out, t = [], fz["_a_ms"]
    for length in fz["segment_plan"]["lengths_s"]:
        out.append((t, t + length * 1000))
        t += length * 1000
    return out


def make_synthetic_freeze(*, activation_a_iso, lengths_s, phase,
                          ingestion_dir, label, series=("KXBTC15M",),
                          budgets=None, thresholds=None,
                          storage_cap_bytes=None):
    """Build a SYNTHETIC freeze record (rehearsal/tests only). Clearly
    labelled synthetic; never usable as an activation freeze — the
    activation path additionally requires synthetic == False plus the
    DAT flags, which only the principal's activation step can supply."""
    a_ms = parse_iso(activation_a_iso)
    end_iso = iso_of_ms(a_ms + sum(lengths_s) * 1000)
    budgets = dict(budgets or PROD_BUDGETS)
    if storage_cap_bytes is not None:
        budgets["storage_cap_bytes"] = storage_cap_bytes
    return {
        "schema": FREEZE_SCHEMA,
        "synthetic": True,
        "label": label,
        "activation_A": activation_a_iso,
        "hard_end": end_iso,
        "segment_plan": {"lengths_s": list(lengths_s)},
        "phase": dict(phase),
        "thresholds": dict(thresholds or PROD_THRESHOLDS),
        "budgets": budgets,
        "denominator_rule": DENOMINATOR_RULE,
        "runner_leg": "ubuntu-24.04-arm",
        "series": list(series),
        "code_hashes": {
            "cap1_sha256": sha256_file(
                os.path.join(ingestion_dir, "cap1.py")),
            "transport_sha256": sha256_file(
                os.path.join(ingestion_dir, "cap1_transport.py")),
            "config_sha256": sha256_file(
                os.path.join(ingestion_dir, "cap1_config_v1.json"))},
        "dat": {"tv_pass": True, "clock_pass": True,
                "note": "SYNTHETIC rehearsal freeze — DAT flags are "
                        "fixture values, not destination evidence"},
    }


def write_json(path, doc):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
