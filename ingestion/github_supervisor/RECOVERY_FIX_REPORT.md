# CAP-1 Supervisor — Recovery/Takeover Wiring Fix Report (2026-10-28)

## The defect

`SegmentJob._step_claim` (supervisor.py) contains a CAS takeover
branch for a STALE claim, gated on `job.recovery`. The constructor
sets `recovery = False`, and **neither entry point ever set it** —
the only setter in the codebase was the lab `chain_driver.py`
(`recover()`). Consequence: any re-dispatch of an already-claimed
segment refused at check 4 ("atomic claim lost") forever; the
takeover path was dead code in both the rehearsal (`seg_main.py`)
and production (`production/prod_main.py`) entries.

Observed on GitHub, 2026-10-28: seg-03's first job
(gh-40790946031-s3) claimed, published started.json + one
heartbeat (virtual 2026-11-03T21:29:00Z, state LEADIN_A), then
died silently. Every subsequent dispatch refused — attempt B at
check 4 (claim held), the principal's recovery dispatches at
check 1 (hash transcription) and check 4.

**Negative control (this session):** the pre-fix `seg_main.py`
exactly as pushed (from the public clone, SHA-256 37e8940d…)
run against a copy of tonight's real seg-03 state with the
byte-perfect predecessor input → `REFUSED:4`, exit 3.

**Second defect (rehearsal only):** `_claim_heartbeat_age()`
measures `clock.now_s() − heartbeat.at_local`. Under the
rehearsal's virtual clock every instance of a segment starts at
the same formula-pinned virtual start (the workflow computes it
from the freeze), so a recovery instance would measure the dead
instance's heartbeat age as ≈0 s — forever below
`claim_stale_s` (900) — and takeover would refuse "not stale"
even with the flag wired. Production uses the wall clock and
never had this defect.

## The fix (wiring only — supervisor.py is byte-unchanged)

### 1. Recovery auto-detection, both entries

After constructing the `SegmentJob`, the entry sets
`job.recovery = True` **iff the claim store already holds a
claim for the segment** (`claim_store.read(k) is not None`),
evaluated before the job runs:

- `seg_main.py`: `detect_prior_claim()` runs before clock
  creation (its result also feeds the virtual-clock offset);
  the flag is applied to the segment job after construction.
  Seal jobs are excluded.
- `production/prod_main.py`: the same detection inside
  `build_segment_job()` (so the CLI path and every test seam
  inherit it), after construction, before return.

Invariants preserved: a first-ever dispatch (no prior claim)
keeps `recovery=False` and the takeover path unreachable;
`ClaimHeld` idempotence unchanged; check ordering unchanged; a
LIVE claim still refuses — the supervisor's takeover branch
itself arbitrates staleness, exactly as before. A claim-store
read failure is treated as "no claim" (the claim step remains
the arbiter), never as a recovery.

### 2. Rehearsal virtual-clock staleness offset (seg_main only)

When recovery is auto-detected **and** the clock is virtual,
the instance's virtual start becomes

```
max(scheduled_virtual_start,
    last_sign_of_life_epoch + claim_stale_s + 60)
```

where last-sign-of-life is the segment branch's `heartbeat.json`
`at_local`, falling back to the claim's `claimed_at_local` — the
supervisor's own fallback order in `_claim_heartbeat_age`
(helper `last_sign_of_life_s()`). `prod_main.py` receives **no**
timing change (wall clock only).

**Why this is faithful, not a fudge:** under the virtual clock,
"the claim is stale" can only mean a statement inside the
segment's simulated timeline. The dead instance's last heartbeat
is the last simulated instant it was alive; the frozen rule
permits takeover once the heartbeat is ≥ `claim_stale_s` old.
The offset starts the recovery instance at the *earliest*
simulated instant at which that is true (+60 s margin) — the
measured age at the claim step is exactly `claim_stale_s + 60`
(verified in T2: the takeover record carries
`heartbeat_age_s: 960.0`). Starting later would only inflate the
age; starting at the formula start would falsify it as ~0. The
supervisor's capture accounting keys off absolute slot times, so
the dead interval is accounted by the existing machinery, not
hidden by the shift. And liveness protection survives in the
only sense the virtual frame supports: a genuinely live owner
keeps republishing `heartbeat.json` with advancing `at_local`,
and `_claim_heartbeat_age` re-reads the CURRENT heartbeat at the
claim step — an owner that has advanced past the recovery
instance's start measures fresh and refuses the takeover (T4
exercises exactly this with a concurrent owner process).

**Late-start / GAP accounting engagement (verified in T2):**
tonight's kill happened in LEADIN_A *before* the segment window
opened (heartbeat governor total 0), and the offset resumes at
21:45 virtual — still before the 22:00 window open — so no
in-window slots existed to miss. The recovered segment's final
record is in family with the healthy seg-02: `crosscheck OK`,
DISCOVERY 20, OBSERVATION_VALID 300 (seg-02: 300), governor seed
carried (seed_global 702 → seed_at_start 750 with the disclosed
48-request killed-run credit → final_global 1059,
segment_actual 357). Nothing fabricated, nothing silently
dropped. Mid-window kills — where MISSED_SLOT/GAP accounting
bites — remain covered by FI-3/FI-6, still passing.

## Files changed (for a later approved push)

| file | SHA-256 before | SHA-256 after |
|---|---|---|
| ingestion/github_supervisor/seg_main.py | 37e8940d156a86a30ab72a1fad8dfded660a77e78afbd85f6b01f9bcc8b82b6d | 4788166d7a7633855a527da03b56e293cfaabaf7410a4b086c5ec36e90ee5aac |
| ingestion/github_supervisor/production/prod_main.py | dcc57338df7f9994c0940b9c0c9d2114b03e8e88b912a5b3dc2967d121c0847f | ad2abe326702f0d872db4054fdc4b64fcf69dda720d503bcfe2ef4a03340df2f |
| ingestion/github_supervisor/test_recovery_wiring.py | — (new) | c3b59b47f56e276b071f12b7000de5d0a905f566c7b249855c8cbb504b46931c |
| ingestion/github_supervisor/test_results/recovery_run1.json | — (new) | (suite output, byte-identical to run2) |
| ingestion/github_supervisor/test_results/recovery_run2.json | — (new) | (suite output, byte-identical to run1) |
| ingestion/github_supervisor/RECOVERY_FIX_REPORT.md | — (new) | this file |

**Not changed:** supervisor.py
(f0ece8d6b36a92d02e9caedf6ea6d21f1240d648c91eae9055bfc730b078aede,
byte-identical before/after — the takeover logic itself needed
no modification), chain_driver.py, claims.py, all other files.

**Frozen five, verified byte-identical before AND after:**
cap1.py 5e854872116fbec8c02ad63a0216f29d5ff06d6a9bad69372cbf2a987ea8ba50 ·
cap1_transport.py 00539951430f5bff496edebf02b59357f89dcdfe09f6f27301d5842eeb54d0ec ·
cap1_config_v1.json 77abaee418df93e9a27834d13fb40b43b04e96f9b6aeeb7ca0fc5786f1427771 ·
cap1_run.py 06abf24cab40f74ce5b3923760380202c186bb4c37d669e9ce4448edf567d53c ·
cap1_verdict.py 1064345d8acf4951be551028477eb72cb43d2f2c03a636f35597cb60fe730ebb

## Test results

New suite `test_recovery_wiring.py` — **22/22 PASS, twice
byte-identical** (test_results/recovery_run1.json ==
recovery_run2.json):

- **T1** seg_main + git, fresh segment 1: HANDED_OFF, claim
  ours with no takeover_of, no recovery_started.json. (5 checks)
- **T2** seg_main + git against tonight's real seg-03 state
  (fixture = copy of the live-repo mirror; seg-02 manifest
  doc_hash anchors the fixture to the recorded input
  7a2d7588…): recovery dispatch TAKES OVER
  (recovery_started.json published; claim records
  takeover_of run gh-40790946031-s3 with heartbeat_age_s 960.0),
  completes, HANDED_OFF, crosscheck OK. (7 checks)
- **T3** production builder, dir stores: job A recovery=False
  at construction (no claim), claims and heartbeats; job B
  auto-detected recovery=True; takeover of the FRESH claim
  refused at check 4 ("takeover refused"); claim still owned
  by A. (6 checks)
- **T4** seg_main + git, LIVE concurrent owner republishing
  heartbeat.json with advancing at_local: recovery dispatch
  REFUSED:4; claim untouched; no recovery_started.json.
  (4 checks)

Pre-existing suites, re-run after the change (baselines in
parentheses, identical): supervisor **38/38** (38/38),
failure-injection **30/30** (30/30), production **23/23**
(23/23), CAP-1 **9/9** (9/9), transport **11/11** (11/11),
E2E **10/10** (10/10).

## Exact diffs

### ingestion/github_supervisor/seg_main.py

```diff
--- /tmp/cap1-clone/ingestion/github_supervisor/seg_main.py	2026-10-29 01:17:51.292688480 +0000
+++ seg_main.py	2026-10-29 01:22:48.864655341 +0000
@@ -43,6 +43,51 @@
         return super().dispatch(**kw)
 
 
+def detect_prior_claim(claim_store, segment_index):
+    """Recovery auto-detection (fix for the unreachable-takeover
+    defect): a segment is a RECOVERY dispatch iff the claim store
+    already holds a claim for it. Read-only arbitration only — the
+    supervisor's claim step still decides (a live claim's takeover
+    is refused there; a first-ever dispatch, with no prior claim,
+    keeps recovery=False and the takeover path unreachable).
+    Returns the claim doc, or None. Any read failure is treated as
+    no claim: the claim step itself remains the arbiter."""
+    read = getattr(claim_store, "read", None)
+    if read is None:
+        return None
+    try:
+        return read(segment_index)
+    except Exception:
+        return None
+
+
+def last_sign_of_life_s(publisher, segment_index, claim_doc):
+    """Epoch seconds of the previous instance's last sign of life:
+    the segment branch's heartbeat.json at_local, falling back to
+    the claim's claimed_at_local — the supervisor's own fallback
+    order in SegmentJob._claim_heartbeat_age. None if neither
+    parses."""
+    at_local = None
+    try:
+        raw = publisher.read(sup.seg_branch(segment_index),
+                             "heartbeat.json")
+    except Exception:
+        raw = None
+    if raw:
+        try:
+            at_local = json.loads(raw).get("at_local")
+        except Exception:
+            at_local = None
+    if not at_local and claim_doc:
+        at_local = claim_doc.get("claimed_at_local")
+    if not at_local:
+        return None
+    try:
+        return G.parse_iso(at_local[:19] + "Z") / 1000.0
+    except Exception:
+        return None
+
+
 def main(argv=None):
     ap = argparse.ArgumentParser()
     ap.add_argument("--freeze", required=True)
@@ -84,7 +129,36 @@
     else:
         dispatcher = PrintDispatch()
 
-    clock = VirtualClock(args.virtual_start) \
+    # Recovery auto-detection, evaluated BEFORE the job is built:
+    # a prior claim in the store makes this dispatch a recovery.
+    prior_claim = None
+    if args.role != "seal":
+        prior_claim = detect_prior_claim(claim_store, args.segment)
+
+    clock_start = args.virtual_start
+    if args.virtual_start is not None and prior_claim is not None:
+        # Rehearsal virtual-clock staleness (seg_main only; the
+        # production entry is wall-clock and takes no offset): the
+        # workflow formula pins EVERY instance of a segment to the
+        # same virtual start, so a recovery instance would measure
+        # the dead instance's heartbeat age as ~0 and the takeover
+        # would refuse "not stale" forever. Resume the timeline
+        # just past the staleness gap instead: the recovery
+        # instance's clock starts at the dead instance's last sign
+        # of life + claim_stale_s + 60 — the earliest virtual
+        # instant at which the claim is genuinely stale, so the
+        # measured age is the true gap the frozen rule requires,
+        # neither fabricated larger nor smaller in effect (any
+        # later start measures an even older heartbeat; the
+        # supervisor's slot accounting keys off absolute slot
+        # times, so the missed interval is accounted, not hidden).
+        last = last_sign_of_life_s(publisher, args.segment,
+                                   prior_claim)
+        if last is not None:
+            stale_s = freeze["phase"].get("claim_stale_s", 900)
+            clock_start = max(clock_start, last + stale_s + 60)
+
+    clock = VirtualClock(clock_start) \
         if args.virtual_start is not None else WallClock()
 
     if args.section_c.startswith("synthetic:"):
@@ -122,6 +196,11 @@
         probe_provider=probe_provider,
         dispatch_input=args.dispatch_input,
         image_version=args.image_version, events=[])
+    if prior_claim is not None:
+        # Wire the takeover path (SegmentJob._step_claim): with a
+        # prior claim present this dispatch may take over a STALE
+        # claim; a live claim still refuses there.
+        job.recovery = True
     while not job.finished:
         wake = job.step()
         if wake is None:
```

### ingestion/github_supervisor/production/prod_main.py

```diff
--- /tmp/cap1-clone/ingestion/github_supervisor/production/prod_main.py	2026-10-29 01:17:51.292688480 +0000
+++ production/prod_main.py	2026-10-29 01:22:53.474655306 +0000
@@ -151,7 +151,7 @@
     clock = clock if clock is not None else WallClock()
     if connector is None:
         connector = build_live_connector(clock)
-    return sup.SegmentJob(
+    job = sup.SegmentJob(
         freeze=freeze, segment_index=segment_index, run_id=run_id,
         publisher=publisher, claim_store=claim_store,
         dispatcher=dispatcher, connector=connector, clock=clock,
@@ -163,6 +163,22 @@
         dispatch_input=dispatch_input,
         image_version=image_version,
         events=events if events is not None else [])
+    # Recovery auto-detection (fix for the unreachable-takeover
+    # defect): a prior claim in the store makes this dispatch a
+    # recovery, wiring SegmentJob._step_claim's takeover branch.
+    # A first-ever dispatch (no prior claim) keeps recovery=False
+    # and the takeover path unreachable. Read-only arbitration
+    # only — the claim step still refuses takeover of a LIVE claim.
+    # The production clock is the wall clock: NO timing offset is
+    # applied here (the virtual-clock staleness offset exists only
+    # in the rehearsal entry, seg_main).
+    try:
+        prior_claim = claim_store.read(segment_index)
+    except Exception:
+        prior_claim = None
+    if prior_claim is not None:
+        job.recovery = True
+    return job
 
 
 def build_seal_job(*, freeze, run_id, publisher, claim_store,
```
