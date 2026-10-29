"""CAP-1 GitHub supervisor — segment lifecycle around the FROZEN
collector (design sections 3-8). The supervisor:

- reads all window/segment parameters from the freeze record,
- verifies the handoff chain fail-closed before capturing (sec. 5.3),
- takes the segment's ATOMIC claim (claims.py; design sec. 4.3 check 4
  as strengthened by principal refinement (i)),
- gates on section-C clock evidence (sec. 6),
- drives the frozen cap1_run.Tick — full ticks normally, capture-only
  ticks in yield/lead-in phases — through the runner's own injection
  seams (transport, probe_fn, now_fn, sleep_fn) and module layout,
  exactly as the frozen E2E suite does. The frozen files are never
  modified; the runner's module constants are pointed at the segment's
  work directory and ownership interval per segment (sec. 3.4),
- publishes evidence chunks / heartbeat / manifest / final to the
  segment branch (publisher.py),
- dispatches the successor (dispatcher.py) with re-dispatch,
- carries the budget ledger and the section-F consecutive counter
  across the handoff (sec. 6, 7.1),
- composes the seal (verdict_compose.py) in the seal role.

Steppable design: step() performs everything due at the injected
clock's current time and returns the next wake time, so the lab chain
driver can interleave two segments deterministically on one virtual
clock (true overlap semantics) while a production job simply loops
step()+sleep on the wall clock.
"""
import json
import os
import re
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_LAB = os.path.dirname(os.path.dirname(_HERE))
if _LAB not in sys.path:
    sys.path.insert(0, _LAB)

from ingestion import cap1, cap1_run as R, cap1_transport as ct  # noqa: E402
from . import gh_common as G  # noqa: E402
from . import claims as claims_mod  # noqa: E402
from . import verdict_compose  # noqa: E402
from .pacer import TransportPacer  # noqa: E402
from .publisher import IntegrityError  # noqa: E402

ING_DIR = os.path.join(_LAB, "ingestion")
COHORT_SLOTS = R.COHORT_SLOTS
LATENESS_MS = R.LATENESS_MS


def seg_branch(k, namespace=None):
    """Evidence branch for segment k. With no namespace (absent or
    empty freeze field — production and all pre-existing freezes)
    the name is byte-identical to the legacy form; with a namespace
    ("r2") the run's branches are isolated from every other run
    sharing the repository."""
    if namespace:
        return "cap1-%s-segments/seg-%02d" % (namespace, k)
    return "cap1-segments/seg-%02d" % k


def seal_branch(namespace=None):
    if namespace:
        return "cap1-%s-seal" % namespace
    return "cap1-seal"


_CHUNK_RE = re.compile(r"^chunks/chunk-(\d+)\.json$")


def _new_pub_error_state():
    return {"counter": 0, "records": 0, "record_failures": 0,
            "total": 0, "categories": {}, "in_record": False}


def _attempt_pub_error_record(publisher, branch, *, segment_index,
                              run_id, relpath, exc, at_local, state):
    """Fix B: publish ONE write-once sanitized error record for a
    failed publish, via a DIRECT publisher call. Never raises and
    never records recursively (guarded by state["in_record"] and by
    bypassing the caller's _publish funnel). Mutates `state`:
    total/categories are updated for every failure; records /
    record_failures count the record publish outcome."""
    category = G.classify_publication_error(exc)
    state["total"] = state.get("total", 0) + 1
    cats = state.setdefault("categories", {})
    cats[category] = cats.get(category, 0) + 1
    if state.get("in_record"):
        return category
    state["in_record"] = True
    try:
        seq = state.get("counter", 0)
        state["counter"] = seq + 1
        excerpt_src = getattr(exc, "last_error", None) or str(exc)
        doc = {
            "schema": "cap1-pubfail/1",
            "segment_index": segment_index,
            "run_id": run_id,
            "branch": branch,
            "relpath": relpath,
            "attempts": int(getattr(exc, "attempts", 0) or 0),
            "exhausted": bool(getattr(exc, "exhausted", False)),
            "category": category,
            "error_excerpt": G.sanitize_error_text(excerpt_src),
            "at_local": at_local}
        data = (G.canon(doc) + "\n").encode()
        publisher.publish(branch, "errors/pubfail-%04d.json" % seq,
                          data)
        state["records"] = state.get("records", 0) + 1
    except Exception:
        state["record_failures"] = \
            state.get("record_failures", 0) + 1
    finally:
        state["in_record"] = False
    return category


def _chunk_inventory(publisher, branch):
    """Authoritative chunk inventory: [(index, relpath)] sorted by
    index, established via publisher.list() where available.
    Raises on a list() error or an unparseable entry under chunks/;
    falls back to sequential reads only for legacy wrappers that
    provide no list() at all."""
    list_fn = getattr(publisher, "list", None)
    if list_fn is None:
        out = []
        idx = 0
        while publisher.read(branch,
                             "chunks/chunk-%04d.json" % idx) \
                is not None:
            out.append((idx, "chunks/chunk-%04d.json" % idx))
            idx += 1
        return out
    entries = list_fn(branch, "chunks/")
    out = []
    for entry in entries or []:
        m = _CHUNK_RE.match(entry)
        if not m:
            raise ValueError("unparseable chunk entry: %r" % entry)
        out.append((int(m.group(1)), entry))
    out.sort()
    return out


def _is_contiguous(indices):
    return list(indices) == list(range(len(indices)))


class OwnershipFilter:
    """Transport wrapper: listing responses are restricted to markets
    this segment OWNS (close_time in [lo, hi)); markets owned by the
    successor are harvested (full objects) for the handoff's early-
    market list. Per-ticker requests pass through (the engine only
    asks about markets it knows, which post-filtering are owned)."""

    def __init__(self, inner, lo_ms, hi_ms, next_hi_ms):
        self.inner = inner
        self.lo_ms, self.hi_ms, self.next_hi_ms = lo_ms, hi_ms, \
            next_hi_ms
        self.harvested = {}

    def get(self, path, params=None):
        status, raw = self.inner.get(path, params)
        if not params or "series_ticker" not in params:
            return status, raw
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            return status, raw
        kept = []
        for m in body.get("markets", []):
            close = cap1.parse_iso_ms(m["close_time"])
            if self.lo_ms <= close < self.hi_ms:
                kept.append(m)
            elif self.hi_ms <= close < self.next_hi_ms:
                self.harvested[m["ticker"]] = m
        body["markets"] = kept
        return status, json.dumps(body).encode()


class SegmentJob:
    def __init__(self, *, freeze, segment_index, run_id, publisher,
                 claim_store, dispatcher, connector, clock,
                 work_root, section_c_provider, probe_provider,
                 dispatch_input, image_version="unknown", events=None):
        self.fz = freeze
        self.ns = G.evidence_namespace(freeze)
        self._dispatch_input = dispatch_input
        self.k = segment_index
        self.n = len(freeze["segment_plan"]["lengths_s"])
        self.run_id = run_id
        self.publisher = publisher
        self.claims = claim_store
        self.dispatcher = dispatcher
        self.clock = clock                      # now_s()/sleep_s()
        self.recovery = False
        self.events = events if events is not None else []
        self.image_version = image_version
        bounds = G.segment_bounds(freeze)
        self.b_lo, self.b_hi = bounds[segment_index - 1]
        self.next_hi = bounds[segment_index][1] \
            if segment_index < self.n else freeze["_end_ms"]
        phase = freeze["phase"]
        self.t_dispatch = self.b_hi - phase["dispatch_lead_s"] * 1000
        self.t_phase_b = self.b_hi - phase["phase_b_s"] * 1000
        self.t_tail_end = self.b_hi + phase["tail_s"] * 1000
        self.t_lead_b = self.b_lo - phase["phase_b_s"] * 1000
        self.branch = seg_branch(segment_index, self.ns)
        self.workdir = os.path.join(work_root, "seg-%02d"
                                    % segment_index)
        os.makedirs(self.workdir, exist_ok=True)
        self.ownership_filter = OwnershipFilter(
            ct.HttpTransport(connector=connector),
            self.b_lo, self.b_hi, self.next_hi)
        self.pacer = TransportPacer(self.ownership_filter,
                                    clock.now_s, clock.sleep_s,
                                    phase_b_cap=5)
        self.pacer.mode = "NORMAL"
        self.section_c_provider = section_c_provider
        self.probe_provider = probe_provider
        # lifecycle state
        self.state = "INIT"
        self.finished = False
        self.exit_reason = None
        self.exit_code = 0
        self.manifest = None
        self.manifest_sha = None
        self.pred_manifest = None
        self.seed_initial = 0
        self.seed_credit = 0
        self.consec_over = 0
        self.clock_rec = None
        self.clock_degraded = False
        self.crosscheck = "PENDING" if segment_index > 1 else "NA"
        self._scan = {"cohort": 0, "ops": 0}
        self._gov_total = 0
        self._chunk_idx = 0
        self._published_lines = {"cohort": 0, "ops": 0,
                                 "raw_cohort": 0, "raw_ops": 0}
        self._next_tick = None
        self._next_chunk = None
        self._next_hb = None
        self._dispatch_attempts = 0
        self._attestation_confirmed = False
        self._lead_discovery_done = False
        self._boundary_done = False
        self._final_discovery_done = False
        self._sealed_tick_done = False
        self._pub_failures = 0
        self.bytes_published = 0
        # Fix A: sha256 of the exact published bytes per chunk index.
        self._chunk_hashes = {}
        self._last_gate_verified_chunks = 0
        self._publication_gate_info = None
        # Fix B: durable publication-error record state.
        self._pub_err_state = _new_pub_error_state()

    # ------------------------------------------------- Fix B helpers
    @property
    def _pub_error_record_failures(self):
        return self._pub_err_state["record_failures"]

    @_pub_error_record_failures.setter
    def _pub_error_record_failures(self, value):
        self._pub_err_state["record_failures"] = value

    def publication_error_summary(self):
        st = self._pub_err_state
        return {"total": st["total"],
                "categories": dict(st["categories"]),
                "records": st["records"],
                "record_failures": st["record_failures"]}

    def publication_error_summary_line(self):
        s = self.publication_error_summary()
        return ("PUBLICATION_ERRORS total=%d categories=%s "
                "records=%d record_failures=%d" % (
                    s["total"],
                    json.dumps(s["categories"], sort_keys=True),
                    s["records"], s["record_failures"]))

    # ------------------------------------------------------------- util
    def _now_ms(self):
        return int(self.clock.now_s() * 1000)

    def _event(self, kind, **kw):
        ev = {"event": kind, "segment": self.k, "run_id": self.run_id,
              "at_local": G.iso_local_label(self.clock.now_s())}
        ev.update(kw)
        self.events.append(ev)

    def _publish(self, relpath, doc):
        data = (G.canon(doc) + "\n").encode() \
            if not isinstance(doc, bytes) else doc
        before = self.publisher.bytes_published
        try:
            res = self.publisher.publish(self.branch, relpath, data)
        except Exception as e:
            # Fix B: FIRST classify+sanitize and attempt the durable
            # error record, THEN re-raise so every existing caller's
            # control flow is unchanged. The record attempt itself
            # never raises (see _attempt_pub_error_record).
            _attempt_pub_error_record(
                self.publisher, self.branch,
                segment_index=self.k, run_id=self.run_id,
                relpath=relpath, exc=e,
                at_local=G.iso_local_label(self.clock.now_s()),
                state=self._pub_err_state)
            raise
        self.bytes_published += self.publisher.bytes_published - before
        return res

    def _read_remote(self, branch, relpath):
        raw = self.publisher.read(branch, relpath)
        return json.loads(raw) if raw is not None else None

    # -------------------------------------------------- runner plumbing
    def _point_runner(self):
        R.WINDOW_DIR = self.workdir
        R.FREEZE_PATH = os.path.join(self.workdir, "FREEZE.json")
        R.STATE_PATH = os.path.join(self.workdir, "state.json")
        R.STORES = {"cohort": os.path.join(self.workdir, "cohort.json"),
                    "ops": os.path.join(self.workdir, "ops.json")}
        R.RAW_DIRS = {"cohort": os.path.join(self.workdir, "raw_cohort"),
                      "ops": os.path.join(self.workdir, "raw_ops")}
        R.COHORT_LO = G.iso_of_ms(self.b_lo)
        R.COHORT_HI = G.iso_of_ms(self.b_hi)
        R.HARD_END = self.fz["hard_end"]
        R.SERIES = tuple(self.fz["series"])

    def _write_runner_freeze(self):
        doc = {"tv_pass": bool(self.fz["dat"]["tv_pass"]),
               "clock_pass": bool(self.fz["dat"]["clock_pass"]),
               "transport_sha256": self.fz["code_hashes"]
               ["transport_sha256"],
               "cap1_sha256": self.fz["code_hashes"]["cap1_sha256"],
               "config_sha256": self.fz["code_hashes"]["config_sha256"]}
        G.write_json(os.path.join(self.workdir, "FREEZE.json"), doc)

    def _seed_state(self):
        # Successor discovery seed: with the frozen 900 s cadence,
        # last = B_k - 1800 s makes the successor's polls fire at
        # B_k - 900 s (its lead-in discovery, deviation D-3) and at
        # B_k (interval open) — and at no other lead-in instant.
        seed_disc = (self.b_lo - 1_800_000) if self.k > 1 else 0
        seed_total = self.seed_initial + getattr(
            self, "_recovered_usage_estimate", 0)
        state = {"gov": {"total_requests": seed_total,
                         "request_times": [], "rate_limit_events": []},
                 "last_discovery_ms": seed_disc, "restarts": {},
                 "stopped": None, "sealed": False,
                 "last_tick_status": "ok"}
        G.write_json(os.path.join(self.workdir, "state.json"), state)

    def _make_tick(self):
        self._point_runner()
        fz, err = R.load_freeze()
        if fz is None:
            raise RuntimeError("runner freeze rejected: %s" % err)
        return R.Tick(fz, transport=self.pacer,
                      probe_fn=self.probe_provider,
                      now_fn=self.clock.now_s,
                      sleep_fn=self.clock.sleep_s)

    def _chain_path(self, name):
        return os.path.join(self.workdir, name + ".json") \
            if name in ("cohort", "ops") else os.path.join(
                self.workdir, name)

    def _after_tick(self, tick):
        """Section-F scan + stopped detection over both chains."""
        thr = self.fz["thresholds"]
        stop_fired = False
        for name in ("cohort", "ops"):
            path = self._chain_path(name)
            if not os.path.exists(path):
                continue
            chain = cap1.ChainStore(path)
            recs = chain.records
            for rec in recs[self._scan[name]:]:
                if rec.get("kind") == "OBSERVATION":
                    if rec.get("invalid_reason") == "CLOCK_EXCESSIVE":
                        self.consec_over += 1
                    elif rec.get("validity") == "VALID":
                        self.consec_over = 0
            self._scan[name] = len(recs)
        self._gov_total = tick.state["gov"]["total_requests"]
        if self.consec_over >= thr["consecutive_over_max_stop"] \
                and not tick.state.get("stopped"):
            tick.stop("CLOCK_F",
                      "section F: %d consecutive observations over "
                      "%d ms" % (self.consec_over,
                                 thr["per_observation_max_ms"]))
            stop_fired = True
        if tick.state.get("stopped"):
            code = tick.state["stopped"]["code"]
            self._terminate("STOP:%s" % code, 5)
            self._event("STOPPED", code=code)
        return stop_fired

    def _run_tick(self, mode):
        tick = self._make_tick()
        now = self._now_ms()
        if tick.chain_errors:
            rc = tick.run()          # records terminal STOP itself
        elif mode == "FULL":
            rc = tick.run()
        else:                        # capture-only (yield / lead-in B)
            if tick.state.get("stopped"):
                rc = 5
            elif tick.state.get("sealed"):
                rc = 0
            elif now >= self.fz["_end_ms"]:
                rc = 0               # hard end: no venue request at or
                # after A+48h (design sec. 7.6); the frozen runner's own
                # seal path is bypassed in capture-only mode, so the
                # supervisor enforces the boundary itself.
            else:
                try:
                    tick.capture_due(now)
                    tick.save()
                    rc = 0
                except R.StopSignal as e:
                    tick.stop(e.cond.code, str(e.cond))
                    rc = 5
                except ct.StopCondition as e:
                    tick.stop(e.code, str(e))
                    rc = 5
        self._after_tick(tick)
        return rc

    # ------------------------------------------------------- lifecycle
    def step(self):
        """Perform everything due now; return next wake (epoch s) or
        None when finished."""
        if self.finished:
            return None
        handler = getattr(self, "_step_%s" % self.state.lower())
        return handler()

    def _step_init(self):
        # Verification checks 1-3 (design sec. 4.3) run in VERIFY; INIT
        # only loads the predecessor manifest (read-only).
        if self.k > 1:
            self.pred_manifest = self._read_remote(
                seg_branch(self.k - 1, self.ns), "manifest.json")
        self.state = "VERIFY"
        return self.clock.now_s()

    def _refuse(self, check, detail):
        doc = {"schema": "cap1-refused/1", "segment_index": self.k,
               "run_id": self.run_id, "check_failed": check,
               "detail": detail,
               "at_local": G.iso_local_label(self.clock.now_s()),
               "venue_requests_made": 0}
        try:
            self._publish("refused.json", doc)
        except Exception:
            pass
        self._event("REFUSED", check=check, detail=detail)
        self.finished = True
        self.exit_reason = "REFUSED:%s" % check
        self.exit_code = 3
        return None

    def _step_verify(self):
        fz = self.fz
        if self.k == 1:
            if self._dispatch_input != fz["_sha256"]:
                return self._refuse(1, "dispatch input != freeze hash")
        else:
            m = self.pred_manifest
            if m is None:
                return self._refuse(1, "predecessor manifest missing")
            if self._dispatch_input != G.doc_hash(m):
                return self._refuse(
                    1, "dispatch input != predecessor manifest hash")
            if m.get("successor_segment_index") != self.k:
                return self._refuse(1, "manifest names another "
                                       "successor")
            if m.get("predecessor_manifest_sha256") is None:
                return self._refuse(2, "manifest chain field missing")
            if m.get("freeze_sha256") != fz["_sha256"]:
                return self._refuse(2, "manifest freeze hash mismatch")
            # Walk the manifest chain back to the freeze hash (sec.
            # 5.3): every link must hash-match its successor's pointer.
            cur = m
            while cur.get("segment_index", 1) > 1:
                prev = self._read_remote(
                    seg_branch(cur["segment_index"] - 1, self.ns),
                    "manifest.json")
                if prev is None or G.doc_hash(prev) != \
                        cur.get("predecessor_manifest_sha256"):
                    return self._refuse(2, "manifest chain walk "
                                           "breaks at seg %d"
                                        % (cur["segment_index"] - 1))
                cur = prev
            if cur.get("predecessor_manifest_sha256") != fz["_sha256"]:
                return self._refuse(2, "manifest chain does not root "
                                       "at the freeze hash")
            own = m.get("ownership", {})
            if own.get("successor_lo_ms") != self.b_lo or \
                    own.get("successor_hi_ms") != self.b_hi:
                return self._refuse(3, "ownership tiling mismatch")
            req = m.get("requests", {})
            self.seed_initial = req.get("used_global_attested", 0) \
                + req.get("tail_reserve", 0)
            cap_total = fz["budgets"]["total_requests"]
            if self.seed_initial > cap_total:
                return self._refuse(5, "budget arithmetic overflow")
            self.consec_over = m.get("clock", {}).get(
                "consecutive_over_250ms", 0)
            # Predecessor chunk verification (sec. 5.3): recompute the
            # attested head at the attested line count from chunks.
            if not self._verify_pred_chunks(m):
                return self._refuse(2, "predecessor chunk head does "
                                       "not reproduce manifest")
            # Handoff market list integrity (its hash is in the
            # manifest); the list itself is consumed at START.
            hm = self._read_remote(seg_branch(self.k - 1, self.ns),
                                   "handoff_markets.json")
            if hm is None or G.doc_hash(hm) != \
                    m.get("successor_markets_sha256"):
                return self._refuse(2, "handoff market list hash "
                                       "mismatch")
            self._handoff_markets = hm.get("markets", {})
        self.state = "CLAIM"
        return self.clock.now_s()

    def _verify_pred_chunks(self, manifest):
        """Walk the predecessor's published chunks; the chunk whose
        cumulative cohort line count equals the manifest's attested
        count must carry the manifest's attested head hash."""
        lines_want = manifest.get("chain_lines", {}).get("cohort")
        head_want = manifest.get("chain_head_hash")
        if lines_want is None:
            return False
        idx = 0
        while True:
            chunk = self._read_remote(
                seg_branch(self.k - 1, self.ns), "chunks/chunk-%04d.json" % idx)
            if chunk is None:
                return False
            files = chunk.get("files", {})
            cohort = files.get("cohort", {})
            if cohort.get("to_line") == lines_want:
                return chunk.get("chain_heads", {}).get(
                    "cohort") == head_want
            if cohort.get("to_line", 0) > lines_want:
                return False
            idx += 1
            if idx > 10000:
                return False

    # ------------------------------------------------ Fix A: gate
    def _verify_own_publication(self, attested_lines, attested_head,
                                require_local_complete=False):
        """Publication-completeness gate: RE-READ this segment's own
        chunks from the backend (the same publisher.read/list path
        successors and the seal use — never in-memory counters).
        Returns (ok, detail); self._last_gate_verified_chunks holds
        the number of chunks re-read and verified."""
        self._last_gate_verified_chunks = 0
        if attested_lines is None:
            return False, "attested_lines missing"
        try:
            inventory = _chunk_inventory(self.publisher, self.branch)
        except Exception as e:
            return False, "chunk inventory failed: %s" % \
                G.sanitize_error_text(e)
        indices = [i for i, _p in inventory]
        if not _is_contiguous(indices):
            return False, "chunk indices non-contiguous: %s" % \
                (indices[:20],)
        missing = [i for i in sorted(self._chunk_hashes)
                   if i not in set(indices)]
        if missing:
            return False, ("published chunk indices missing on "
                           "re-read: %s" % (missing[:20],))
        chunks = []
        for idx, relpath in inventory:
            raw = self.publisher.read(self.branch, relpath)
            if raw is None:
                return False, "listed chunk %d unreadable" % idx
            if idx in self._chunk_hashes and \
                    G.sha256_bytes(raw) != self._chunk_hashes[idx]:
                return False, ("chunk %d re-read bytes differ from "
                               "published bytes" % idx)
            try:
                chunks.append(json.loads(raw))
            except Exception as e:
                return False, "chunk %d unparseable: %s" % (idx, e)
        self._last_gate_verified_chunks = len(chunks)
        # Reconstruction from the re-read chunks must reach the
        # attested cohort line count with the attested head.
        attested_ok = False
        if attested_lines == 0 and attested_head in (None, "GENESIS"):
            attested_ok = True
        for chunk in chunks:
            cohort = chunk.get("files", {}).get("cohort", {})
            if cohort.get("to_line") == attested_lines:
                if chunk.get("chain_heads", {}).get(
                        "cohort") == attested_head:
                    attested_ok = True
                break
            if cohort.get("to_line", 0) > attested_lines:
                break
        if not attested_ok:
            return False, ("no re-read chunk reaches attested "
                           "cohort lines=%s head=%s" % (
                               attested_lines, attested_head))
        if require_local_complete:
            # Tail completeness (HANDED_OFF / WINDOW_END): the
            # reconstructed files must cover ALL local lines, so a
            # failed tail chunk cannot hide unpublished captures
            # behind an already-attested manifest head.
            dest = tempfile.mkdtemp(prefix="gate-reconstruct-")
            try:
                counts = reconstruct_chain_files(
                    self.publisher, self.branch, dest)
            except Exception as e:
                return False, "reconstruction failed: %s" % \
                    G.sanitize_error_text(e)
            for name in ("cohort", "ops", "raw_cohort", "raw_ops"):
                local_n = len(self._chain_lines(name))
                if counts.get(name, 0) != local_n:
                    return False, (
                        "reconstructed %s lines %s != local %s"
                        % (name, counts.get(name, 0), local_n))
        return True, "verified %d chunks; attested cohort lines=%s" \
            % (len(chunks), attested_lines)

    def _block_publication_incomplete(self, attested_lines,
                                      attested_head, detail):
        info = {"attested_lines": attested_lines,
                "attested_head": attested_head,
                "verified_chunks": self._last_gate_verified_chunks,
                "detail": str(detail)}
        self._publication_gate_info = info
        self._event("PUBLICATION_GATE_FAILED",
                    attested_lines=attested_lines,
                    attested_head=attested_head,
                    verified_chunks=self._last_gate_verified_chunks,
                    detail=str(detail))
        self._terminate("BLOCKED:PUBLICATION_INCOMPLETE", 7)

    def _claim_heartbeat_age(self):
        fn = getattr(self.claims, "heartbeat_age", None)
        if fn is not None:
            return fn(self.k, self.clock.now_s())
        # Git-backed claims: liveness is the segment branch's
        # heartbeat.json (at_local is an iso_local_label timestamp).
        hb = self._read_remote(self.branch, "heartbeat.json")
        if hb and hb.get("at_local"):
            try:
                last_ms = G.parse_iso(hb["at_local"][:19] + "Z")
                return self.clock.now_s() - last_ms / 1000.0
            except Exception:
                return None
        # No heartbeat record: fall back to the claim's own
        # timestamp (last sign of life of a job killed before its
        # first heartbeat).
        read = getattr(self.claims, "read", None)
        if read is not None:
            try:
                doc = read(self.k)
            except Exception:
                doc = None
            if doc and doc.get("claimed_at_local"):
                try:
                    claimed_ms = G.parse_iso(
                        doc["claimed_at_local"][:19] + "Z")
                    return self.clock.now_s() - claimed_ms / 1000.0
                except Exception:
                    return None
        return None

    def _step_claim(self):
        against = self.fz["_sha256"] if self.k == 1 else \
            G.doc_hash(self.pred_manifest)
        try:
            self.claims.claim(self.k, self.run_id, against,
                              self.clock.now_s())
        except claims_mod.ClaimHeld:
            pass                      # idempotent self-claim
        except claims_mod.ClaimLost as e:
            if not getattr(self, "recovery", False):
                return self._refuse(4, "atomic claim lost: %s" % e)
            # Recovery path: CAS takeover of a STALE claim only
            # (claims.py). A live claim is never taken over.
            age = self._claim_heartbeat_age()
            stale = self.fz["phase"].get("claim_stale_s", 900)
            try:
                self.claims.takeover(self.k, self.run_id, against,
                                     self.clock.now_s(), stale, age)
                self._event("RECOVERY_TAKEOVER", heartbeat_age_s=age)
            except claims_mod.ClaimHeld:
                pass
            except claims_mod.ClaimLost as e2:
                return self._refuse(4, "takeover refused: %s" % e2)
        self.state = "CLOCK_GATE"
        return self.clock.now_s()

    def _step_clock_gate(self):
        rec = self.section_c_provider()
        self.clock_rec = rec
        der = (rec or {}).get("derivation") or {}
        bound = der.get("bound_ms")
        thr = self.fz["thresholds"]
        summary = None
        from . import clock_evidence as ce
        summary = ce.summarize_section_c(rec)
        if der.get("status") != "DERIVED" or bound is None:
            return self._abort_clock("ABORTED:CLOCK_UNPROVEN")
        if self.k == 1 and bound > thr["activation_clock_bound_ms"]:
            return self._abort_clock("ABORTED:CLOCK_GATE")
        if self.k > 1 and bound > thr["per_observation_max_ms"]:
            return self._abort_clock("ABORTED:CLOCK_UNPROVEN")
        if bound > thr["activation_clock_bound_ms"]:
            self.clock_degraded = True
            self._event("CLOCK_DEGRADED", bound_ms=bound)
        self._clock_summary = summary
        self.state = "START"
        return self.clock.now_s()

    def _abort_clock(self, reason):
        self._write_final(reason)
        self._event("ABORTED", reason=reason)
        self.finished = True
        self.exit_reason = reason
        self.exit_code = 4
        return None

    def _seed_handoff_markets(self):
        """Seed this segment's cohort chain with a DISCOVERY record
        for the markets the predecessor harvested into my interval
        (design sec. 3.3: early slots belong to the owner; without
        this record the owner could not know them before its first
        own discovery poll at B_k, and their pre-B slots would be
        uncapturable). Provenance is labelled HANDOFF."""
        markets = []
        for ticker, m in sorted(
                getattr(self, "_handoff_markets", {}).items()):
            close = cap1.parse_iso_ms(m["close_time"])
            if self.b_lo <= close < self.b_hi:
                markets.append({
                    "ticker": m["ticker"],
                    "series_ticker": m["series_ticker"],
                    "open_time": m["open_time"],
                    "close_time": m["close_time"]})
        if not markets:
            return
        chain = cap1.ChainStore(self._chain_path("cohort"))
        for rec in chain.records:
            if rec.get("kind") == "DISCOVERY" and \
                    rec.get("discovery_source") == "HANDOFF" and \
                    rec.get("from_segment") == self.k - 1:
                return      # already seeded by the killed run
        chain.append({
            "kind": "DISCOVERY", "markets": markets,
            "recorded_at_local": G.iso_of_ms(self._now_ms()),
            "discovery_source": "HANDOFF",
            "from_segment": self.k - 1})

    def _restore_from_branch(self):
        """Recovery resume (design sec. 8): if this segment's branch
        already carries published chunks, a previous run of this
        segment was killed after publishing them. Reconstruct the
        chains from the published chunks into this run's (fresh)
        workspace, continue the chunk sequence after the existing
        chunks, and carry an UPPER-BOUND estimate of the killed
        run's venue usage into the governor seed — the killed run's
        exact counter died with it, so the segment's attested
        actuals become a bound, not an exact count (disclosed in
        RECOVERY_FEASIBILITY.md; the seal's budget check governs by
        bound for any chain containing a recovered segment)."""
        # Fix C.1: the chunk inventory is established AUTHORITATIVELY
        # via publisher.list(); a single transient read->None can no
        # longer truncate it (P4 finding a'). Any doubt makes the
        # restore UNVERIFIABLE and blocks the segment fail-closed
        # instead of guessing a too-low _chunk_idx.
        try:
            inventory = _chunk_inventory(self.publisher, self.branch)
        except Exception as e:
            return self._block_restore_unverifiable(
                "chunk inventory failed: %s"
                % G.sanitize_error_text(e))
        if not inventory:
            return False    # died pre-chunk: legitimate fresh start
        indices = [i for i, _p in inventory]
        if not _is_contiguous(indices):
            return self._block_restore_unverifiable(
                "chunk indices non-contiguous: %s" % (indices[:20],))
        try:
            reconstruct_chain_files(self.publisher, self.branch,
                                    self.workdir)
        except Exception as e:
            return self._block_restore_unverifiable(
                "chain reconstruction failed: %s"
                % G.sanitize_error_text(e))
        last_chunk = None
        for idx, relpath in inventory:
            raw = self.publisher.read(self.branch, relpath)
            if raw is None:
                return self._block_restore_unverifiable(
                    "listed chunk %d reads as None" % idx)
            try:
                chunk = json.loads(raw)
            except Exception as e:
                return self._block_restore_unverifiable(
                    "listed chunk %d unparseable: %s" % (idx, e))
            self._chunk_hashes[idx] = G.sha256_bytes(raw)
            for name, fr in chunk.get("files", {}).items():
                self._published_lines[name] = fr.get("to_line", 0)
            last_chunk = chunk
        self._chunk_idx = len(inventory)
        # Cross-check: reconstructed local lines must equal the last
        # chunk's cumulative to_line for every file it carries.
        if last_chunk is not None:
            for name, fr in last_chunk.get("files", {}).items():
                try:
                    local_n = len(self._chain_lines(name))
                except Exception as e:
                    return self._block_restore_unverifiable(
                        "reconstructed %s unreadable: %s" % (name, e))
                if local_n != fr.get("to_line", 0):
                    return self._block_restore_unverifiable(
                        "reconstructed %s lines %s != last chunk "
                        "to_line %s" % (name, local_n,
                                        fr.get("to_line", 0)))
        records = sum(len(self._chain_lines(name))
                      for name in ("cohort", "ops"))
        per_fetch = 1 + self.fz["budgets"]["max_retries"]
        self._recovered_usage_estimate = \
            records * per_fetch \
            + len(self.fz["series"]) * per_fetch * 10
        self._event("RECOVERY_CHAIN_RESTORED",
                    chunks=self._chunk_idx,
                    records=records,
                    usage_estimate=self._recovered_usage_estimate)
        return True

    def _block_restore_unverifiable(self, detail):
        self._event("RESTORE_UNVERIFIABLE",
                    detail=G.sanitize_error_text(detail))
        self._terminate("BLOCKED:RESTORE_UNVERIFIABLE", 7)
        return False

    def _step_start(self):
        self._restore_from_branch()
        if self.finished:
            return None
        self._write_runner_freeze()
        self._seed_state()
        if self.k > 1:
            self._seed_handoff_markets()
        started = {
            "schema": "cap1-started/1", "segment_index": self.k,
            "run_id": self.run_id,
            "started_at_local": G.iso_local_label(self.clock.now_s()),
            "dispatch_input": self._dispatch_input,
            "freeze_sha256": self.fz["_sha256"],
            "synthetic": bool(self.fz.get("synthetic")),
            "runner": {"leg": self.fz.get("runner_leg"),
                       "image_version": self.image_version},
            "clock": getattr(self, "_clock_summary", None),
            "clock_degraded": self.clock_degraded,
            "seed_global": self.seed_initial,
            "consecutive_over_250ms_inherited": self.consec_over}
        # started.json is write-once evidence of the segment's FIRST
        # start (the claim winner). A recovery start publishes its
        # own record alongside it instead of colliding with it.
        started_name = "started.json"
        if self._read_remote(self.branch, "started.json") is not None:
            started_name = "recovery_started.json"
        try:
            self._publish(started_name, started)
        except Exception as e:
            self._event("PUBLICATION_FAILURE", where="started",
                        error=str(e))
        now = self.clock.now_s()
        self._next_hb = now
        self._next_chunk = now + self.fz["phase"]["chunk_interval_s"]
        if self.k == 1:
            self.state = "ACTIVE"
            self._next_tick = self.fz["_a_ms"] / 1000.0
        elif now * 1000 >= self.b_lo:
            # Late start (recovery): the interval is already open.
            self.state = "ACTIVE"
            self._next_tick = now
        elif now * 1000 < self.t_lead_b:
            self.state = "LEADIN_A"
            self._next_tick = None
        else:
            self.state = "LEADIN_B"
            self.pacer.set_phase_b(self.k % 2)
            self._next_tick = now
        return now

    # ------------------------------------------------------------ main
    def _due_wake(self, candidates):
        now = self.clock.now_s()
        future = [c for c in candidates if c is not None]
        return min(future) if future else now + 60

    def _step_leadin_a(self):
        now = self.clock.now_s()
        self._housekeeping(now)
        if self.finished:
            return None
        if now * 1000 >= self.t_lead_b:
            self.state = "LEADIN_B"
            self.pacer.set_phase_b(self.k % 2)
            self._next_tick = now
            return now
        return self._due_wake([self.t_lead_b / 1000.0, self._next_hb,
                               self._next_chunk])

    def _step_leadin_b(self):
        now = self.clock.now_s()
        self._housekeeping(now)
        if self.finished:
            return None
        if now * 1000 >= self.b_lo:
            self.state = "ACTIVE"
            self.pacer.set_normal()
            self._next_tick = now
            return now
        if self._next_tick is not None and now >= self._next_tick:
            # The FIRST lead-in tick is a FULL tick: one paced
            # discovery poll. A market closing exactly at B_k opens
            # at B_k - 900s, after the predecessor's manifest, so
            # neither the handoff list nor any earlier poll can name
            # it; without this poll its pre-B slots are unknowable
            # and section 3.3's ownership capture is impossible.
            # Deviation D-3 (RATE_CONCURRENCY_PROOF.md): the poll
            # passes through the Phase-B pacer, so the proven
            # cross-job bounds are unchanged.
            mode = "FULL" if not self._lead_discovery_done \
                else "CAPTURE_ONLY"
            self._run_tick(mode)
            self._lead_discovery_done = True
            self._next_tick = now + self.fz["phase"]["tick_interval_s"]
            if self.finished:
                return None
        return self._due_wake([self.b_lo / 1000.0, self._next_tick,
                               self._next_hb, self._next_chunk])

    def _step_active(self):
        now = self.clock.now_s()
        self._housekeeping(now)
        if self.finished:
            return None
        now_ms = now * 1000
        if self.k < self.n and now_ms >= self.t_dispatch:
            # A tick due at this instant runs FIRST, so a discovery
            # poll landing exactly on manifest time is inside the
            # attested counts and the harvest.
            if self._next_tick is not None and now >= self._next_tick:
                self._run_tick("FULL")
                self._next_tick = now + \
                    self.fz["phase"]["tick_interval_s"]
                if self.finished:
                    return None
            self._publish_manifest_and_dispatch()
            if self.finished:
                return None
            self.state = "YIELD"
            self.pacer.set_phase_b(self.k % 2)
            self._next_tick = now
            return now
        if self.k == self.n and now_ms >= self.fz["_end_ms"]:
            if not self._sealed_tick_done:
                # Final due slots (POST-CLOSE at the boundary) via a
                # capture-only tick; _run_tick's hard-end guard blocks
                # any request at/after A+48h, and no discovery poll is
                # issued at the boundary (design sec. 7.6).
                self._run_tick("CAPTURE_ONLY")
                self._sealed_tick_done = True
                if self.finished:
                    return None
            self._finish_window_end()
            return None
        if self._next_tick is not None and now >= self._next_tick:
            self._run_tick("FULL")
            self._next_tick = now + self.fz["phase"]["tick_interval_s"]
            if self.finished:
                return None
            self._maybe_boundary_records()
        return self._due_wake([
            self._next_tick, self._next_hb, self._next_chunk,
            (self.t_dispatch / 1000.0) if self.k < self.n else
            (self.fz["_end_ms"] / 1000.0)])

    def _step_yield(self):
        now = self.clock.now_s()
        self._housekeeping(now)
        if self.finished:
            return None
        now_ms = now * 1000
        # One handoff-completeness discovery at B-300s (through the
        # pacer; see RATE_CONCURRENCY_PROOF deviation note D-1).
        if not self._final_discovery_done and \
                now_ms >= self.b_hi - 300_000:
            self._run_tick("FULL")
            self._final_discovery_done = True
            if self.finished:
                return None
        elif self._next_tick is not None and now >= self._next_tick:
            self._run_tick("CAPTURE_ONLY")
            self._next_tick = now + self.fz["phase"]["tick_interval_s"]
            if self.finished:
                return None
        self._maybe_attestation_check(now)
        if now_ms >= self.t_tail_end:
            self._finish_handed_off()
            return None
        return self._due_wake([self._next_tick, self._next_hb,
                               self._next_chunk,
                               self.t_tail_end / 1000.0])

    # ------------------------------------------------------- housework
    def _housekeeping(self, now):
        if self._next_hb is not None and now >= self._next_hb:
            self._publish_heartbeat()
            self._next_hb = now + self.fz["phase"]["heartbeat_s"]
            if self.k > 1 and self.crosscheck == "PENDING":
                self._crosscheck()
        if self._next_chunk is not None and now >= self._next_chunk:
            self._publish_chunk()
            if self.finished:
                return
            self._next_chunk = now + \
                self.fz["phase"]["chunk_interval_s"]
            self._storage_check()

    def _publish_heartbeat(self):
        hb = {"schema": "cap1-heartbeat/1", "segment_index": self.k,
              "run_id": self.run_id,
              "at_local": G.iso_local_label(self.clock.now_s()),
              "state": self.state, "gov_total_global": self._gov_total,
              "chunks_published": self._chunk_idx,
              "consecutive_over_250ms": self.consec_over}
        try:
            self._publish("heartbeat.json", hb)
            self.claims.heartbeat(self.k, self.clock.now_s())
        except Exception as e:
            self._pub_failures += 1
            self._event("PUBLICATION_FAILURE", where="heartbeat",
                        error=str(e))

    def _chain_lines(self, name):
        path = self._chain_path(name)
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            return [ln.rstrip("\n") for ln in f if ln.strip()]

    def _publish_chunk(self):
        files = {}
        heads = {}
        for name in ("cohort", "ops", "raw_cohort", "raw_ops"):
            lines = self._chain_lines(name)
            start = self._published_lines[name]
            files[name] = {"from_line": start, "to_line": len(lines),
                           "lines": lines[start:]}
            if name in ("cohort", "ops"):
                path = self._chain_path(name)
                heads[name] = cap1.ChainStore(path).tip \
                    if os.path.exists(path) else "GENESIS"
        chunk = {"schema": G.CHUNK_SCHEMA, "segment_index": self.k,
                 "chunk_index": self._chunk_idx,
                 "published_at_local": G.iso_local_label(
                     self.clock.now_s()),
                 "files": files, "chain_heads": heads}
        idx = self._chunk_idx
        data = (G.canon(chunk) + "\n").encode()
        try:
            self._publish("chunks/chunk-%04d.json" % idx, data)
        except Exception as e:
            self._pub_failures += 1
            self._event("PUBLICATION_FAILURE", where="chunk",
                        error=str(e))
            if isinstance(e, IntegrityError):
                # Fix C.2: write-once conflict at the current index
                # means local and published state have irreconcilably
                # diverged. The Fix-B record was already written
                # inside _publish. Halt instead of retrying this
                # index for the rest of the window.
                self._terminate("ABORTED:CHAIN_INTEGRITY", 6)
            return
        # Fix A.1: record the exact published bytes' hash.
        self._chunk_hashes[idx] = G.sha256_bytes(data)
        for name in files:
            self._published_lines[name] = files[name]["to_line"]
        self._chunk_idx += 1

    def _storage_check(self):
        total = self.bytes_published
        for root, _d, names in os.walk(self.workdir):
            total += sum(os.path.getsize(os.path.join(root, n))
                         for n in names)
        remaining = self.n - self.k
        projection = total + (total // max(self.k, 1)) * remaining \
            if self.k else total
        if projection > self.fz["budgets"]["storage_cap_bytes"]:
            self._event("STORAGE_STOP", projection=projection)
            tick = self._make_tick()
            tick.stop("STORAGE_PROJECTION",
                      "projected %d bytes over cap" % projection)
            self._after_tick(tick)

    # --------------------------------------------------------- boundary
    def _maybe_boundary_records(self):
        """Segment 1 appends BOUNDARY_INELIGIBLE records for markets
        closing in [A, A+300s) (boundary spec section 2). Pre-A slots
        are NOT recorded by the supervisor: the frozen runner GAPs
        beyond-lateness ones as MISSED_SLOT, and the composed reader
        reclassifies those GAPs into BOUNDARY_EXCLUDED /
        SLOT_BEFORE_ACTIVATION (the spec assigns the categories to
        the verdict-reader layer, section 4)."""
        if self.k != 1 or self._boundary_done:
            return
        tick = self._make_tick()
        known = tick.known_markets()
        if not known:
            return
        self._boundary_done = True
        a_ms = self.fz["_a_ms"]
        path = self._chain_path("cohort")
        chain = cap1.ChainStore(path)
        for ticker, (seg, m) in sorted(known.items()):
            if seg != "cohort":
                continue
            close_ms = cap1.parse_iso_ms(m["close_time"])
            if a_ms <= close_ms < a_ms + 300_000:
                chain.append({"kind": "BOUNDARY_INELIGIBLE",
                              "ticker": ticker,
                              "close_time": m["close_time"]})

    # -------------------------------------------------- manifest/final
    def _tail_reserve(self):
        tick = self._make_tick()
        now = self._now_ms()
        per_fetch = 1 + self.fz["budgets"]["max_retries"]
        slots = 0
        for ticker, (seg, m) in tick.known_markets().items():
            if seg != "cohort":
                continue
            chain = tick.chains[seg]
            done = tick.captured(chain, ticker)
            open_ms = cap1.parse_iso_ms(m["open_time"])
            close_ms = cap1.parse_iso_ms(m["close_time"])
            for slot in COHORT_SLOTS:
                if slot in done:
                    continue
                base = open_ms if slot == "OPEN" else close_ms
                due = base + R.SLOT_OFFSETS[slot]
                if now <= due + LATENESS_MS:
                    slots += 1
        discovery = len(self.fz["series"]) * 1 * per_fetch
        return slots * per_fetch + discovery, slots

    def _record_counts(self):
        counts = {}
        for name in ("cohort", "ops"):
            path = self._chain_path(name)
            if os.path.exists(path):
                counts[name] = cap1.health_summary(
                    cap1.ChainStore(path))
            else:
                counts[name] = {"tip_hash": "GENESIS"}
        return counts

    def _build_manifest(self, seal=False):
        counts = self._record_counts()
        tick = self._make_tick()
        used = tick.state["gov"]["total_requests"]
        reserve, nslots = (0, 0) if seal else self._tail_reserve()
        bounds = G.segment_bounds(self.fz)
        succ = None
        if not seal:
            succ = {"lo_ms": bounds[self.k][0], "hi_ms": bounds[self.k][1]}
        markets_doc = {"schema": "cap1-handoff-markets/1",
                       "segment_index": self.k,
                       "markets": self.ownership_filter.harvested}
        self._publish("handoff_markets.json", markets_doc)
        from . import clock_evidence as ce
        clock_sum = ce.summarize_section_c(self.clock_rec) or {}
        clock_sum["consecutive_over_250ms"] = self.consec_over
        manifest = {
            "schema": G.MANIFEST_SCHEMA,
            "segment_index": self.k,
            "successor_segment_index": None if seal else self.k + 1,
            "freeze_sha256": self.fz["_sha256"],
            "window": {"activation_A": self.fz["activation_A"],
                       "hard_end": self.fz["hard_end"]},
            "ownership": {"lo_ms": self.b_lo, "hi_ms": self.b_hi,
                          "successor_lo_ms": succ["lo_ms"] if succ
                          else None,
                          "successor_hi_ms": succ["hi_ms"] if succ
                          else None},
            "predecessor_manifest_sha256":
                self.fz["_sha256"] if self.k == 1 else
                G.doc_hash(self.pred_manifest),
            "chain_head_hash": counts["cohort"].get("tip_hash"),
            "chain_heads": {n: counts[n].get("tip_hash")
                            for n in counts},
            "chain_lines": {n: len(self._chain_lines(n))
                            for n in ("cohort", "ops")},
            "record_counts": counts,
            "requests": {"seed_global": self.seed_initial
                         - self.seed_credit,
                         "used_global_attested": used,
                         "tail_reserve": reserve,
                         "successor_allocation":
                             self.fz["budgets"]["total_requests"]
                             - used - reserve},
            "clock": clock_sum,
            "runner": {"leg": self.fz.get("runner_leg"),
                       "image_version": self.image_version,
                       "run_id": self.run_id},
            "successor_markets_sha256": G.doc_hash(markets_doc),
            "published_at_local": G.iso_local_label(
                self.clock.now_s())}
        return manifest

    def _publish_manifest_and_dispatch(self):
        self._publish_chunk()          # attested head must be published
        if self.finished:
            return                     # chunk integrity halt (Fix C.2)
        # NOTE: _build_manifest publishes handoff_markets.json; that
        # file is hash-referenced only by the manifest, so if the
        # gate below blocks the manifest, the handoff file remains
        # on the branch unreferenced (disclosed in the fix report).
        self.manifest = self._build_manifest()
        self.manifest_sha = G.doc_hash(self.manifest)
        attested_lines = self.manifest.get("chain_lines", {}).get(
            "cohort")
        attested_head = self.manifest.get("chain_head_hash")
        ok, detail = self._verify_own_publication(
            attested_lines, attested_head)
        if not ok:
            self._block_publication_incomplete(
                attested_lines, attested_head, detail)
            return
        try:
            self._publish("manifest.json", self.manifest)
        except Exception as e:
            self._event("PUBLICATION_FAILURE", where="manifest",
                        error=str(e))
            self._terminate("STOP:PUBLICATION", 5)
            return
        self._attempt_dispatch()

    def _attempt_dispatch(self):
        self._dispatch_attempts += 1
        nxt = self.k + 1
        try:
            self.dispatcher.dispatch(
                segment_index=nxt, manifest_sha256=self.manifest_sha,
                role="segment")
            self._event("DISPATCHED", successor=nxt,
                        attempt=self._dispatch_attempts)
        except Exception as e:
            self._event("DISPATCH_FAILED", successor=nxt,
                        attempt=self._dispatch_attempts, error=str(e))

    def _maybe_attestation_check(self, now):
        if self.manifest is None or self._attestation_confirmed:
            return
        started = self._read_remote(seg_branch(self.k + 1, self.ns),
                                    "started.json")
        if started is not None:
            self._attestation_confirmed = True
            return
        offsets = self.fz["phase"].get("redispatch_offsets_s",
                                       [360, 720])
        # attempts: 1 = initial dispatch; due times for attempts 2, 3
        # are t_dispatch + offsets[0], t_dispatch + offsets[1].
        idx = self._dispatch_attempts - 1
        if idx < len(offsets) and \
                now >= self.t_dispatch / 1000.0 + offsets[idx]:
            self._attempt_dispatch()

    # ------------------------------------------------------ crosscheck
    def _crosscheck(self):
        pred = self._read_remote(seg_branch(self.k - 1, self.ns), "final.json")
        if pred is None:
            grace = self.fz["phase"].get("final_grace_s", 900)
            if self._now_ms() > self.b_lo + grace * 1000:
                self.crosscheck = "UNRESOLVED"
                self._event("CROSSCHECK_UNRESOLVED")
            return
        exit_reason = pred.get("exit_reason", "")
        if exit_reason.startswith("STOP") or \
                exit_reason.startswith("QUARANTINED"):
            self.crosscheck = "PREDECESSOR_STOP"
            self._event("CROSSCHECK_PREDECESSOR_STOP",
                        exit_reason=exit_reason)
            self._write_final("ABORTED:PREDECESSOR_STOP")
            self.finished = True
            self.exit_reason = "ABORTED:PREDECESSOR_STOP"
            self.exit_code = 4
            return
        m = self.pred_manifest
        req = m.get("requests", {})
        final_global = pred.get("requests", {}).get("final_global")
        if final_global is None or \
                final_global > req.get("used_global_attested", 0) \
                + req.get("tail_reserve", 0):
            self.crosscheck = "FAILED"
            self._event("CROSSCHECK_FAILED",
                        detail="final exceeds attested + reserve")
            self._write_final("ABORTED:CHAIN_INTEGRITY")
            self.finished = True
            self.exit_reason = "ABORTED:CHAIN_INTEGRITY"
            self.exit_code = 6
            return
        # Section-F adoption: the manifest carried the counter as of
        # manifest time, but the predecessor kept capturing through
        # its yield tail AFTER publishing it. The predecessor's full
        # chain is published by now (its final chunk precedes
        # final.json), so recompute the exact trailing
        # consecutive-CLOCK_EXCESSIVE count from the published chain
        # and adopt it — it supersedes the manifest snapshot.
        adopted = self._adopt_pred_clock_counter()
        if adopted is not None:
            self.consec_over = adopted
        # Credit the reserve back (design sec. 7.1): the segment's
        # governor counter was seeded at attested + reserve.
        # seed_credit is the amount SUBTRACTED from the counter, so
        # the effective seed is seed_initial - seed_credit.
        self.seed_credit = (req.get("used_global_attested", 0)
                            + req.get("tail_reserve", 0)) - final_global
        state_path = os.path.join(self.workdir, "state.json")
        if os.path.exists(state_path):
            state = G.read_json(state_path)
            state["gov"]["total_requests"] -= self.seed_credit
            G.write_json(state_path, state)
            self._gov_total = state["gov"]["total_requests"]
        self.crosscheck = "OK"
        self._event("CROSSCHECK_OK", seed_credit=self.seed_credit,
                    consec_over_adopted=self.consec_over)

    def _adopt_pred_clock_counter(self):
        """Trailing consecutive-CLOCK_EXCESSIVE count over the
        predecessor's complete published cohort chain (same rule as
        _after_tick). None if the chain cannot be reconstructed —
        the caller then keeps the manifest-carried value."""
        import tempfile
        dest = tempfile.mkdtemp(prefix="pred-final-")
        try:
            reconstruct_chain_files(self.publisher,
                                    seg_branch(self.k - 1, self.ns), dest)
            chain = cap1.ChainStore(os.path.join(dest, "cohort.json"))
        except Exception:
            self._event("CROSSCHECK_CLOCK_ADOPT_FAILED")
            return None
        count = 0
        for rec in chain.records:
            if rec.get("kind") == "OBSERVATION":
                if rec.get("invalid_reason") == "CLOCK_EXCESSIVE":
                    count += 1
                elif rec.get("validity") == "VALID":
                    count = 0
        return count

    # ------------------------------------------------------------ end
    def _terminate(self, exit_reason, exit_code):
        if self.finished:
            return
        self._write_final(exit_reason)
        self.finished = True
        self.exit_reason = exit_reason
        self.exit_code = exit_code

    def _finish_handed_off(self):
        self._publish_chunk()
        if self.finished:
            return                     # chunk integrity halt (Fix C.2)
        if self.manifest is not None:
            attested_lines = self.manifest.get(
                "chain_lines", {}).get("cohort")
            attested_head = self.manifest.get("chain_head_hash")
            ok, detail = self._verify_own_publication(
                attested_lines, attested_head,
                require_local_complete=True)
            if not ok:
                self._block_publication_incomplete(
                    attested_lines, attested_head, detail)
                return
        self._write_final("HANDED_OFF")
        self._event("HANDED_OFF")
        self.finished = True
        self.exit_reason = "HANDED_OFF"
        self.exit_code = 0

    def _finish_window_end(self):
        self._publish_chunk()
        if self.finished:
            return                     # chunk integrity halt (Fix C.2)
        seal_manifest = self._build_manifest(seal=True)
        attested_lines = seal_manifest.get("chain_lines", {}).get(
            "cohort")
        attested_head = seal_manifest.get("chain_head_hash")
        ok, detail = self._verify_own_publication(
            attested_lines, attested_head,
            require_local_complete=True)
        if not ok:
            self._block_publication_incomplete(
                attested_lines, attested_head, detail)
            return
        try:
            self._publish("manifest.json", seal_manifest)
        except Exception as e:
            self._event("PUBLICATION_FAILURE", where="seal-manifest",
                        error=str(e))
        self.manifest = seal_manifest
        self.manifest_sha = G.doc_hash(seal_manifest)
        self._write_final("WINDOW_END")
        try:
            self.dispatcher.dispatch(
                segment_index=self.n + 1,
                manifest_sha256=self.manifest_sha, role="seal")
            self._event("DISPATCHED", successor="seal")
        except Exception as e:
            self._event("DISPATCH_FAILED", successor="seal",
                        error=str(e))
        self.finished = True
        self.exit_reason = "WINDOW_END"
        self.exit_code = 0

    def _write_final(self, exit_reason):
        try:
            counts = self._record_counts()
        except Exception:
            # A blocked/restore final must still be writable even
            # when the local chains are unreadable.
            counts = {"cohort": {"tip_hash": "GENESIS"},
                      "ops": {"tip_hash": "GENESIS"}}
        tick_state = None
        sp = os.path.join(self.workdir, "state.json")
        if os.path.exists(sp):
            tick_state = G.read_json(sp)
        final_global = tick_state["gov"]["total_requests"] \
            if tick_state else self.seed_initial
        from . import clock_evidence as ce
        clock_sum = ce.summarize_section_c(self.clock_rec) or {}
        clock_sum["consecutive_over_250ms"] = self.consec_over
        doc = {"schema": "cap1-final/1", "segment_index": self.k,
               "run_id": self.run_id, "exit_reason": exit_reason,
               "finished_at_local": G.iso_local_label(
                   self.clock.now_s()),
               "chain_heads": {n: counts[n].get("tip_hash")
                               for n in counts},
               "record_counts": counts,
               "requests": {
                   "seed_global": self.seed_initial - self.seed_credit,
                   "seed_at_start": self.seed_initial,
                   "seed_credit": self.seed_credit,
                   "final_global": final_global,
                   "segment_actual": final_global
                   - (self.seed_initial - self.seed_credit)},
               "clock": clock_sum,
               "crosscheck": self.crosscheck,
               "publication": {
                   "chunks_published": self._chunk_idx,
                   "failures": self._pub_failures,
                   "error_records": {
                       "count": self._pub_err_state["records"],
                       "record_failures":
                           self._pub_err_state["record_failures"],
                       "location": "errors/"}},
               "storage_bytes_published": self.bytes_published}
        if self._publication_gate_info is not None:
            doc["publication_gate"] = dict(self._publication_gate_info)
        try:
            self._publish("final.json", doc)
        except Exception as e:
            self._event("PUBLICATION_FAILURE", where="final",
                        error=str(e))
        return doc


def reconstruct_chain_files(publisher, branch, dest_dir):
    """Rebuild a segment's append-only files from its published
    chunks (the seal's evidence path: nothing is read from a segment
    job's local disk, because on GitHub that disk no longer exists).
    Returns {file_name: line_count}. Chunks are applied in index
    order; overlapping re-chunks are tolerated because each chunk
    states absolute line ranges — a later chunk's range wins."""
    os.makedirs(dest_dir, exist_ok=True)
    contents = {}
    # Fix C.3: inventory via publisher.list(); read exactly the
    # listed indices in order. A listed-but-unreadable chunk RAISES
    # instead of silently truncating the chain. (Legacy wrappers
    # without list() keep the old sequential-read behaviour via
    # _chunk_inventory's fallback.)
    inventory = _chunk_inventory(publisher, branch)
    indices = [i for i, _p in inventory]
    if not _is_contiguous(indices):
        raise ValueError("chunk indices non-contiguous on %s: %s"
                         % (branch, indices[:20]))
    chunks = []
    for idx, relpath in inventory:
        raw = publisher.read(branch, relpath)
        if raw is None:
            raise RuntimeError(
                "listed chunk %d on %s is unreadable" % (idx, branch))
        chunks.append(json.loads(raw))
    for chunk in chunks:
        for name, part in chunk.get("files", {}).items():
            lines = contents.setdefault(name, [])
            start = part["from_line"]
            if len(lines) < start:
                lines.extend([""] * (start - len(lines)))
            end = start + len(part["lines"])
            if len(lines) < end:
                lines.extend([""] * (end - len(lines)))
            lines[start:end] = part["lines"]
            fname = name + ".json" if name in ("cohort", "ops") else name
            contents.setdefault("_fname_" + name, fname)
    counts = {}
    for name, lines in contents.items():
        if name.startswith("_fname_"):
            continue
        fname = contents["_fname_" + name]
        with open(os.path.join(dest_dir, fname), "w",
                  encoding="utf-8") as f:
            for ln in lines:
                f.write(ln + "\n")
        counts[name] = len(lines)
    return counts


class SealJob:
    """Composes the window from published evidence only, writes the
    end-boundary accounting, and produces SEAL + composed verdict
    (design section 10.8). Makes no venue requests of any kind."""

    def __init__(self, *, freeze, run_id, publisher, claim_store,
                 work_root, events=None):
        self.fz = freeze
        self.run_id = run_id
        self.publisher = publisher
        self.claims = claim_store
        self.work_root = work_root
        self.events = events if events is not None else []
        self.n = len(freeze["segment_plan"]["lengths_s"])
        self.branch = seal_branch(G.evidence_namespace(freeze))
        self.verdict = None
        # Fix B (minimal, seal path): same error-record treatment.
        self._pub_err_state = _new_pub_error_state()

    def _publish(self, relpath, doc):
        data = (G.canon(doc) + "\n").encode() \
            if not isinstance(doc, bytes) else doc
        try:
            return self.publisher.publish(self.branch, relpath, data)
        except Exception as e:
            category = _attempt_pub_error_record(
                self.publisher, self.branch,
                segment_index=self.n + 1, run_id=self.run_id,
                relpath=relpath, exc=e,
                at_local=G.iso_local_label(
                    getattr(self, "_now_s", 0.0)),
                state=self._pub_err_state)
            self.events.append({
                "event": "PUBLICATION_FAILURE", "where": relpath,
                "category": category,
                "error_records": self._pub_err_state["records"],
                "record_failures":
                    self._pub_err_state["record_failures"]})
            raise

    def publication_error_summary(self):
        st = self._pub_err_state
        return {"total": st["total"],
                "categories": dict(st["categories"]),
                "records": st["records"],
                "record_failures": st["record_failures"]}

    def run(self, dispatch_input, now_s=0.0):
        self._now_s = now_s
        # Claim the seal role atomically (segment index n+1).
        try:
            self.claims.claim(self.n + 1, self.run_id, dispatch_input,
                              now_s)
        except claims_mod.ClaimLost:
            self.events.append({"event": "SEAL_CLAIM_LOST",
                                "run_id": self.run_id})
            return 3
        except claims_mod.ClaimHeld:
            pass
        # Fix C.3: a reconstruction / composition / publication
        # failure must fail LOUDLY — a recorded SEAL_FAILED event
        # and a nonzero return — never a verdict composed from a
        # silently truncated chain.
        try:
            segments = []
            seal_dir = os.path.join(self.work_root, "seal")
            os.makedirs(seal_dir, exist_ok=True)
            for k in range(1, self.n + 1):
                branch = seg_branch(k, G.evidence_namespace(self.fz))
                dest = os.path.join(seal_dir, "seg-%02d" % k)
                reconstruct_chain_files(self.publisher, branch, dest)
                manifest = self._read(branch, "manifest.json")
                final = self._read(branch, "final.json")
                segments.append({
                    "index": k,
                    "cohort_chain_path": os.path.join(
                        dest, "cohort.json"),
                    "manifest": manifest, "final": final})
            self.verdict = verdict_compose.compose(self.fz, segments)
            seal = {"schema": "cap1-seal/1",
                    "sealed_at_local": "recorded by publisher commit",
                    "run_id": self.run_id,
                    "freeze_sha256": self.fz["_sha256"],
                    "synthetic": bool(self.fz.get("synthetic")),
                    "verdict": self.verdict["verdict"],
                    "fail_reasons": self.verdict["fail_reasons"],
                    "segment_finals": [
                        {"segment": s["index"],
                         "exit_reason": (s["final"] or {}).get(
                             "exit_reason"),
                         "present": s["final"] is not None}
                        for s in segments]}
            for name, doc in (("SEAL.json", seal),
                              ("verdict.json", self.verdict)):
                self._publish(name, doc)
        except Exception as e:
            self.events.append({
                "event": "SEAL_FAILED", "run_id": self.run_id,
                "error": G.sanitize_error_text(e),
                "error_records": self._pub_err_state["records"],
                "record_failures":
                    self._pub_err_state["record_failures"]})
            return 6
        self.events.append({"event": "SEALED",
                            "verdict": self.verdict["verdict"]})
        return 0

    def _read(self, branch, relpath):
        raw = self.publisher.read(branch, relpath)
        return json.loads(raw) if raw is not None else None
