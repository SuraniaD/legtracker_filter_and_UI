"""
Filter job: applies the old LegTracker ingestion gates 1-3 to the ingestion data
and fills the tracker's category-1 tables.

Reads (public, ingestion DB)     bills, member, bill_action, bill_subject, bill_title,
                                 bill_summary, bill_fetch_log
Writes (tracker schema)          bills, bill_actions, bill_subjects, filter_decisions,
                                 bill_enrichment (a 'pending' row for every new kept bill),
                                 pipeline_runs

Gates, exactly as the old sync.process_bill (rules.py holds the copied code):
  1  latest action contains a dead-bill keyword             -> inactive
  2  policy area in ALDF_POLICY_AREAS                       -> kept (policy_area)
     any subject in tracker.subject_categories (active)     -> kept (subject)
     animal keyword in display/all titles or CRS summary    -> kept (keyword)
     animal-related committee in the latest action          -> kept (committee)
  3  embedding similarity to the 16 anchor phrases >= 20    -> kept (semantic)
     otherwise deferred (no subjects and no summary yet) or no_match
Gate 4 (AI score >= 30) is the enrichment job's.

Only bills that changed are re-evaluated: a bill is looked at again when its
update date moved past the one recorded with its decision, or when the refresh
service fetched new data for it after the decision was made.

Differences from the old code, none of which change a decision:
  - gate 3 is only computed for bills no rule matched (the old code computed it
    for every active bill, but it can only change the outcome for those),
  - an embedding failure leaves the bill undecided (retried next run) instead
    of rejecting it as no_match,
  - a kept bill that later stops matching is removed from the tracker (the old
    code left the stale row in place).

Usage:
  export DATABASE_URL=...  OPENROUTER_API_KEY=...
  python filter_job.py --preflight                # migrate tracker schema, check inputs + key
  python filter_job.py --dry-run                  # rule counts, no writes, no API calls
  python filter_job.py --dry-run --with-semantic  # + gate 3 (embedding calls), still no writes
  python filter_job.py                            # evaluate changed bills and write
  python filter_job.py --reevaluate               # evaluate every bill again
"""

import argparse
import hashlib
import json
import os
import signal
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from sqlalchemy import bindparam, create_engine, text

import rules
from embeddings import Embedder, EmbeddingError

FILTER_LOCK_KEY = 7_311_960_005
REFRESH_LOCK_KEY = 7_311_960_004
HERE = os.path.dirname(os.path.abspath(__file__))


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_congress(today: date | None = None) -> int:
    today = today or date.today()
    return (today.year - 1789) // 2 + 1


def db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("Set DATABASE_URL.")
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        try:
            import psycopg  # noqa: F401
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        except ImportError:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def migrate_tracker_schema():
    """alembic upgrade head for the tracker schema (idempotent)."""
    from alembic import command
    from alembic.config import Config
    cfg = Config(os.path.join(HERE, "tracker_db", "alembic.ini"))
    command.upgrade(cfg, "head")


# ── loading inputs ───────────────────────────────────────────────────────────

@dataclass
class RawBill:
    id: int
    congress: int
    bill_type: str
    bill_number: int
    title: str | None
    origin_chamber: str | None
    introduced_date: date | None
    policy_area: str | None
    latest_action_date: date | None
    latest_action_text: str | None
    source_url: str | None
    sponsor_bioguide_id: str | None
    sponsor_name: str | None
    sponsor_party: str | None
    sponsor_state: str | None
    src: datetime | None
    subjects: list = field(default_factory=list)
    titles: list = field(default_factory=list)         # [(title, title_type)] in API order
    summary_html: str | None = None

    @property
    def source_id(self) -> str:
        return f"{self.congress}-{self.bill_type.upper()}-{self.bill_number}"


def load_inputs(conn, congress: int):
    bills = {}
    for r in conn.execute(text("""
        SELECT b.id, b.congress, b.bill_type, b.bill_number, b.title, b.origin_chamber,
               b.introduced_date, b.policy_area, b.latest_action_date, b.latest_action_text,
               COALESCE(b.legislation_url, b.api_url) AS source_url, b.sponsor_bioguide_id,
               COALESCE(NULLIF(m.full_name, ''),
                        NULLIF(TRIM(COALESCE(m.first_name, '') || ' ' || COALESCE(m.last_name, '')), '')) AS sponsor_name,
               m.party AS sponsor_party, m.state AS sponsor_state,
               GREATEST(b.update_date_including_text, b.update_date) AS src
        FROM bills b LEFT JOIN member m ON m.bioguide_id = b.sponsor_bioguide_id
        WHERE b.congress = :c"""), {"c": congress}):
        bills[(r.bill_type, r.bill_number)] = RawBill(**r._mapping)

    for r in conn.execute(text("SELECT bill_type, bill_number, subject_name FROM bill_subject WHERE congress = :c ORDER BY id"), {"c": congress}):
        if (b := bills.get((r.bill_type, r.bill_number))):
            b.subjects.append(r.subject_name)
    for r in conn.execute(text("SELECT bill_type, bill_number, title, title_type FROM bill_title WHERE congress = :c ORDER BY id"), {"c": congress}):
        if (b := bills.get((r.bill_type, r.bill_number))):
            b.titles.append((r.title, r.title_type))
    # The old code used the LAST summary in the API's list; rows keep API order by id.
    for r in conn.execute(text("""
        SELECT DISTINCT ON (bill_type, bill_number) bill_type, bill_number, text_html
        FROM bill_summary WHERE congress = :c ORDER BY bill_type, bill_number, id DESC"""), {"c": congress}):
        if (b := bills.get((r.bill_type, r.bill_number))):
            b.summary_html = r.text_html

    fetched = {(r.bill_type, r.bill_number): r.last for r in conn.execute(text("""
        SELECT bill_type, bill_number, max(fetched_at) AS last FROM bill_fetch_log
        WHERE congress = :c AND status = 'ok' GROUP BY 1, 2"""), {"c": congress})}
    decisions = {r.raw_bill_id: r for r in conn.execute(text(
        "SELECT raw_bill_id, decision, raw_updated_at, decided_at FROM tracker.filter_decisions"))}
    active_subjects = {r[0] for r in conn.execute(text(
        "SELECT subject_name FROM tracker.subject_categories WHERE active"))}
    return bills, fetched, decisions, active_subjects


def needs_evaluation(b: RawBill, decision, fetched_at, reevaluate: bool) -> bool:
    if reevaluate or decision is None:
        return True
    if b.src is not None and (decision.raw_updated_at is None or decision.raw_updated_at < b.src):
        return True
    return fetched_at is not None and fetched_at > decision.decided_at


# ── deciding ─────────────────────────────────────────────────────────────────

@dataclass
class Verdict:
    bill: RawBill
    decision: str | None = None          # kept / inactive / no_match / deferred; None = undecided
    matched_rule: str | None = None
    prefilter: int | None = None
    hits: dict = field(default_factory=dict)
    official_title: str | None = None
    official_summary: str | None = None
    approved_subjects: list = field(default_factory=list)
    candidate_text: str | None = None


def apply_rules(b: RawBill, active_subjects: set) -> Verdict:
    """Gates 1 and 2. Leaves decision None when gate 3 is needed."""
    v = Verdict(b)
    if not rules.is_action_active(b.latest_action_text):
        v.decision = "inactive"
        return v
    v.official_title = rules.official_title_from_titles(b.titles)
    v.official_summary = rules.official_summary_from_html(b.summary_html)
    v.approved_subjects = [s for s in b.subjects if s in active_subjects]

    curated, matched_subjects = rules.match_bill(b.policy_area, b.subjects, active_subjects)
    keyword_hit, keyword_terms = rules.keyword_match(rules.all_title_text(b.title, b.titles), v.official_summary)
    committee_hit, committees = rules.committee_match(b.latest_action_text)
    v.hits = {"policy_area": b.policy_area in rules.ALDF_POLICY_AREAS, "subject": bool(matched_subjects),
              "keyword": keyword_hit, "committee": committee_hit,
              "keyword_terms": keyword_terms[:10], "committees": committees}
    for rule in ("policy_area", "subject", "keyword", "committee"):   # reported rule = first that hit
        if v.hits[rule]:
            v.decision, v.matched_rule = "kept", rule
            return v
    v.candidate_text = rules.build_candidate_text(b.title, b.policy_area, b.subjects, v.official_summary,
                                                  official_title=v.official_title)
    return v


def apply_semantic(v: Verdict, vec, anchors):
    v.prefilter = rules.prefilter_score(vec, anchors) if v.candidate_text.strip() else 0
    if v.prefilter >= rules.PREFILTER_THRESHOLD:
        v.decision, v.matched_rule = "kept", "semantic"
    else:
        unindexed = not v.bill.subjects and not v.official_summary
        v.decision = "deferred" if unindexed else "no_match"


def content_hash(v: Verdict) -> str:
    """Fingerprint of everything the AI scoring reads; the enrichment job re-runs when it changes."""
    b = v.bill
    payload = {"title": b.title, "official_title": v.official_title, "policy_area": b.policy_area,
               "subjects": sorted(v.approved_subjects), "last_action_text": b.latest_action_text,
               "official_summary": v.official_summary}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


# ── writing ──────────────────────────────────────────────────────────────────

def write_batch(conn, verdicts, now):
    kept = [v for v in verdicts if v.decision == "kept"]
    dropped = [v for v in verdicts if v.decision in ("inactive", "no_match", "deferred")]

    if dropped:  # cascades to actions, subjects, enrichment, watchlists, tags
        conn.execute(text("DELETE FROM tracker.bills WHERE raw_bill_id = ANY(:ids)"),
                     {"ids": [v.bill.id for v in dropped]})

    actions_by_bill = defaultdict(list)
    if kept:
        for r in conn.execute(text("""
            SELECT a.bill_type, a.bill_number, a.action_date, a.action_text, a.action_type,
                   a.action_code, a.source_system
            FROM bill_action a
            JOIN unnest(CAST(:types AS text[]), CAST(:nums AS int[])) AS k(t, n)
              ON a.bill_type = k.t AND a.bill_number = k.n
            WHERE a.congress = :c ORDER BY a.id"""),
                {"types": [v.bill.bill_type for v in kept], "nums": [v.bill.bill_number for v in kept],
                 "c": kept[0].bill.congress}):
            actions_by_bill[(r.bill_type, r.bill_number)].append(r)

    new_bills = 0
    for v in kept:
        b = v.bill
        # same bill re-created upstream with a new id: drop the old tracker row first
        conn.execute(text("DELETE FROM tracker.bills WHERE source_id = :s AND raw_bill_id <> :r"),
                     {"s": b.source_id, "r": b.id})
        row = conn.execute(text("""
            INSERT INTO tracker.bills (raw_bill_id, source_id, congress, bill_type, bill_number, title,
                official_title, introduced_date, origin_chamber, policy_area, last_action_date,
                last_action_text, source_url, official_summary, sponsor_bioguide_id, sponsor_name,
                sponsor_party, sponsor_state, matched_rule, prefilter_score, content_hash,
                raw_updated_at, filtered_at, updated_at)
            VALUES (:raw_bill_id, :source_id, :congress, :bill_type, :bill_number, :title,
                :official_title, :introduced_date, :origin_chamber, :policy_area, :last_action_date,
                :last_action_text, :source_url, :official_summary, :sponsor_bioguide_id, :sponsor_name,
                :sponsor_party, :sponsor_state, :matched_rule, :prefilter_score, :content_hash,
                :raw_updated_at, :now, :now)
            ON CONFLICT (raw_bill_id) DO UPDATE SET
                source_id = EXCLUDED.source_id, title = EXCLUDED.title,
                official_title = EXCLUDED.official_title, introduced_date = EXCLUDED.introduced_date,
                origin_chamber = EXCLUDED.origin_chamber, policy_area = EXCLUDED.policy_area,
                last_action_date = EXCLUDED.last_action_date, last_action_text = EXCLUDED.last_action_text,
                source_url = EXCLUDED.source_url, official_summary = EXCLUDED.official_summary,
                sponsor_bioguide_id = EXCLUDED.sponsor_bioguide_id, sponsor_name = EXCLUDED.sponsor_name,
                sponsor_party = EXCLUDED.sponsor_party, sponsor_state = EXCLUDED.sponsor_state,
                matched_rule = EXCLUDED.matched_rule, prefilter_score = EXCLUDED.prefilter_score,
                content_hash = EXCLUDED.content_hash, raw_updated_at = EXCLUDED.raw_updated_at,
                filtered_at = EXCLUDED.filtered_at, updated_at = EXCLUDED.updated_at
            RETURNING id, (xmax = 0) AS inserted"""),
            {"raw_bill_id": b.id, "source_id": b.source_id, "congress": b.congress,
             "bill_type": b.bill_type.upper(), "bill_number": b.bill_number, "title": b.title,
             "official_title": v.official_title, "introduced_date": b.introduced_date,
             "origin_chamber": b.origin_chamber, "policy_area": b.policy_area,
             "last_action_date": b.latest_action_date, "last_action_text": b.latest_action_text,
             "source_url": b.source_url, "official_summary": v.official_summary,
             "sponsor_bioguide_id": b.sponsor_bioguide_id, "sponsor_name": b.sponsor_name,
             "sponsor_party": b.sponsor_party, "sponsor_state": b.sponsor_state,
             "matched_rule": v.matched_rule, "prefilter_score": v.prefilter,
             "content_hash": content_hash(v), "raw_updated_at": b.src, "now": now}).one()
        tracker_id, inserted = row.id, row.inserted
        new_bills += int(inserted)

        conn.execute(text("DELETE FROM tracker.bill_subjects WHERE bill_id = :id"), {"id": tracker_id})
        if v.approved_subjects:
            conn.execute(text("INSERT INTO tracker.bill_subjects (bill_id, subject_name) VALUES (:id, :s) ON CONFLICT DO NOTHING"),
                         [{"id": tracker_id, "s": s} for s in dict.fromkeys(v.approved_subjects)])
        conn.execute(text("DELETE FROM tracker.bill_actions WHERE bill_id = :id"), {"id": tracker_id})
        acts = actions_by_bill.get((b.bill_type, b.bill_number), [])
        if acts:
            conn.execute(text("""INSERT INTO tracker.bill_actions
                (bill_id, action_date, action_text, action_type, action_code, source_system)
                VALUES (:id, :d, :t, :ty, :co, :ss)"""),
                [{"id": tracker_id, "d": a.action_date, "t": a.action_text, "ty": a.action_type,
                  "co": a.action_code, "ss": a.source_system} for a in acts])
        # New bills queue for AI scoring. Changed bills keep showing their current
        # score; the enrichment job re-scores them when content_hash moves.
        conn.execute(text("INSERT INTO tracker.bill_enrichment (bill_id) VALUES (:id) ON CONFLICT DO NOTHING"),
                     {"id": tracker_id})

    decided = [v for v in verdicts if v.decision]
    if decided:
        conn.execute(text("""
            INSERT INTO tracker.filter_decisions (raw_bill_id, source_id, decision, matched_rule,
                prefilter_score, raw_updated_at, decided_at)
            VALUES (:r, :s, :d, :m, :p, :u, :now)
            ON CONFLICT (raw_bill_id) DO UPDATE SET source_id = EXCLUDED.source_id,
                decision = EXCLUDED.decision, matched_rule = EXCLUDED.matched_rule,
                prefilter_score = EXCLUDED.prefilter_score, raw_updated_at = EXCLUDED.raw_updated_at,
                decided_at = EXCLUDED.decided_at"""),
            [{"r": v.bill.id, "s": v.bill.source_id, "d": v.decision, "m": v.matched_rule,
              "p": v.prefilter, "u": v.bill.src, "now": now} for v in decided])
    return new_bills


# ── run ──────────────────────────────────────────────────────────────────────

class Stop(Exception):
    pass


def _run_gate3(need_sem, semantic, state):
    """Embeds candidate texts for bills no rule matched and applies gate 3.
    Fills state['embedder'] and state['failed'] as it goes."""
    state["failed"] = 0
    if not (need_sem and semantic):
        return
    try:
        state["embedder"] = embedder = Embedder()
        anchors = embedder.embed_many(rules.ANCHOR_PHRASES)
    except EmbeddingError as exc:
        print(f"  Gate 3 unavailable ({exc}); {len(need_sem)} bills left undecided for the next run.")
        return
    for i in range(0, len(need_sem), 256):
        chunk = need_sem[i:i + 256]
        try:
            vecs = embedder.embed_many([v.candidate_text for v in chunk])
            for v, vec in zip(chunk, vecs):
                apply_semantic(v, vec, anchors)
        except EmbeddingError as exc:
            # One bad text fails the whole request: retry one by one so only the
            # bad ones stay undecided. Stop early if the API itself is down.
            print(f"  embedding batch failed ({exc}); retrying its {len(chunk)} texts one by one")
            consecutive = 0
            for j, v in enumerate(chunk):
                try:
                    apply_semantic(v, embedder.embed_many([v.candidate_text])[0], anchors)
                    consecutive = 0
                except EmbeddingError:
                    state["failed"] += 1
                    consecutive += 1
                    if consecutive >= 3 and j < len(chunk) - 1:
                        rest = len(chunk) - j - 1
                        state["failed"] += rest
                        print(f"  3 failures in a row: embeddings look unavailable; {rest} more bills left undecided")
                        break
        print(f"  gate 3: {min(i + 256, len(need_sem))}/{len(need_sem)} bills embedded ({embedder.requests} requests)")


def run(args) -> int:
    congress = args.congress or current_congress()
    engine = create_engine(db_url(), pool_pre_ping=True)
    write = not args.dry_run
    semantic = write or args.with_semantic

    lock_conn = None
    if write:
        lock_conn = engine.connect()
        if not lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": FILTER_LOCK_KEY}).scalar():
            print("Another filter run is already in progress; exiting.")
            return 0
        lock_conn.commit()

        def on_term(signum, frame):
            raise Stop()
        signal.signal(signal.SIGTERM, on_term)

    t0 = time.monotonic()
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('tracker.filter_decisions')")).scalar():
            print("The tracker schema doesn't exist yet. Run `python filter_job.py --preflight` first "
                  "(it creates the schema), or let start.sh do it.")
            if lock_conn:
                lock_conn.close()
            return 1
        bills, fetched, decisions, active_subjects = load_inputs(conn, congress)
    todo = [b for k, b in sorted(bills.items(), key=lambda kv: (kv[1].latest_action_date or date.min), reverse=True)
            if needs_evaluation(b, decisions.get(b.id), fetched.get(k), args.reevaluate)]
    if args.limit:
        todo = todo[:args.limit]
    print(f"Congress {congress}: {len(bills)} bills in `bills`, {len(todo)} to evaluate "
          f"({len(bills) - len(todo) if not args.limit else 'limit applied'} unchanged since their last decision). "
          f"{len(active_subjects)} approved subjects.")

    verdicts = [apply_rules(b, active_subjects) for b in todo]
    need_sem = [v for v in verdicts if v.decision is None]

    run_id = None
    if write:
        with engine.begin() as conn:
            run_id = conn.execute(text("""INSERT INTO tracker.pipeline_runs (run_type, status, bills_seen)
                VALUES ('filter', 'running', :n) RETURNING id"""), {"n": len(todo)}).scalar()

    embedder = None
    sem_failed = 0
    interrupted = False
    try:
        _run_gate3(need_sem, semantic, state := {})
        embedder, sem_failed = state.get("embedder"), state.get("failed", 0)
    except Stop:
        interrupted = True
        embedder, sem_failed = state.get("embedder"), state.get("failed", 0)
        print("\nStopped during gate 3; nothing written. Re-run to continue.")
    # ── report ──
    by_rule = Counter(v.matched_rule for v in verdicts if v.decision == "kept")
    by_decision = Counter(v.decision or "undecided" for v in verdicts)
    hits = Counter(r for v in verdicts for r in ("policy_area", "subject", "keyword", "committee") if v.hits.get(r))
    print("\nDecisions" + (" (dry run, nothing written)" if not write else "") + ":")
    print(f"  inactive (gate 1)          {by_decision['inactive']:>6}")
    for rule in ("policy_area", "subject", "keyword", "committee", "semantic"):
        print(f"  kept by {rule:<18} {by_rule[rule]:>6}")
    print(f"  kept, total                {sum(by_rule.values()):>6}")
    print(f"  no_match                   {by_decision['no_match']:>6}")
    print(f"  deferred (no subjects/summary yet) {by_decision['deferred']:>3}")
    if not semantic:
        print(f"  need gate 3 (not run)      {len(need_sem):>6}  -> ~{-(-len(need_sem) // 64) + 1} embedding requests "
              f"for {len(need_sem)} texts; run with --with-semantic to include")
    elif by_decision["undecided"]:
        print(f"  undecided (embedding failed; retried next run) {by_decision['undecided']}")
    print("Rule hits, overlapping (a bill can hit several):",
          ", ".join(f"{r}={hits[r]}" for r in ("policy_area", "subject", "keyword", "committee")))
    if args.samples:
        for rule in ("policy_area", "subject", "keyword", "committee", "semantic"):
            ex = [v for v in verdicts if v.matched_rule == rule][:args.samples]
            if ex:
                print(f"  e.g. {rule}: " + "; ".join(
                    f"{v.bill.source_id} {(v.official_title or v.bill.title or '')[:70]!r}" for v in ex))

    if not write:
        print(f"\nDone in {time.monotonic() - t0:.0f}s"
              + (f"; {embedder.requests} embedding requests" if embedder else "") + ".")
        return 0

    # ── write ──
    new_total = 0
    status, error = "completed", None
    try:
        if interrupted:
            raise Stop()
        now = utcnow()
        for i in range(0, len(verdicts), args.batch_size):
            with engine.begin() as conn:
                new_total += write_batch(conn, verdicts[i:i + args.batch_size], now)
        if sem_failed or by_decision["undecided"]:
            status, error = "completed", f"{by_decision['undecided']} bills undecided (embedding failures); retried next run"
    except Stop:
        status, error = "interrupted", "stopped by SIGTERM; re-run to continue"
        print("\nStopped; finished batches are saved. Re-run to continue.")
    except Exception as exc:
        status, error = "failed", f"{type(exc).__name__}: {str(exc)[:400]}"
        print(f"\nFailed: {error}")
    with engine.begin() as conn:
        conn.execute(text("""UPDATE tracker.pipeline_runs SET status = :s, bills_kept = :k,
            bills_rejected = :rj, embedding_calls = :e, error = :err, finished_at = now(),
            checkpoint = CAST(:cp AS jsonb) WHERE id = :id"""),
            {"s": status, "k": sum(by_rule.values()),
             "rj": by_decision["inactive"] + by_decision["no_match"] + by_decision["deferred"],
             "e": embedder.requests if embedder else 0, "err": error,
             "cp": json.dumps({"congress": congress, "new_tracker_bills": new_total}), "id": run_id})
        in_tracker = conn.execute(text("SELECT count(*) FROM tracker.bills WHERE congress = :c"), {"c": congress}).scalar()
        pending = conn.execute(text("SELECT count(*) FROM tracker.bill_enrichment WHERE status = 'pending'")).scalar()
    lock_conn.close()
    print(f"\n{status.capitalize()}: {new_total} new bills in the tracker; tracker.bills now holds {in_tracker} "
          f"for congress {congress}; {pending} waiting for AI scoring. ({time.monotonic() - t0:.0f}s)")
    return 0 if status != "failed" else 1


# ── preflight ────────────────────────────────────────────────────────────────

def preflight(args) -> int:
    results = []

    def report(level, msg):
        results.append(level)
        print(f"  [{level}] {msg}")

    congress = args.congress or current_congress()
    print(f"Filter preflight for congress {congress}")
    try:
        engine = create_engine(db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            report("PASS", f"connected to '{conn.execute(text('SELECT current_database()')).scalar()}'")
    except Exception as exc:
        report("FAIL", f"cannot connect: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1

    try:
        migrate_tracker_schema()
        with engine.connect() as conn:
            rev = conn.execute(text("SELECT version_num FROM tracker.alembic_version")).scalar()
            n_subj = conn.execute(text("SELECT count(*) FROM tracker.subject_categories WHERE active")).scalar()
        report("PASS", f"tracker schema at {rev}; {n_subj} approved subjects")
    except Exception as exc:
        report("FAIL", f"tracker schema migration failed: {type(exc).__name__}: {str(exc).splitlines()[0][:300]}")
        return 1

    with engine.connect() as conn:
        missing = [t for t in ("bills", "member", "bill_action", "bill_subject", "bill_title", "bill_summary", "bill_fetch_log")
                   if not conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t}"}).scalar()]
        cols = {r[0] for r in conn.execute(text("""SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'bills'"""))}
    if missing:
        report("FAIL", f"input tables missing: {', '.join(missing)} (run the refresh service first)")
        return 1
    if not {"policy_area", "introduced_date", "legislation_url"} <= cols:
        report("FAIL", "bills lacks policy_area/introduced_date/legislation_url (run the refresh service first)")
        return 1
    with engine.connect() as conn:
        c = {"c": congress}
        n = {t: conn.execute(text(f"SELECT count(DISTINCT (bill_type, bill_number)) FROM {t} WHERE congress = :c"), c).scalar()
             for t in ("bills", "bill_subject", "bill_title", "bill_summary", "bill_action")}
        with_policy = conn.execute(text("SELECT count(*) FROM bills WHERE congress = :c AND policy_area IS NOT NULL"), c).scalar()
        has_idx = conn.execute(text("""SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'
                 AND tablename = 'bill_action' AND indexdef ILIKE '%(congress, bill_type, bill_number)%'""")).scalar()
    if n["bills"] == 0:
        report("FAIL", f"no bills for congress {congress}")
    else:
        report("PASS", f"inputs for congress {congress}: {n['bills']} bills, {with_policy} with policy area, "
                       f"{n['bill_subject']} with subjects, {n['bill_title']} with titles, "
                       f"{n['bill_summary']} with a summary, {n['bill_action']} with actions")
    report("PASS" if has_idx else "WARN",
           "bill_action has an index on (congress, bill_type, bill_number)" if has_idx else
           "bill_action has no index on (congress, bill_type, bill_number): copying actions will be slow "
           "(create it with: python refresh.py --preflight --with-action-index)")

    with engine.connect() as conn:
        free = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": FILTER_LOCK_KEY}).scalar()
        if free:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": FILTER_LOCK_KEY})
        refresh_busy = not conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": REFRESH_LOCK_KEY}).scalar()
        if not refresh_busy:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": REFRESH_LOCK_KEY})
    report("PASS" if free else "WARN", "no other filter run active" if free else "another filter run is active")
    if refresh_busy:
        report("WARN", "the refresh service is running now; bills it updates afterwards are re-evaluated on the next filter run")

    try:
        vecs = Embedder().embed_many(rules.ANCHOR_PHRASES[:2])
        report("PASS", f"OpenRouter embeddings work ({len(vecs[0])} dims)")
    except EmbeddingError as exc:
        report("FAIL" if not args.dry_run else "WARN", f"OpenRouter embeddings: {exc}")

    failed = results.count("FAIL")
    print(f"Preflight: {results.count('PASS')} passed, {results.count('WARN')} warnings, {failed} failed")
    return 1 if failed else 0


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--congress", type=int, default=int(env("BACKFILL_CONGRESS")) if env("BACKFILL_CONGRESS") else None)
    p.add_argument("--dry-run", action="store_true", help="report decisions, write nothing")
    p.add_argument("--with-semantic", action="store_true", help="with --dry-run: also run gate 3 (embedding calls)")
    p.add_argument("--reevaluate", action="store_true", help="evaluate every bill, not only changed ones")
    p.add_argument("--limit", type=int, help="evaluate at most N bills (testing)")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--samples", type=int, default=3, help="example bills printed per rule (0 = none)")
    p.add_argument("--preflight", action="store_true")
    args = p.parse_args()
    sys.exit(preflight(args) if args.preflight else run(args))


if __name__ == "__main__":
    main()
