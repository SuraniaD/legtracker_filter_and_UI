"""
Daily refresh of the ingestion database for ONE congress (default: the current one).
Feeds both the tracker and the prediction model.

Step 1, list   GET /bill/{congress} (all pages, ~80 requests)
               upsert `bills`; one `raw_ingestion_response` row per NEW or CHANGED bill only
Step 2, bills  for every bill whose update date moved since each endpoint was last fetched:
  detail       /bill/{c}/{t}/{n}              bills.policy_area, introduced_date, legislation_url,
                                              bills.sponsor_bioguide_id + `member` upsert
  actions      .../actions                    bill_action        (delete + insert per bill)
  cosponsors   .../cosponsors                 bill_cosponsor     (upsert on bill + bioguide_id)
  text         .../text                       bill_text_version  (upsert on bill + version + format)
  subjects     .../subjects                   bill_subject       (delete + insert per bill)
  summaries    .../summaries                  bill_summary       (delete + insert per bill)
  titles       .../titles                     bill_title         (delete + insert per bill)

Rows for the ingestion repo's tables are produced by transforms.py, which mirrors
that repo's src/transform.py, and written to the columns read from the database
itself, so the prediction model's feature code sees the same data as before.

Change detection: bill_fetch_log keeps, per (bill, endpoint), the bill's update
date at the time of the fetch. A bill is re-fetched when GREATEST(update_date,
update_date_including_text) in `bills` is later. Zero-count sub-resources (per the
detail response) are skipped without a call. HTTP 404 is recorded and not retried.

Safe to stop at any time (Ctrl+C / SIGTERM); the next run resumes. Only one copy
runs per database (Postgres advisory lock). Rate-limited to --max-per-hour.

Usage:
  export DATABASE_URL=...  CONGRESS_API_KEY=...
  python refresh.py --preflight      # create gap tables, check DB/API/permissions
  python refresh.py --dry-run        # worklist from current `bills`, no API calls
  python refresh.py                  # list step + refresh of changed bills
  python refresh.py --skip-list      # only the per-bill step
  python refresh.py --list-only      # only the list step

Environment overrides (handy on Railway): BACKFILL_CONGRESS, BACKFILL_ENDPOINTS,
BACKFILL_MAX_PER_HOUR, BACKFILL_CONCURRENCY, BACKFILL_SKIP_LIST=1.
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import httpx
from sqlalchemy import MetaData, and_, create_engine, delete, func, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

import transforms as tf
from gap_models import (BILL_NEW_COLUMNS, NEW_TABLES, BillFetchLog, BillSubject,
                        BillSummary, BillTitle)

API_BASE = "https://api.congress.gov/v3"
PAGE_SIZE = 250
MAX_RETRIES = 5
ENDPOINTS = ("detail", "actions", "cosponsors", "text", "subjects", "summaries", "titles")
RUN_SOURCE = "congress_gov_daily_refresh"
LOCK_KEY = 7_311_960_004  # same id as the earlier gap backfill: the two never overlap

# Ingestion-repo tables this job writes to, and the unique keys its upserts rely on.
REPO_TABLES = ("bills", "raw_ingestion_response", "bill_action", "bill_cosponsor",
               "bill_text_version", "member")
UPSERT_KEYS = {
    "bills": ("congress", "bill_type", "bill_number"),
    "bill_cosponsor": ("congress", "bill_type", "bill_number", "bioguide_id"),
    "bill_text_version": ("congress", "bill_type", "bill_number", "version_type", "format_type"),
    "member": ("bioguide_id",),
}

# endpoint -> (URL suffix, list extractor, key of its count in the detail response)
SUB_RESOURCES = {
    "actions":    ("actions",    lambda d: d.get("actions") or [], "actions"),
    "cosponsors": ("cosponsors", lambda d: d.get("cosponsors") or [], "cosponsors"),
    "text":       ("text",       lambda d: d.get("textVersions") or [], "textVersions"),
    "subjects":   ("subjects",   lambda d: (d.get("subjects") or {}).get("legislativeSubjects") or [], "subjects"),
    "summaries":  ("summaries",  lambda d: d.get("summaries") or [], "summaries"),
    "titles":     ("titles",     lambda d: d.get("titles") or [], "titles"),
}
# Missing from the detail response means "none exist" for these; titles/actions always exist.
ABSENT_MEANS_ZERO = {"cosponsors", "text", "subjects", "summaries"}


# ── small helpers ────────────────────────────────────────────────────────────

def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_congress(today: date | None = None) -> int:
    today = today or date.today()
    return (today.year - 1789) // 2 + 1


def _db_url() -> str:
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


def _api_key() -> str:
    key = os.environ.get("CONGRESS_API_KEY")
    if not key:
        sys.exit("Set CONGRESS_API_KEY.")
    return key


def make_client() -> httpx.AsyncClient:
    """Factory so tests can swap in a mock transport."""
    return httpx.AsyncClient()


# ── HTTP: rate limit + retry ─────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, max_per_hour: int):
        self.interval = 3600.0 / max_per_hour
        self._next = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval


@dataclass
class Counters:
    requests: int = 0
    skipped_calls: int = 0


class ApiError(Exception):
    pass


class NotFound(ApiError):
    pass


async def get_json(client, limiter, counters, api_key, path, params=None) -> dict:
    url = f"{API_BASE}/{path}"
    query = {"api_key": api_key, "format": "json", **(params or {})}
    for attempt in range(1, MAX_RETRIES + 1):
        await limiter.wait()
        counters.requests += 1
        try:
            response = await client.get(url, params=query, timeout=30.0)
        except httpx.TransportError as exc:
            if attempt == MAX_RETRIES:
                raise ApiError(f"network error on /{path}: {type(exc).__name__}") from None
            await asyncio.sleep(2 ** attempt)
            continue
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise ApiError(f"HTTP {response.status_code} on /{path} after {MAX_RETRIES} attempts")
            retry_after = response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
            print(f"    HTTP {response.status_code} on /{path}; retrying in {wait:.0f}s")
            await asyncio.sleep(wait)
            continue
        if response.status_code == 404:
            raise NotFound(f"HTTP 404 on /{path}")
        if response.status_code >= 400:
            raise ApiError(f"HTTP {response.status_code} on /{path}")
        return response.json()
    raise ApiError(f"gave up on /{path}")


async def get_all_pages(client, limiter, counters, api_key, path, extract, max_pages=None) -> list:
    items, offset, pages = [], 0, 0
    while True:
        data = await get_json(client, limiter, counters, api_key, path,
                              {"limit": PAGE_SIZE, "offset": offset})
        page = extract(data)
        items.extend(page)
        pages += 1
        if not (data.get("pagination") or {}).get("next") or not page:
            return items
        if max_pages and pages >= max_pages:
            return items
        offset += PAGE_SIZE


# ── database setup ───────────────────────────────────────────────────────────

def ensure_schema(engine):
    with engine.begin() as conn:
        if not conn.execute(text("SELECT to_regclass('public.bills')")).scalar():
            sys.exit("Table `bills` not found. Point DATABASE_URL at the ingestion database.")
        for column, sql_type in BILL_NEW_COLUMNS.items():
            conn.execute(text(f"ALTER TABLE bills ADD COLUMN IF NOT EXISTS {column} {sql_type}"))
    for table in NEW_TABLES:
        table.create(engine, checkfirst=True)


def reflect_repo_tables(engine) -> dict:
    md = MetaData()
    md.reflect(engine, only=list(REPO_TABLES))
    return {name: md.tables[name] for name in REPO_TABLES}


def unique_keys(conn, table: str) -> list:
    rows = conn.execute(text("""
        SELECT array_agg(a.attname::text ORDER BY a.attname)
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
        WHERE i.indrelid = to_regclass(:t) AND (i.indisunique OR i.indisprimary)
        GROUP BY i.indexrelid
    """), {"t": f"public.{table}"}).all()
    return [tuple(r[0]) for r in rows]


def acquire_run_lock(engine):
    conn = engine.connect()
    if not conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar():
        conn.close()
        return None
    conn.commit()
    return conn


# ── run logging (ingestion_runs / run_steps) ─────────────────────────────────

def start_run(engine):
    try:
        with engine.begin() as conn:
            return conn.execute(text("""
                INSERT INTO ingestion_runs (source, status, started_at, records_fetched, records_written)
                VALUES (:s, 'running', :t, 0, 0) RETURNING id"""), {"s": RUN_SOURCE, "t": utcnow()}).scalar()
    except Exception as exc:
        print(f"  (ingestion_runs not written: {type(exc).__name__})")
        return None


def log_step(engine, run_id, step, status, started, detail, error=None):
    if run_id is None:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO run_steps (run_id, step_name, status, started_at, completed_at, detail, error_message)
            VALUES (:r, :n, :s, :st, :ct, CAST(:d AS json), :e)"""),
            {"r": run_id, "n": step, "s": status, "st": started, "ct": utcnow(),
             "d": json.dumps(detail, default=str), "e": error})


def finish_run(engine, run_id, status, fetched, written, error=None, watermark=None):
    if run_id is None:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ingestion_runs SET status = :s, completed_at = :t, records_fetched = :f,
                   records_written = :w, error_message = :e, watermark = COALESCE(:wm, watermark)
            WHERE id = :id"""), {"s": status, "t": utcnow(), "f": fetched, "w": written,
                                 "e": error, "wm": watermark, "id": run_id})


# ── step 1: list ─────────────────────────────────────────────────────────────

async def list_step(engine, tables, congress, client, limiter, counters, api_key, max_pages=None) -> dict:
    """Upserts every bill of the congress; raw rows only for new/changed bills."""
    raws = await get_all_pages(client, limiter, counters, api_key, f"bill/{congress}",
                               lambda d: d.get("bills") or [], max_pages=max_pages)
    bills_t, raw_t = tables["bills"], tables["raw_ingestion_response"]
    has_intro = "introduced_date" in bills_t.c

    with engine.connect() as conn:
        existing = {(r.bill_type, r.bill_number): (r.update_date, r.update_date_including_text)
                    for r in conn.execute(text("""SELECT bill_type, bill_number, update_date,
                                                  update_date_including_text FROM bills WHERE congress = :c"""),
                                          {"c": congress})}

    rows, raw_by_key, errors, seen = [], {}, [], set()
    for raw in raws:
        try:
            row = tf.transform_bill(raw)
        except (KeyError, ValueError, TypeError) as exc:
            errors.append({"bill": raw.get("url", "unknown"), "error": str(exc)})
            continue
        key = (row["bill_type"], row["bill_number"])
        if key in seen:          # the API can repeat a bill across pages while it updates
            continue
        seen.add(key)
        if has_intro:  # same keys on every row (multi-row insert)
            row["introduced_date"] = tf.parse_date(raw.get("introducedDate"))
        rows.append(row)
        raw_by_key[key] = raw

    now = utcnow()
    new = changed = 0
    update_cols = ["title", "origin_chamber", "latest_action_date", "latest_action_text",
                   "update_date", "update_date_including_text", "api_url"]
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            chunk = [{**r, "created_at": now, "updated_at": now} for r in rows[i:i + 500]]
            stmt = pg_insert(bills_t).values(chunk)
            set_ = {**{c: stmt.excluded[c] for c in update_cols}, "updated_at": now}
            if has_intro:  # never blank out a date the detail step already stored
                set_["introduced_date"] = func.coalesce(stmt.excluded.introduced_date, bills_t.c.introduced_date)
            stmt = stmt.on_conflict_do_update(
                index_elements=list(UPSERT_KEYS["bills"]), set_=set_,
            ).returning(bills_t.c.id, bills_t.c.bill_type, bills_t.c.bill_number)
            returned = conn.execute(stmt).all()
            by_key = {(r["bill_type"], r["bill_number"]): r for r in chunk}
            raw_rows = []
            for bill_id, btype, bnum in returned:
                key = (btype, bnum)
                row = by_key[key]
                before = existing.get(key)
                if before is None:
                    new += 1
                elif before != (row["update_date"], row["update_date_including_text"]):
                    changed += 1
                else:
                    continue   # unchanged: no raw history row
                raw_rows.append({"bill_id": bill_id, "api_url": raw_by_key[key].get("url"),
                                 "raw_json": raw_by_key[key], "date_created": now})
            if raw_rows:
                conn.execute(insert(raw_t), raw_rows)
    return {"fetched": len(raws), "upserted": len(rows), "new": new, "changed": changed,
            "unchanged": len(rows) - new - changed, "errors": len(errors), "error_samples": errors[:5]}


# ── step 2: per-bill refresh ─────────────────────────────────────────────────

@dataclass
class BillWork:
    congress: int
    bill_type: str
    bill_number: int
    source_update_date: datetime | None
    endpoints: list
    detail_raw: dict | None = None
    results: dict = field(default_factory=dict)
    errors: dict = field(default_factory=dict)
    not_found: set = field(default_factory=set)

    @property
    def path(self) -> str:
        return f"bill/{self.congress}/{self.bill_type.lower()}/{self.bill_number}"

    @property
    def label(self) -> str:
        return f"{self.congress}-{self.bill_type}-{self.bill_number}"


def build_worklist(engine, congress, endpoints, refresh, limit):
    with engine.connect() as conn:
        bills = conn.execute(text("""
            SELECT congress, bill_type, bill_number,
                   GREATEST(update_date_including_text, update_date) AS src
            FROM bills WHERE congress = :c
            ORDER BY latest_action_date DESC NULLS LAST, bill_type, bill_number
        """), {"c": congress}).all()
        logs = {}
        if not refresh:
            for row in conn.execute(text("""
                SELECT bill_type, bill_number, endpoint, status, source_update_date
                FROM bill_fetch_log WHERE congress = :c"""), {"c": congress}):
                logs[(row.bill_type, row.bill_number, row.endpoint)] = row
    work = []
    for b in bills:
        needed = []
        for endpoint in endpoints:
            log = logs.get((b.bill_type, b.bill_number, endpoint))
            if (log is None or log.status == "error"
                    or (b.src is not None and (log.source_update_date is None or log.source_update_date < b.src))):
                needed.append(endpoint)
        if needed:
            # detail first: its counts let zero-item endpoints be skipped
            needed.sort(key=lambda e: ENDPOINTS.index(e))
            work.append(BillWork(b.congress, b.bill_type, b.bill_number, b.src, needed))
        if limit and len(work) >= limit:
            break
    return work, len(bills)


async def fetch_bill(w: BillWork, client, limiter, counters, api_key, sem):
    async with sem:
        counts = {}
        if "detail" in w.endpoints:
            try:
                w.detail_raw = await get_json(client, limiter, counters, api_key, w.path)
                bill = w.detail_raw.get("bill") or {}
                for ep, (_, _, count_key) in SUB_RESOURCES.items():
                    info = bill.get(count_key)
                    if isinstance(info, dict):
                        counts[ep] = info.get("countIncludingWithdrawnCosponsors", info.get("count"))
                    elif ep in ABSENT_MEANS_ZERO:
                        counts[ep] = 0
            except NotFound:
                w.not_found.update(w.endpoints)  # no bill => no sub-resources either
                return w
            except ApiError as exc:
                w.errors["detail"] = str(exc)
        for ep in w.endpoints:
            if ep == "detail":
                continue
            if counts.get(ep) == 0:
                w.results[ep] = []
                counters.skipped_calls += 1
                continue
            suffix, extract, _ = SUB_RESOURCES[ep]
            try:
                w.results[ep] = await get_all_pages(client, limiter, counters, api_key,
                                                    f"{w.path}/{suffix}", extract)
            except NotFound:
                w.not_found.add(ep)
            except ApiError as exc:
                w.errors[ep] = str(exc)
    return w


def write_bill(conn, tables, w: BillWork) -> int:
    """Writes one bill's fetched endpoints in the caller's transaction."""
    c, t, n = w.congress, w.bill_type, w.bill_number
    ident = {"congress": c, "bill_type": t, "bill_number": n}
    now = utcnow()
    written = 0

    def same_bill(table):
        return and_(table.c.congress == c, table.c.bill_type == t, table.c.bill_number == n)

    def replace(table, rows):
        conn.execute(delete(table).where(same_bill(table)))
        if rows:
            conn.execute(insert(table), [{**ident, **r} for r in rows])
        return len(rows)

    # detail -> bills columns + sponsor/member
    if w.detail_raw is not None:
        values = tf.transform_detail_fields(w.detail_raw)
        member = None
        try:
            member = tf.transform_sponsor(w.detail_raw)
        except (KeyError, TypeError):
            w.errors.setdefault("detail", "sponsor without bioguideId")
        if member:
            m = tables["member"]
            stmt = pg_insert(m).values(**member)
            conn.execute(stmt.on_conflict_do_update(
                index_elements=["bioguide_id"],
                set_={k: stmt.excluded[k] for k in member if k != "bioguide_id"}))
            values["sponsor_bioguide_id"] = member["bioguide_id"]
        b = tables["bills"]
        conn.execute(b.update().where(same_bill(b)).values(**values, updated_at=now))
        written += 1

    if "actions" in w.results:
        rows, errs = tf.transform_many(tf.transform_action, w.results["actions"], c, t, n)
        conn.execute(delete(tables["bill_action"]).where(same_bill(tables["bill_action"])))
        if rows:
            conn.execute(insert(tables["bill_action"]), rows)
        written += len(rows)
        if errs:
            w.errors.setdefault("actions", f"{len(errs)} malformed actions skipped")

    if "cosponsors" in w.results:
        rows, errs = tf.transform_many(tf.transform_cosponsor, w.results["cosponsors"], c, t, n)
        rows = list({r["bioguide_id"]: r for r in rows}.values())  # one row per person per statement
        if rows:
            cs = tables["bill_cosponsor"]
            stmt = pg_insert(cs).values(rows)
            conn.execute(stmt.on_conflict_do_update(
                index_elements=list(UPSERT_KEYS["bill_cosponsor"]),
                set_={k: stmt.excluded[k] for k in rows[0] if k not in UPSERT_KEYS["bill_cosponsor"]}))
        written += len(rows)
        if errs:
            w.errors.setdefault("cosponsors", f"{len(errs)} cosponsors without bioguideId skipped")

    if "text" in w.results:
        rows, errs = tf.transform_text_versions(w.results["text"], c, t, n)
        tv = tables["bill_text_version"]
        # NULLs never collide in a unique constraint, so rows without a version
        # type would duplicate on every refresh: replace those instead.
        conn.execute(delete(tv).where(same_bill(tv), tv.c.version_type.is_(None)))
        keyed = [r for r in rows if r["version_type"] is not None]
        unkeyed = [r for r in rows if r["version_type"] is None]
        if keyed:
            # one row per key, in case the API repeats a version/format pair
            keyed = list({(r["version_type"], r["format_type"]): r for r in keyed}.values())
            stmt = pg_insert(tv).values(keyed)
            conn.execute(stmt.on_conflict_do_update(
                index_elements=list(UPSERT_KEYS["bill_text_version"]),
                set_={k: stmt.excluded[k] for k in ("version_date", "text_url", "raw_json")}))
        if unkeyed:
            conn.execute(insert(tv), unkeyed)
        written += len(rows)
        if errs:
            w.errors.setdefault("text", f"{len(errs)} malformed text versions skipped")

    if "subjects" in w.results:
        written += replace(BillSubject.__table__, tf.transform_subjects(w.results["subjects"]))
    if "summaries" in w.results:
        written += replace(BillSummary.__table__, tf.transform_summaries(w.results["summaries"]))
    if "titles" in w.results:
        written += replace(BillTitle.__table__, tf.transform_titles(w.results["titles"]))

    # fetch log (a soft error like "1 malformed item skipped" still counts as fetched)
    for ep in w.endpoints:
        if ep in w.not_found:
            status, msg = "not_found", "not found at Congress.gov"
        elif ep == "detail":
            status = "ok" if w.detail_raw is not None else "error"
            msg = None if status == "ok" else w.errors.get("detail", "not fetched")
        else:
            status = "ok" if ep in w.results else "error"
            msg = w.errors.get(ep) if status == "error" else None
        count = (1 if ep == "detail" else len(w.results.get(ep, []))) if status == "ok" else None
        stmt = pg_insert(BillFetchLog).values(
            **ident, endpoint=ep, status=status, item_count=count,
            source_update_date=w.source_update_date, fetched_at=now,
            error_message=(msg or None) and msg[:500])
        conn.execute(stmt.on_conflict_do_update(
            index_elements=["congress", "bill_type", "bill_number", "endpoint"],
            set_={k: stmt.excluded[k] for k in
                  ("status", "item_count", "source_update_date", "fetched_at", "error_message")}))
    return written


# ── preflight ────────────────────────────────────────────────────────────────

def preflight(args) -> int:
    results = []

    def report(level, msg):
        results.append(level)
        print(f"  [{level}] {msg}")

    congress = args.congress or current_congress()
    print(f"Preflight for congress {congress}")
    try:
        engine = create_engine(_db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            version = conn.execute(text("SHOW server_version")).scalar()
            db = conn.execute(text("SELECT current_database()")).scalar()
        report("PASS", f"connected to database '{db}' (PostgreSQL {version})")
    except SystemExit:
        raise
    except Exception as exc:
        report("FAIL", f"cannot connect: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1

    # ingestion-repo tables and the unique keys the upserts need
    with engine.connect() as conn:
        missing = [t for t in REPO_TABLES
                   if not conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t}"}).scalar()]
        if missing:
            report("FAIL", f"ingestion tables missing: {', '.join(missing)}")
            return 1
        report("PASS", "ingestion tables present: " + ", ".join(REPO_TABLES))
        bad = [t for t, cols in UPSERT_KEYS.items() if tuple(sorted(cols)) not in unique_keys(conn, t)]
    if bad:
        report("FAIL", f"unique key needed for upserts not found on: {', '.join(bad)}")
    else:
        report("PASS", "unique keys for upserts present (bills, bill_cosponsor, bill_text_version, member)")

    try:
        with engine.connect() as conn:
            before = {t.name: conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t.name}"}).scalar()
                      for t in NEW_TABLES}
        ensure_schema(engine)
        created = [n for n, e in before.items() if not e]
        report("PASS", "gap tables ready" + (f" (created: {', '.join(created)})" if created else " (already existed)"))
    except SystemExit as exc:
        report("FAIL", str(exc))
        return 1
    except Exception as exc:
        report("FAIL", f"could not create tables/columns: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1

    try:
        reflect_repo_tables(engine)
        report("PASS", "table columns read from the database")
    except Exception as exc:
        report("FAIL", f"could not read table definitions: {type(exc).__name__}")

    with engine.connect() as conn:
        n_bills = conn.execute(text("SELECT count(*) FROM bills WHERE congress = :c"), {"c": congress}).scalar()
        logged = dict(conn.execute(text("""SELECT endpoint, count(*) FILTER (WHERE status = 'ok')
            FROM bill_fetch_log WHERE congress = :c GROUP BY 1"""), {"c": congress}).all())
    report("PASS", f"{n_bills} bills for congress {congress} in `bills`"
           + (f"; fetched so far: {logged}" if logged else "; nothing fetched yet"))
    if n_bills == 0 and getattr(args, "skip_list", False):
        report("FAIL", "no bills and --skip-list set: nothing to do")

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute(text("""INSERT INTO bill_fetch_log (congress, bill_type, bill_number, endpoint, status, fetched_at)
                                 VALUES (0, 'PREFLIGHT', 0, 'detail', 'ok', now())"""))
            conn.execute(text("UPDATE bills SET updated_at = updated_at WHERE false"))
            trans.rollback()
        report("PASS", "can write (test changes rolled back)")
    except Exception as exc:
        report("FAIL", f"cannot write: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")

    with engine.connect() as conn:
        have_logs = all(conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t}"}).scalar()
                        for t in ("ingestion_runs", "run_steps"))
        others = []
        if have_logs:
            others = conn.execute(text("""SELECT source, started_at FROM ingestion_runs
                WHERE status = 'running' AND source <> :s AND started_at > :since"""),
                {"s": RUN_SOURCE, "since": utcnow() - timedelta(hours=12)}).all()
    report("PASS" if have_logs else "WARN",
           "ingestion_runs / run_steps found" if have_logs else "ingestion_runs / run_steps missing; runs won't be logged")
    for src, started in others:
        report("WARN", f"another ingestion job looks active: source='{src}' since {started:%Y-%m-%d %H:%M} UTC. "
                       "Don't run the weekly ingestion at the same time as this job")

    if args.with_action_index:
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_bill_action_identity "
                                  "ON bill_action (congress, bill_type, bill_number)"))
            report("PASS", "index ix_bill_action_identity on bill_action is in place")
        except Exception as exc:
            report("WARN", f"could not create bill_action index: {type(exc).__name__}")

    lock = acquire_run_lock(engine)
    if lock is None:
        report("WARN", "another refresh is running right now (advisory lock held)")
    else:
        lock.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
        lock.close()
        report("PASS", "no other refresh running")

    try:
        r = httpx.get(f"{API_BASE}/bill/{congress}",
                      params={"api_key": _api_key(), "format": "json", "limit": 1}, timeout=30.0)
        if r.status_code == 200:
            remaining = r.headers.get("X-RateLimit-Remaining")
            api_count = (r.json().get("pagination") or {}).get("count")
            report("PASS", f"Congress.gov key works; API lists {api_count} bills for congress {congress}"
                   + (f"; {remaining} requests left this hour" if remaining else ""))
        elif r.status_code in (401, 403):
            report("FAIL", f"Congress.gov rejected the API key (HTTP {r.status_code})")
        else:
            report("FAIL", f"Congress.gov returned HTTP {r.status_code}")
    except SystemExit as exc:
        report("FAIL", str(exc))
    except httpx.HTTPError as exc:
        report("FAIL", f"cannot reach api.congress.gov: {type(exc).__name__}")

    failed = results.count("FAIL")
    print(f"Preflight: {results.count('PASS')} passed, {results.count('WARN')} warnings, {failed} failed")
    return 1 if failed else 0


# ── main ─────────────────────────────────────────────────────────────────────

async def run(args):
    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    bad = set(endpoints) - set(ENDPOINTS)
    if bad:
        sys.exit(f"Unknown endpoint(s): {', '.join(sorted(bad))}. Valid: {', '.join(ENDPOINTS)}")
    congress = args.congress or current_congress()
    engine = create_engine(_db_url(), pool_pre_ping=True)
    ensure_schema(engine)

    if args.dry_run:
        work, total = build_worklist(engine, congress, endpoints, args.refresh, args.limit)
        fetches = sum(len(w.endpoints) for w in work)
        print(f"Congress {congress}: {total} bills in `bills`, {len(work)} need work "
              f"({fetches} endpoint fetches at most; the list step would run first unless --skip-list).")
        print(f"Upper bound at {args.max_per_hour}/hour: {fetches / args.max_per_hour:.1f} h.")
        return

    lock_conn = acquire_run_lock(engine)
    if lock_conn is None:
        print("Another refresh is already running against this database; exiting.")
        return
    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except (NotImplementedError, RuntimeError):
            pass

    tables = reflect_repo_tables(engine)
    api_key = _api_key()
    limiter = RateLimiter(args.max_per_hour)
    counters = Counters()
    sem = asyncio.Semaphore(args.concurrency)
    run_id = start_run(engine)
    written = failed = not_found = done = 0
    t0 = time.monotonic()

    try:
        async with make_client() as client:
            if not args.skip_list:
                started = utcnow()
                print(f"Step 1: list of congress {congress}")
                summary = await list_step(engine, tables, congress, client, limiter, counters, api_key,
                                          max_pages=args.list_max_pages)
                log_step(engine, run_id, "list", "success" if not summary["errors"] else "failed", started, summary)
                print(f"  {summary['fetched']} bills listed: {summary['new']} new, {summary['changed']} changed, "
                      f"{summary['unchanged']} unchanged, {summary['errors']} malformed "
                      f"({counters.requests} requests)")
            if args.list_only:
                finish_run(engine, run_id, "success", counters.requests, 0, watermark=utcnow())
                lock_conn.close()
                return

            work, total = build_worklist(engine, congress, endpoints, args.refresh, args.limit)
            fetches = sum(len(w.endpoints) for w in work)
            print(f"Step 2: {len(work)} of {total} bills need work ({fetches} endpoint fetches at most; "
                  f"upper bound {fetches / args.max_per_hour:.1f} h at {args.max_per_hour}/hour)")
            for i in range(0, len(work), args.batch_size):
                batch = work[i:i + args.batch_size]
                started = utcnow()
                await asyncio.gather(*(fetch_bill(w, client, limiter, counters, api_key, sem) for w in batch))
                batch_written, batch_errors = 0, []
                for w in batch:
                    with engine.begin() as conn:
                        batch_written += write_bill(conn, tables, w)
                    batch_errors += [{"bill": w.label, "endpoint": e, "error": m} for e, m in w.errors.items()]
                    not_found += len(w.not_found)
                done += len(batch)
                written += batch_written
                failed += len(batch_errors)
                log_step(engine, run_id, "refresh", "success" if not batch_errors else "failed", started,
                         {"bills": len(batch), "rows_written": batch_written, "requests_so_far": counters.requests,
                          "errors": len(batch_errors), "error_samples": batch_errors[:5]})
                rate = done / (time.monotonic() - t0)
                eta = (len(work) - done) / rate / 3600 if rate else 0
                print(f"  {done}/{len(work)} bills | {counters.requests} requests "
                      f"({counters.skipped_calls} skipped as empty) | {failed} errors | "
                      f"{not_found} not found | ETA {eta:.1f} h")
    except (KeyboardInterrupt, asyncio.CancelledError):
        finish_run(engine, run_id, "failed", counters.requests, written, "interrupted; re-run to resume")
        print("\nStopped. Progress is saved; run the same command again to continue.", flush=True)
        lock_conn.close()
        return

    finish_run(engine, run_id, "success" if failed == 0 else "failed", counters.requests, written,
               None if failed == 0 else f"{failed} fetches had errors; re-run retries them", watermark=utcnow())
    lock_conn.close()
    print(f"Done: {done} bills refreshed, {counters.requests} requests, {failed} errors, {not_found} not found.")


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--congress", type=int, default=int(env("BACKFILL_CONGRESS")) if env("BACKFILL_CONGRESS") else None)
    p.add_argument("--endpoints", default=env("BACKFILL_ENDPOINTS") or ",".join(ENDPOINTS),
                   help=f"comma-separated subset of: {','.join(ENDPOINTS)}")
    p.add_argument("--limit", type=int, help="refresh at most N bills (testing)")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--concurrency", type=int, default=int(env("BACKFILL_CONCURRENCY") or 4))
    p.add_argument("--max-per-hour", type=int, default=int(env("BACKFILL_MAX_PER_HOUR") or 4800))
    p.add_argument("--refresh", action="store_true", help="ignore bill_fetch_log; re-fetch every bill")
    p.add_argument("--skip-list", action="store_true", default=env("BACKFILL_SKIP_LIST") == "1",
                   help="skip step 1 (env BACKFILL_SKIP_LIST=1)")
    p.add_argument("--list-only", action="store_true", help="run only step 1")
    p.add_argument("--list-max-pages", type=int, help="cap list pages (testing)")
    p.add_argument("--dry-run", action="store_true", help="worklist from current `bills`, no API calls")
    p.add_argument("--preflight", action="store_true", help="create gap tables, check everything, exit")
    p.add_argument("--with-action-index", action="store_true",
                   help="with --preflight: create the index on bill_action (congress, bill_type, bill_number)")
    args = p.parse_args()
    if args.preflight:
        sys.exit(preflight(args))
    try:
        asyncio.run(run(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
