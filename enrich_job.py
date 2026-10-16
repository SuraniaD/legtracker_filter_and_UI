"""
Enrichment job: gate 4 of the old LegTracker ingestion.

For every bill the filter kept, it asks the scoring LLM (via OpenRouter) for a
relevance score, topics, rationale and plain-language summary, exactly as the
old process_bill_ai did (ai_rules.py holds the copied prompt, parser and
normaliser). Score >= 30 -> 'kept' (shown on the site) and embedded for search;
below 30 -> 'low_score' (stored, not shown).

Queue (tracker.bill_enrichment joined to tracker.bills), in this order:
  1. bills kept by a filter rule (policy area, subject, keyword, committee)
  2. bills kept by the semantic gate, highest prefilter score first
Within the queue:
  - 'pending' and 'failed' rows (failed ones until --max-attempts),
  - scored rows whose bill changed since (ai_source_hash != bills.content_hash),
  - kept rows still missing an up-to-date embedding.

Differences from the old code (fixes, not changes in scoring):
  - a failed AI call never deletes or hides a bill: a new bill becomes 'failed'
    and is retried; a bill being re-scored keeps its current score,
  - 429/5xx/timeouts are retried before moving to a fallback model,
  - embeddings are refreshed when the bill's text changes (the old code never did),
  - token usage and OpenRouter's reported cost are tracked per run.

Usage:
  export DATABASE_URL=...  OPENROUTER_API_KEY=...
  python enrich_job.py --preflight            # checks + one tiny scoring call
  python enrich_job.py --dry-run              # queue size and cost estimate, no calls
  python enrich_job.py --limit 20             # score 20 bills (test)
  python enrich_job.py                        # score the whole queue
  python enrich_job.py --only rules           # only bills kept by rules (or: semantic)

Environment: SCANNING_LLM_MODEL (default google/gemini-2.5-flash),
FALLBACK_LLM_MODELS (comma-separated), ENRICH_CONCURRENCY (default 8),
PRICE_IN_PER_M / PRICE_OUT_PER_M (USD per million tokens, for the estimate only).
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
from sqlalchemy import create_engine, text

import ai_rules
from embeddings import EMBEDDING_MODEL, Embedder, EmbeddingError

ENRICH_LOCK_KEY = 7_311_960_006
FILTER_LOCK_KEY = 7_311_960_005
MAX_RETRIES = 4
OUTPUT_TOKENS_GUESS = 250       # per bill, for the dry-run estimate only


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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


def models() -> list[str]:
    primary = os.environ.get("SCANNING_LLM_MODEL") or os.environ.get("LLM_MODEL") or "google/gemini-2.5-flash"
    fallbacks = [m.strip() for m in (os.environ.get("FALLBACK_LLM_MODELS") or "").split(",") if m.strip()]
    return [primary] + [m for m in fallbacks if m != primary]


def make_client() -> httpx.AsyncClient:
    """Factory so tests can swap in a mock transport."""
    return httpx.AsyncClient(timeout=120.0)


# ── queue ────────────────────────────────────────────────────────────────────

QUEUE_SQL = """
    SELECT b.id, b.source_id, b.title, b.official_title, b.policy_area, b.last_action_text,
           b.official_summary, b.content_hash, b.matched_rule, b.prefilter_score,
           e.status, e.attempts, e.ai_source_hash, e.ai_summary, e.embedding_source_hash,
           (e.embedding IS NULL) AS no_embedding,
           COALESCE((SELECT array_agg(s.subject_name ORDER BY s.subject_name)
                     FROM tracker.bill_subjects s WHERE s.bill_id = b.id), '{}') AS subjects
    FROM tracker.bill_enrichment e JOIN tracker.bills b ON b.id = e.bill_id
    WHERE (e.status = 'pending')
       OR (e.status = 'failed' AND e.attempts < :max_attempts)
       OR (e.status IN ('kept', 'low_score') AND e.ai_source_hash IS DISTINCT FROM b.content_hash
           AND e.attempts < :max_attempts)
       OR (e.status = 'kept' AND e.ai_source_hash IS NOT DISTINCT FROM b.content_hash
           AND (e.embedding IS NULL OR e.embedding_source_hash IS DISTINCT FROM e.ai_source_hash))
    ORDER BY (b.matched_rule = 'semantic'), b.prefilter_score DESC NULLS LAST, b.id
"""


def load_queue(conn, args):
    rows = conn.execute(text(QUEUE_SQL), {"max_attempts": args.max_attempts}).all()
    if args.only == "rules":
        rows = [r for r in rows if r.matched_rule != "semantic"]
    elif args.only == "semantic":
        rows = [r for r in rows if r.matched_rule == "semantic"]
    if args.limit:
        rows = rows[:args.limit]
    return rows


def needs_scoring(r) -> bool:
    return r.status in ("pending", "failed") or r.ai_source_hash != r.content_hash


def as_document(r):
    """The attributes ai_rules._build_user_prompt / embedding_text read."""
    return SimpleNamespace(source_id=r.source_id, title=r.title, official_title=r.official_title,
                           policy_area=r.policy_area, last_action_text=r.last_action_text,
                           official_summary=r.official_summary, ai_summary=r.ai_summary,
                           subjects=[SimpleNamespace(name=s) for s in (r.subjects or [])])


# ── LLM ──────────────────────────────────────────────────────────────────────

class Usage:
    def __init__(self):
        self.calls = self.prompt_tokens = self.completion_tokens = 0
        self.cost = 0.0
        self.cost_reported = False


async def call_llm(client, user_prompt: str, usage: Usage):
    """Old _call_llm: primary model, then fallbacks; same payload. Adds retries
    on 429/5xx/timeouts per model. Returns (parsed_json, model_used)."""
    url = (os.environ.get("OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1").rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY', '')}",
               "Content-Type": "application/json", "X-Title": "AnimalLegislationTracker"}
    last_error = "no model tried"
    for model in models():
        payload = {"model": model,
                   "messages": [{"role": "system", "content": ai_rules.SYSTEM_PROMPT},
                                {"role": "user", "content": user_prompt}],
                   "temperature": 0.2, "response_format": {"type": "json_object"},
                   "usage": {"include": True}}
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = await client.post(url, headers=headers, json=payload)
            except httpx.TransportError as exc:
                last_error = f"{model}: network error {type(exc).__name__}"
                await asyncio.sleep(2 ** attempt)
                continue
            usage.calls += 1
            if r.status_code == 429 or r.status_code >= 500:
                last_error = f"{model}: HTTP {r.status_code}"
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code >= 400:
                last_error = f"{model}: HTTP {r.status_code} {r.text[:200]}"
                break                                  # not retryable: next model
            try:
                data = r.json()
                u = data.get("usage") or {}
                usage.prompt_tokens += u.get("prompt_tokens") or 0
                usage.completion_tokens += u.get("completion_tokens") or 0
                if u.get("cost") is not None:
                    usage.cost += float(u["cost"])
                    usage.cost_reported = True
                content = data["choices"][0]["message"]["content"]
                return ai_rules._parse_llm_json(content), model
            except Exception as exc:                   # unparseable reply: next model, as before
                last_error = f"{model}: unparseable reply ({type(exc).__name__})"
                break
    raise RuntimeError(f"All LLM attempts failed: {last_error}")


# ── per-batch work ───────────────────────────────────────────────────────────

async def score_one(r, client, usage, sem):
    async with sem:
        try:
            raw, model = await call_llm(client, ai_rules._build_user_prompt(as_document(r)), usage)
            score, topics, rationale, summary = ai_rules._normalize_output(raw)
            return {"ok": True, "score": score, "topics": topics, "rationale": rationale,
                    "summary": summary, "model": model}
        except Exception as exc:
            return {"ok": False, "error": str(exc)[:500]}


def write_scores(conn, rows, results, now):
    counts = {"kept": 0, "low_score": 0, "failed": 0}
    for r, res in zip(rows, results):
        if res["ok"]:
            status = "kept" if res["score"] >= ai_rules.AI_THRESHOLD else "low_score"
            counts[status] += 1
            conn.execute(text("""
                UPDATE tracker.bill_enrichment SET status = :status, relevance_score = :score,
                    relevance_topics = CAST(:topics AS jsonb), relevance_rationale = :rationale,
                    ai_summary = :summary, ai_model = :model, ai_generated_at = :now,
                    ai_source_hash = :hash, attempts = 0, last_error = NULL, updated_at = :now,
                    embedding = CASE WHEN :status = 'kept' THEN embedding END,
                    embedding_model = CASE WHEN :status = 'kept' THEN embedding_model END,
                    embedding_source_hash = CASE WHEN :status = 'kept' THEN embedding_source_hash END
                WHERE bill_id = :id"""),
                {"status": status, "score": res["score"], "topics": json.dumps(res["topics"]),
                 "rationale": res["rationale"], "summary": res["summary"], "model": res["model"],
                 "now": now, "hash": r.content_hash, "id": r.id})
        else:
            counts["failed"] += 1
            # New bills become 'failed'; bills being re-scored keep their current score and status.
            conn.execute(text("""
                UPDATE tracker.bill_enrichment SET
                    status = CASE WHEN status IN ('kept', 'low_score') THEN status ELSE 'failed' END,
                    attempts = attempts + 1, last_error = :err, updated_at = :now
                WHERE bill_id = :id"""), {"err": res["error"], "now": now, "id": r.id})
    return counts


def embed_kept(engine, embedder, ids):
    """(Re-)embed kept bills whose embedding is missing or stale. Returns count embedded."""
    if not ids:
        return 0
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT b.id, b.title, b.official_title, b.policy_area, b.official_summary, e.ai_summary,
                   e.ai_source_hash,
                   COALESCE((SELECT array_agg(s.subject_name ORDER BY s.subject_name)
                             FROM tracker.bill_subjects s WHERE s.bill_id = b.id), '{}') AS subjects
            FROM tracker.bills b JOIN tracker.bill_enrichment e ON e.bill_id = b.id
            WHERE b.id = ANY(:ids) AND e.status = 'kept'
              AND (e.embedding IS NULL OR e.embedding_source_hash IS DISTINCT FROM e.ai_source_hash)"""),
            {"ids": ids}).all()
    if not rows:
        return 0
    docs = [SimpleNamespace(title=r.title, official_title=r.official_title, policy_area=r.policy_area,
                            official_summary=r.official_summary, ai_summary=r.ai_summary,
                            subjects=[SimpleNamespace(name=s) for s in (r.subjects or [])]) for r in rows]
    vecs = embedder.embed_many([ai_rules.embedding_text(d) for d in docs])
    with engine.begin() as conn:
        for r, v in zip(rows, vecs):
            conn.execute(text("""UPDATE tracker.bill_enrichment SET embedding = CAST(:v AS vector),
                    embedding_model = :m, embedding_source_hash = :h, updated_at = now()
                WHERE bill_id = :id"""),
                {"v": "[" + ",".join(f"{x:.8f}" for x in v) + "]", "m": EMBEDDING_MODEL,
                 "h": r.ai_source_hash, "id": r.id})
    return len(rows)


# ── run ──────────────────────────────────────────────────────────────────────

def estimate(rows):
    to_score = [r for r in rows if needs_scoring(r)]
    chars = sum(len(ai_rules.SYSTEM_PROMPT) + len(ai_rules._build_user_prompt(as_document(r))) for r in to_score)
    tin = chars / 4
    tout = OUTPUT_TOKENS_GUESS * len(to_score)
    pin = float(os.environ.get("PRICE_IN_PER_M") or 0.30)
    pout = float(os.environ.get("PRICE_OUT_PER_M") or 2.50)
    return to_score, tin, tout, tin / 1e6 * pin + tout / 1e6 * pout, pin, pout


async def run(args) -> int:
    engine = create_engine(db_url(), pool_pre_ping=True)
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('tracker.bill_enrichment')")).scalar():
            print("The tracker schema doesn't exist yet. Run the filter first.")
            return 1
        rows = load_queue(conn, args)

    to_score, tin, tout, cost, pin, pout = estimate(rows)
    rule_n = sum(1 for r in to_score if r.matched_rule != "semantic")
    print(f"Queue: {len(rows)} bills ({len(to_score)} to score: {rule_n} kept by rules, "
          f"{len(to_score) - rule_n} by the semantic gate; {len(rows) - len(to_score)} need only an embedding).")
    print(f"Models: {', '.join(models())}. Estimate for scoring: ~{tin / 1e6:.1f}M input + "
          f"~{tout / 1e6:.1f}M output tokens ≈ ${cost:.2f} at ${pin}/M in, ${pout}/M out "
          f"(set PRICE_IN_PER_M / PRICE_OUT_PER_M to your model's prices).")
    if args.dry_run or not rows:
        return 0

    lock = engine.connect()
    if not lock.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": ENRICH_LOCK_KEY}).scalar():
        print("Another enrichment run is already in progress; exiting.")
        return 0
    lock.commit()
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError):
            pass

    with engine.begin() as conn:
        run_id = conn.execute(text("""INSERT INTO tracker.pipeline_runs (run_type, status, bills_seen)
            VALUES ('enrich', 'running', :n) RETURNING id"""), {"n": len(rows)}).scalar()

    usage, totals = Usage(), {"kept": 0, "low_score": 0, "failed": 0, "embedded": 0, "embed_errors": 0}
    embedder = Embedder()
    sem = asyncio.Semaphore(args.concurrency)
    status, error, t0, done = "completed", None, time.monotonic(), 0
    try:
        async with make_client() as client:
            for i in range(0, len(rows), args.batch_size):
                batch = rows[i:i + args.batch_size]
                score_rows = [r for r in batch if needs_scoring(r)]
                results = await asyncio.gather(*(score_one(r, client, usage, sem) for r in score_rows))
                with engine.begin() as conn:
                    c = write_scores(conn, score_rows, results, utcnow())
                for k in c:
                    totals[k] += c[k]
                try:
                    totals["embedded"] += await asyncio.to_thread(embed_kept, engine, embedder, [r.id for r in batch])
                except EmbeddingError as exc:
                    totals["embed_errors"] += 1
                    print(f"  embedding failed for this batch ({exc}); retried next run")
                done += len(batch)
                rate = done / (time.monotonic() - t0)
                eta = (len(rows) - done) / rate / 3600 if rate else 0
                cost_txt = f"${usage.cost:.2f} reported" if usage.cost_reported else \
                    f"~{usage.prompt_tokens / 1e6:.2f}M in / {usage.completion_tokens / 1e6:.2f}M out tokens"
                print(f"  {done}/{len(rows)} | kept {totals['kept']} | low_score {totals['low_score']} | "
                      f"failed {totals['failed']} | embedded {totals['embedded']} | {usage.calls} LLM calls, "
                      f"{cost_txt} | ETA {eta:.1f} h", flush=True)
                with engine.begin() as conn:
                    conn.execute(text("""UPDATE tracker.pipeline_runs SET bills_kept = :k, bills_rejected = :r,
                        llm_calls = :l, embedding_calls = :e, checkpoint = CAST(:cp AS jsonb) WHERE id = :id"""),
                        {"k": totals["kept"], "r": totals["low_score"], "l": usage.calls,
                         "e": embedder.requests, "id": run_id,
                         "cp": json.dumps({"done": done, "of": len(rows), **totals,
                                           "prompt_tokens": usage.prompt_tokens,
                                           "completion_tokens": usage.completion_tokens,
                                           "cost_usd": round(usage.cost, 4) if usage.cost_reported else None})})
    except asyncio.CancelledError:
        status, error = "interrupted", "stopped by SIGTERM; re-run to continue"
        print("\nStopped. Finished batches are saved; re-run to continue.", flush=True)
    except Exception as exc:
        status, error = "failed", f"{type(exc).__name__}: {str(exc)[:400]}"
        print(f"\nFailed: {error}")
    if status == "completed" and (totals["failed"] or totals["embed_errors"]):
        error = f"{totals['failed']} scoring failures, {totals['embed_errors']} embedding batch failures; retried next run"
    with engine.begin() as conn:
        conn.execute(text("""UPDATE tracker.pipeline_runs SET status = :s, error = :e, finished_at = now(),
            llm_calls = :l, embedding_calls = :ec WHERE id = :id"""),
            {"s": status, "e": error, "l": usage.calls, "ec": embedder.requests, "id": run_id})
        summary = dict(conn.execute(text("SELECT status, count(*) FROM tracker.bill_enrichment GROUP BY 1")).all())
    lock.close()
    print(f"\n{status.capitalize()}: kept {totals['kept']}, low_score {totals['low_score']}, failed {totals['failed']}, "
          f"embedded {totals['embedded']}. {usage.calls} LLM calls, {usage.prompt_tokens:,} input + "
          f"{usage.completion_tokens:,} output tokens"
          + (f", ${usage.cost:.2f} reported by OpenRouter" if usage.cost_reported else "")
          + f". Enrichment table now: {summary}. ({time.monotonic() - t0:.0f}s)")
    return 1 if status == "failed" else 0


# ── preflight ────────────────────────────────────────────────────────────────

async def preflight(args) -> int:
    results = []

    def report(level, msg):
        results.append(level)
        print(f"  [{level}] {msg}")

    print("Enrichment preflight")
    try:
        engine = create_engine(db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            if not conn.execute(text("SELECT to_regclass('tracker.bill_enrichment')")).scalar():
                report("FAIL", "tracker schema missing; run the filter first")
                return 1
            counts = dict(conn.execute(text("SELECT status, count(*) FROM tracker.bill_enrichment GROUP BY 1")).all())
            filter_busy = not conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": FILTER_LOCK_KEY}).scalar()
            if not filter_busy:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": FILTER_LOCK_KEY})
            free = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": ENRICH_LOCK_KEY}).scalar()
            if free:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": ENRICH_LOCK_KEY})
        report("PASS", f"tracker.bill_enrichment: {counts or 'empty'}")
    except Exception as exc:
        report("FAIL", f"database: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1
    report("PASS" if free else "WARN", "no other enrichment run active" if free else "another enrichment run is active")
    if filter_busy:
        report("WARN", "the filter is running now; bills it adds afterwards are picked up next run")

    if not os.environ.get("OPENROUTER_API_KEY"):
        report("FAIL", "OPENROUTER_API_KEY is not set")
    else:
        usage = Usage()
        probe = SimpleNamespace(source_id="TEST-1", title="Puppy Protection Act", official_title=None,
                                policy_area="Animals", last_action_text="Introduced in House",
                                official_summary="Prohibits the sale of puppies from commercial breeders.",
                                subjects=[])
        try:
            async with make_client() as client:
                raw, model = await call_llm(client, ai_rules._build_user_prompt(probe), usage)
            score, topics, _, _ = ai_rules._normalize_output(raw)
            report("PASS", f"scoring model {model} answers in JSON (test bill scored {score}, topics {topics})")
        except Exception as exc:
            report("FAIL" if not args.dry_run else "WARN", f"scoring call: {str(exc)[:300]}")
        try:
            v = Embedder().embed_many(["Animal welfare"])
            report("PASS", f"embeddings work ({len(v[0])} dims)")
        except EmbeddingError as exc:
            report("FAIL" if not args.dry_run else "WARN", f"embeddings: {exc}")

    failed = results.count("FAIL")
    print(f"Preflight: {results.count('PASS')} passed, {results.count('WARN')} warnings, {failed} failed")
    return 1 if failed else 0


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--preflight", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="queue size and cost estimate, no API calls")
    p.add_argument("--limit", type=int, help="process at most N bills from the front of the queue")
    p.add_argument("--only", choices=["all", "rules", "semantic"], default=env("ENRICH_ONLY") or "all")
    p.add_argument("--concurrency", type=int, default=int(env("ENRICH_CONCURRENCY") or 8))
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--max-attempts", type=int, default=5, help="stop retrying a bill after this many failures")
    args = p.parse_args()
    try:
        sys.exit(asyncio.run(preflight(args) if args.preflight else run(args)))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()