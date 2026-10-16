# legtracker-ingestion-gap: daily refresh + tracker filter

One repo, one Docker image, two Railway services, chosen by the service's `JOB` variable:

| `JOB` | What runs | Section |
| --- | --- | --- |
| `refresh` (default) | Daily refresh of the ingestion database for the current Congress (tracker + prediction model) | [Refresh service](#refresh-service) |
| `filter` | The tracker's filter: old LegTracker gates 1–3 over the ingestion data, filling the `tracker` schema | [Filter service](#filter-service) |

Both use `start.sh`: preflight first, then `exec` the job (so Railway's SIGTERM reaches it). Extra arguments for the job go in `JOB_ARGS`.

---

# Refresh service

It keeps the ingestion database current for the **current Congress**, for both the tracker and the prediction model. It replaces the weekly ingestion's fill-once scripts with change detection: every day it refreshes everything about the bills that changed, and nothing else.

```
Step 1  list      /bill/{congress}, all pages (~80 requests)
                  → bills (upsert), raw_ingestion_response (only for new or changed bills)
Step 2  per changed bill:
        detail      → bills.policy_area, introduced_date, legislation_url, sponsor_bioguide_id; member
        actions     → bill_action        (delete + insert per bill)
        cosponsors  → bill_cosponsor     (upsert)
        text        → bill_text_version  (upsert)
        subjects    → bill_subject       (delete + insert per bill)
        summaries   → bill_summary       (delete + insert per bill)
        titles      → bill_title         (delete + insert per bill)
```

| File | Purpose |
| --- | --- |
| `refresh.py` | The job: preflight, list step, per-bill refresh, resume, rate limit, SIGTERM handling, single-instance lock |
| `transforms.py` | Response → row transforms. The ones for the ingestion repo's tables mirror its `src/transform.py` exactly (verified field by field on real responses) |
| `gap_models.py` | The new columns and tables (`bill_subject`, `bill_summary`, `bill_title`, `bill_fetch_log`) |
| `filter_job.py`, `rules.py`, `embeddings.py` | The filter service (see below) |
| `tracker_db/` | Alembic migration for the `tracker` schema (applied by the filter's preflight) |
| `start.sh`, `Dockerfile`, `railway.toml`, `requirements.txt` | Deployment |

## How it decides what to fetch

`bill_fetch_log` stores, per bill and endpoint, the bill's update date at the time of the fetch. A bill is re-fetched when the later of `bills.update_date` and `update_date_including_text` moves past that value. Step 1 is what moves it, which is why both steps run together.

- **Zero items:** when the detail response reports 0 cosponsors, text versions, subjects or summaries, that call is skipped and logged as an empty success.
- **HTTP 404:** logged as `not_found` and not retried unless the bill changes.
- **Network, 429 and 5xx errors:** retried 5 times within the run; anything still failing is logged as `error` and retried on the next run.

## Writing to the ingestion repo's tables

Rows are written exactly as the ingestion repo writes them: same transforms, same columns (read from the database at start-up) and the same upsert keys. Three differences, all deliberate:

| Table | Difference | Why |
| --- | --- | --- |
| `raw_ingestion_response` | One row only when a bill is **new or changed**, not every bill on every run | Keeps it a change history instead of a daily copy of every bill |
| `bill_cosponsor`, `bill_action`, `bill_text_version` | Refreshed whenever the bill changes | The weekly scripts only filled bills that had no rows yet |
| `bill_text_version` | Rows with no `version_type` are replaced, not upserted | NULLs never match a unique constraint, so upserting them would duplicate them on every refresh |

## Moving over from the weekly ingestion

1. **Pause the weekly service's schedule** in Railway: remove its cron schedule, but don't delete the service yet. **Never run both at the same time:** both replace `bill_action` rows for the same bills, and neither can see the other. The preflight warns if another ingestion run is marked `running` in `ingestion_runs`.
2. **Push this version and let it deploy.** It keeps the `bill_fetch_log` progress from the gap backfill: detail, subjects, summaries and titles stay fetched for unchanged bills.
3. **The first run is the big one.** Actions, cosponsors and text versions are new endpoints for this service, so every bill gets them once: about 59,000 requests. That's roughly 3–4 hours at `BACKFILL_MAX_PER_HOUR=18000` and `BACKFILL_CONCURRENCY=8`. It can be stopped and resumed like before.
4. **Check the result** (queries below), then **enable the daily schedule:** uncomment `cronSchedule` in `railway.toml`.
5. **Close the weekly service** once you've confirmed the prediction scoring doesn't run inside it.

## Variables (Railway service → Variables)

| Variable | Value |
| --- | --- |
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}`, using your DB service's name |
| `CONGRESS_API_KEY` | Your key |
| `BACKFILL_MAX_PER_HOUR` | `18000` (your key allows ~20,000/hour; leave headroom if anything else shares it) |
| `BACKFILL_CONCURRENCY` | `8` |
| `BACKFILL_ENDPOINTS` | *(optional)* Subset of `detail,actions,cosponsors,text,subjects,summaries,titles` |
| `BACKFILL_SKIP_LIST` | *(optional)* `1` to skip step 1 |
| `BACKFILL_CONGRESS` | *(optional)* Defaults to the current Congress. On January 3, 2027 that becomes 120; run a second service or a one-off with `119` for a few weeks to catch the old Congress's late updates |

## Commands

```bash
python refresh.py --preflight                       # create gap tables, check DB/keys/API, exit
python refresh.py --preflight --with-action-index   # also create the bill_action identity index
python refresh.py --dry-run                         # worklist from current `bills`, no API calls
python refresh.py                                   # step 1 + step 2
python refresh.py --skip-list                       # step 2 only
python refresh.py --list-only                       # step 1 only
python refresh.py --refresh                         # ignore the fetch log and re-fetch every bill
```

## Checking the data

```sql
-- Coverage per endpoint
SELECT endpoint, status, count(*) FROM bill_fetch_log WHERE congress = 119 GROUP BY 1, 2 ORDER BY 1, 2;

-- Runs
SELECT id, status, started_at, completed_at, records_fetched, records_written, error_message
FROM ingestion_runs WHERE source = 'congress_gov_daily_refresh' ORDER BY id DESC LIMIT 5;

-- What the list step saw each run (new / changed / unchanged)
SELECT r.id, s.detail->>'new' AS new, s.detail->>'changed' AS changed, s.detail->>'unchanged' AS unchanged
FROM run_steps s JOIN ingestion_runs r ON r.id = s.run_id
WHERE s.step_name = 'list' ORDER BY r.id DESC LIMIT 5;

-- Bills with the most cosponsors now (spot check against the weekly fill-once data)
SELECT congress, bill_type, bill_number, count(*) FROM bill_cosponsor
WHERE congress = 119 GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10;
```

## Keep the ingestion repo in sync

- **Paste the `gap_models.py` classes and the three `Bill` columns into `src/models.py`,** so that repo's autogenerate doesn't propose dropping them.
- **If anyone runs the old weekly scripts again,** pause this service first. The list backfill is harmless; it just adds raw rows for every bill. The fill-once scripts skip bills that already have rows.

## Tested

The test ran against a database built from the ingestion repo's own `src/models.py`, seeded with data in the weekly fill-once state.

**Transforms:**

- Compared with the repo's `transform.py` functions on real Congress.gov responses (bills, actions, all 29 cosponsors of HR 1085, text versions, sponsor): identical field by field.

**End to end**, against a mock Congress.gov built from those real responses:

- **List step:** pagination across pages; a bill repeated across pages written once; raw rows only for new or changed bills.
- **Stale data replaced:** 1 stale action became 3; 2 stale cosponsors were updated, all 29 present, and a duplicate in the response collapsed.
- **Pagination and skipping:** 300 actions over 2 pages; zero-count endpoints skipped (5 calls instead of 7).
- **Edge cases:**
  - A text version with no type isn't duplicated on later refreshes.
  - A sponsorless bill is handled.
  - A 404 bill costs 1 call and is marked `not_found`.
  - The list step doesn't blank out an introduced date it doesn't have.
- **Unchanged rerun:** 2 requests, no writes.
- **Change-driven refresh:** after one bill moved, only it was re-fetched (7 calls). It gained the new action and cosponsor with no duplicates, and got one new raw row.

**Preflight:** passes on a Railway-style URL, warns when another ingestion run is active, and fails when an upsert's unique key is missing.

The SIGTERM, resume and single-instance behaviour is the same code as the gap backfill, which was tested earlier and ran in production.


---

# Filter service

It applies the **old LegTracker ingestion gates 1–3** to the ingestion data and fills the tracker's category-1 tables. Gate 4 (the AI score of 30 or more) belongs to the enrichment job, which comes next.

```
reads   public.bills, member, bill_action, bill_subject, bill_title, bill_summary, bill_fetch_log
writes  tracker.bills, bill_actions, bill_subjects, filter_decisions,
        bill_enrichment ('pending' row per new kept bill), pipeline_runs
```

| Gate | Rule | Decision |
| --- | --- | --- |
| 1 | Latest action contains a dead-bill keyword (vetoed, failed, tabled, withdrawn, …; whole words, so "deadline" doesn't count). Enacted bills are kept | `inactive` |
| 2 | Policy area is Animals or Agriculture and Food | kept, `policy_area` |
| 2 | Any subject is one of the 38 approved subjects (`tracker.subject_categories`) | kept, `subject` |
| 2 | An animal keyword (120) in the display title, any title, or the CRS summary | kept, `keyword` |
| 2 | Latest action refers it to an agriculture, natural-resources or environment committee | kept, `committee` |
| 3 | Embedding of title, official title, policy area, subjects and summary scores 20 or more against 16 anchor phrases | kept, `semantic` |
| — | Otherwise: `deferred` if the bill has no subjects and no summary yet, else `no_match` | dropped |

`rules.py` is the old code, copied verbatim from `matching.py` and `semantic_scoring.py`, and tested against the original modules. The inputs are read the way the old code read them:

- **Summary:** the *last* summary in Congress.gov's list, with HTML stripped.
- **Official title:** the longest title whose type contains "official title".
- **Keyword rule:** scans the display title plus every title.

The filter keeps the old decisions, with three operational differences:

- **Gate 3 only runs for bills no rule matched.** The old code ran it for every bill, but it can't change anyone else's outcome, so this saves most of the embedding calls.
- **An embedding failure leaves the bill undecided,** and it's retried on the next run. The old code rejected it as `no_match`.
- **A kept bill that later stops matching is removed** from the tracker. The old code left the stale row in place.

**When bills are re-evaluated:** only when their update date moved since their last decision, or when the refresh service fetched new data for them after it, such as a newly added subject. A bill that changes but stays kept keeps showing its current AI score; its `content_hash` changes, which tells the enrichment job to re-score it.

## Set up the filter service on Railway

1. In the same project: **+ New → GitHub Repo →** this repo again. That gives a second service from the same code.
2. **Variables:**

| Variable | Value |
| --- | --- |
| `JOB` | `filter` |
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}`, as for the refresh service |
| `OPENROUTER_API_KEY` | Your OpenRouter key (embeddings for gate 3) |
| `JOB_ARGS` | `--dry-run` for the first deploy (see below), then remove it |

3. **Set its cron** in the service's Settings → Cron Schedule, later than the refresh service's, once both are checked. `railway.toml` is shared by both services, so set schedules per service in the UI rather than in that file.

## First run, step by step

1. **Preflight** runs on every start. It creates the `tracker` schema (the `vector` extension + 10 tables + 38 approved subjects) if missing, checks the inputs and the OpenRouter key, and prints the coverage of each input.
2. **Dry run without gate 3** (`JOB_ARGS=--dry-run`): no writes, no API calls. Prints how many bills each rule keeps, how many are inactive, how many need gate 3, and examples per rule. **Send me this output before going further.**
3. **Dry run with gate 3** (`JOB_ARGS=--dry-run --with-semantic`): still no writes, but makes the embedding calls, so it shows the final count. Optional.
4. **Real run** (remove `JOB_ARGS`): writes everything and queues every kept bill for AI scoring.

## Commands

```bash
python filter_job.py --preflight                 # create tracker schema, check inputs + OpenRouter
python filter_job.py --dry-run                   # rule counts, no writes, no API calls
python filter_job.py --dry-run --with-semantic   # + gate 3, still no writes
python filter_job.py                             # evaluate changed bills and write
python filter_job.py --reevaluate                # evaluate every bill again
```

## Checking the result

```sql
SELECT decision, matched_rule, count(*) FROM tracker.filter_decisions GROUP BY 1, 2 ORDER BY 1, 2;
SELECT matched_rule, count(*) FROM tracker.bills GROUP BY 1 ORDER BY 2 DESC;
SELECT status, count(*) FROM tracker.bill_enrichment GROUP BY 1;
SELECT run_type, status, started_at, bills_seen, bills_kept, bills_rejected, embedding_calls, error
FROM tracker.pipeline_runs ORDER BY started_at DESC LIMIT 5;
```

## Tested

**Rules:** `rules.py` against the original `matching.py` and `semantic_scoring.py` on tricky inputs ("deadline", "Became Public Law", "grapes and apes", tabled, withdrawn, HTML summaries): identical results.

**End to end** on a database built from the ingestion repo's `src/models.py` plus the gap tables, with a mock OpenRouter:

- **Every rule decides correctly:**
  - "deadline" stays active; an enacted bill is kept
  - the keyword is found only in the longest official title
  - committee referral; semantic score 87
  - `no_match`, and `deferred` for a bill with no data
- **Inputs read correctly:**
  - latest summary version used
  - approved subjects only stored
  - actions copied; sponsor taken from `member`
- **Embeddings:** a failed batch is retried text by text, so only the bad text stays undecided, and it's retried on the next run.
- **Upstream changes:**
  - A vetoed bill is removed, cascading through enrichment and a user's watchlist.
  - A subject added through the refresh service, with no update-date change, is picked up through `bill_fetch_log`.
  - A changed summary keeps the bill's AI score visible and flags it for re-scoring.
- **Unchanged rerun:** only the still-undecided bill is evaluated.
- **Safety:** a second filter run exits immediately; SIGTERM during gate 3 stops without writing and logs `interrupted`.
- **`start.sh`:** both `JOB` modes, `JOB_ARGS`, a preflight failure stopping the job, and an unknown `JOB`.

**Not tested here:** the real OpenRouter embeddings endpoint (no key in this environment). The client falls back to one text per request if OpenRouter doesn't accept batched input, and the preflight makes one real call to confirm the key and the 1,536-dimension vectors.
