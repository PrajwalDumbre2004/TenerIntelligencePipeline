# MongoDB → PostgreSQL Migration Plan

## 1. Current State — MongoDB Touchpoints

Three sequential pipelines, orchestrated by `pipeline_main/run.py`, month-by-month:

- **Pipeline2** — scrapes listings → `TenderIntelligence.tenders_YYYY` (upsert on `bid_id`, `$setOnInsert`-only).
- **Pipeline3** — enriches tenders with results/buyer/contract details → same collection, sets `scraped_at`.
- **Pipeline4** — reads tenders with `derived_at=None`, flattens into per-seller participation records, rebuilds `TenderAnalytics` (`seller_tenders`, `sellers`, `seller_buyers`, `seller_categories`, `seller_competitors`).

No FastAPI/backend layer exists yet — `asyncpg`/`psycopg3` in `Postgresql_schema.md` are target-state only.

### Files that must change (MongoDB → PostgreSQL)

| File | Role | Change needed |
|---|---|---|
| `pipeline2/db.py` | Mongo client, tender upserts, `pipeline2_status`, `failed_pages`/`error_logs` | Rewrite as SQL: `bulk_upsert_tenders` → `INSERT ... ON CONFLICT DO NOTHING`; status/log tables need Postgres equivalents |
| `pipeline2/config.py` | `MONGO_URI`, `MONGO_DB_NAME` | Replace with Postgres DSN |
| `pipeline3/db.py` | Reads pending tenders, writes results, `scraped_at` marker, failure logging | Rewrite as SQL: `SELECT ... WHERE scraped_at IS NULL`, `UPDATE tenders SET ...` |
| `pipeline3/config.py` | Mongo URI/db name | Same as pipeline2/config.py |
| `pipeline4/db.py` | Source reads, target upserts for all 5 analytics collections, `ensure_indexes()` | Becomes SQL against `tenders` + `bid_participations` + `sellers` + `seller_competitors`; `seller_buyers`/`seller_categories` become `REFRESH MATERIALIZED VIEW` calls |
| `pipeline4/config.py` | Mongo URI, `SOURCE_DB`/`TARGET_DB`, collection constants | Replace with Postgres connection config; collection names become table/view names |
| `pipeline4/builders.py` | Python aggregation now replicated by Postgres `GROUP BY` | `build_seller_buyers_docs`/`build_seller_categories_docs` become unnecessary (replaced by materialized views). `build_seller_profile` should be driven by a SQL aggregate, not a Python loop. `build_seller_tenders_docs` shrinks — drops `category`/`buyer_name`/`buyer_department`/`ministry`/`month`/`bid_number*`/`ra_number` (live only in `tenders`, joined via `bid_id`), keeps `end_date` (intentional denormalization) |
| `pipeline4/orchestrator.py` | Calls `pipeline4/db.py`; also has an inline Mongo `bulk_write` block (lines 138–192) duplicating `upsert_seller_tenders` | Same SQL rewrite; also needs to compute `participation_year`/`tender_year` (new partition keys), nothing currently computes these |
| `pipeline4/extract.py` | Pure transformation logic, no DB calls | **No change** |
| `pipeline_main/run.py` | Imports `pipeline2.db` functions only | No new DB logic itself, but breaks until dependencies are ported |
| `backfill_derived.py` | Bulk-sets `derived_at` on historical tenders | Needs Postgres port if kept |
| `derived_collections/derived_claude.py`, `derived_collections/build_competitors.py` | Legacy standalone Mongo/SQLite ETL, explicitly superseded by pipeline4 | Confirm: dead code to leave alone, or still needed (e.g. for `watched_sellers`/`pipeline_refresh_state`) |
| `test/validate_data_consistency.py`, `data_quality_report.py`, `spot_check_tender.py` | Direct pymongo QA scripts | Lower priority; port once switch happens |
| `requirements.txt` | `pymongo`, `dnspython`, `certifi` (Atlas TLS) | Add `psycopg[binary]`; drop `pymongo` once nothing imports it |

### Schema gaps found in `Postgresql_schema.md` (must resolve before migrating)

1. ~~**No `derived_at` column** on `tenders`~~ — **RESOLVED (dissolved) by the staged-merge architecture (Section 2).** `derived_at` only existed because pipeline4 ran as a separate, later pass over a permanent `tenders` table, needing to know which rows hadn't been derived yet. In the new design, `tenders` and `bid_participations` are written together inside one merge transaction, right after validation — there is never a moment where a `tenders` row exists without its participants already extracted. The marker isn't needed at all.
2. **No pipeline bookkeeping tables** — `pipeline2_status` (dual-purpose: pipeline3 writes its own `p3_*` fields into the *same* document), `failed_pages`, `failed_tenders`, `error_logs` have no Postgres counterparts. **Not resolved by staging** — this is about tracking scraping progress/failures, orthogonal to where the data ultimately lands. Replaced by `pipeline_runs` + `pipeline_step_log` (Section 2). Worth folding `failed_pages`/`failed_tenders`/`error_logs` directly into `pipeline_step_log` (e.g. `step_name='extract_page_failed'` / `'extract_tender_failed'`, with `detail` JSONB carrying `page_no`/`bid_id`) instead of maintaining four separate tables.
3. **No column for `contract_details`** — deferred; to be added separately by the schema owner. Not addressed by this plan.
4. **Denormalization removal changes builder inputs** — `bid_participations` doesn't carry `category`/`buyer_name`/`month`; any remaining logic needing those must `JOIN tenders USING (bid_id)` instead of reading a flat dict. **Not resolved by staging** — this is a property of the target schema itself, independent of how data arrives there.
5. **New required fields**: `participation_year` (partition key for `bid_participations`) and `tender_year` (partition key for `tenders`) — nothing today computes these; must be derived from `end_date`. **Staging gives the natural place to implement this** (in `merge/merge_tenders.py` / `merge/merge_participations.py`, computed from the staging row's `end_date`), but doesn't compute it automatically — still explicit code to write.
6. **Upsert semantics differ** — Mongo `$setOnInsert` (pipeline2) → `INSERT ... ON CONFLICT DO NOTHING`; Mongo `ReplaceOne(upsert=True)` (pipeline4) → `INSERT ... ON CONFLICT DO UPDATE`. **Staging clarifies where each applies** (staging writes are insert-or-ignore; the merge into `public` is insert-or-update) but each call site still needs the correct `ON CONFLICT` clause written explicitly.
7. **`seller_categories`/`seller_buyers` become materialized views** — pipeline4 Phase 2 no longer needs per-seller Python aggregation for these two; just triggers `REFRESH MATERIALIZED VIEW CONCURRENTLY` after Phase 1 writes land. Unaffected by the staging decision either way.

---

## 2. Target Architecture — Staged, Validated, Atomic Merge

### Design constraints (from requirements)
1. Raw data must be fetched from GeM and land in a **temporary staging area** first. Only after a successful fetch does it get merged into the main DB; staging data is deleted after a successful merge.
2. The merge involves multiple operations in sequence — **all must succeed, or none should take effect** (all-or-nothing).
3. **Every step must be logged.**
4. **Validation checks** run before merge, gating what is allowed into the main DB (rules to be added later; the seam must exist now).

### Key decision: staging as a separate schema, not a separate database
PostgreSQL cannot run a single atomic transaction across two different databases. A `staging` schema in the **same** database as production keeps merges inside one real ACID transaction — the strongest guarantee available, and required for constraint #2. A physically separate database would force a Saga/compensating-transaction pattern, which is materially weaker.

```sql
CREATE SCHEMA staging;

CREATE TABLE staging.tenders_raw (
    run_id             UUID        NOT NULL,       -- one per pipeline_main invocation
    batch_key          TEXT        NOT NULL,        -- e.g. "2026-04" (month)
    bid_id             BIGINT      NOT NULL,
    -- ... same columns as production `tenders` ...
    bid_result_raw     JSONB,
    ra_result_raw      JSONB,
    buyer_details_raw  JSONB,
    contract_details_raw JSONB,
    scraped_at         TIMESTAMPTZ,

    validated          BOOLEAN     NOT NULL DEFAULT FALSE,
    validation_errors  JSONB,
    merged_at          TIMESTAMPTZ,

    PRIMARY KEY (run_id, bid_id)
);
```

`public` schema = the existing `Postgresql_schema.md` design (`tenders`, `bid_participations`, `sellers`, materialized views, `seller_competitors`, etc.). Only the merge step ever writes here, and only inside a transaction.

### Stage boundaries

**A. Extract** (pipeline2 + pipeline3, scraping logic unchanged) — write/enrich rows only in `staging.tenders_raw`, keyed by `(run_id, bid_id)`. Batch is extract-complete using the same checks as today: pipeline2's numFound convergence, pipeline3's `scraped_at IS NOT NULL` count matching batch total.

**B. Validate** (hook for rules to be added later):
```
validate_batch(run_id, batch_key) -> { passed: bool, rows_ok: int, rows_quarantined: int, rows_blocking: int }
```
Writes per-row results into `validation_errors`, flips `validated = TRUE` on rows that clear checks. Policy: "hard" failures (missing `bid_id`, unparseable core fields) block the whole batch; "soft" failures get quarantined (excluded from merge, left for review) without blocking the rest of the batch. Until real checks are written, this stub sets `validated = TRUE` for everything so nothing downstream needs to change later.

**C. Merge** (the atomicity-critical step — replaces most of today's pipeline4 db-write code) — one transaction per batch:
```sql
BEGIN;

INSERT INTO tenders (bid_id, tender_year, category, ..., bid_result_raw, ...)
SELECT bid_id, EXTRACT(YEAR FROM end_date), category, ..., bid_result_raw, ...
FROM staging.tenders_raw
WHERE run_id = $1 AND batch_key = $2 AND validated
ON CONFLICT (bid_id, tender_year) DO UPDATE SET ...;

-- application code: extract_participants() per tender (unchanged Python logic),
-- batched via execute_values/COPY, then:
INSERT INTO bid_participations (seller_id, bid_id, participation_year, ...)
VALUES ...
ON CONFLICT (seller_id, bid_id, participation_year) DO UPDATE SET ...;

UPDATE staging.tenders_raw
SET merged_at = now()
WHERE run_id = $1 AND batch_key = $2 AND validated;

COMMIT;
```
Any failure rolls back the entire transaction — staging rows are left exactly as they were (`merged_at IS NULL`), so the batch can simply be retried. No partial `tenders` rows without matching `bid_participations`, ever.

Deliberately **not** in this transaction: rebuilding `sellers`, refreshing `seller_categories`/`seller_buyers` materialized views, recomputing `seller_competitors`. These are fully derivable from `bid_participations` at any time (same "incremental extract, full recompute" principle pipeline4 already uses), so they don't need atomicity with the raw merge — if they fail, the core data is already safely committed and the step can just be re-run.

**D. Cleanup** — only after the merge transaction commits:
- **Decision: cleanup deletes rows, it never drops the schema/table.** `DELETE FROM staging.tenders_raw WHERE run_id = ...` — the `staging` schema and `staging.tenders_raw` table definition persist across every run; only the rows belonging to a completed run are removed. Dropping the schema would destroy the table the next extraction run needs to write into.
- Recommended: don't hard-delete immediately. Keep merged rows in staging for a retention window (e.g. 7 days) as a replay/audit buffer, purged by a separate scheduled job. Independent of the merge transaction, so it can't compromise atomicity.
- **Cleanup does not wait for the aggregates stage.** Staging's only job is feeding the `tenders`/`bid_participations` merge — once that transaction commits, staging has served its purpose. The aggregates stage (below) reads exclusively from `bid_participations`/`tenders` in the `public` schema, never from staging, so a slow or failing aggregate step must not hold staging cleanup hostage.

### Impacted-Seller Recompute (ported from today's pipeline4 Phases 2–3)

Today's pipeline4 doesn't recompute analytics for all ~306K sellers on every run — only for the sellers who appear in the newly-derived batch ("impacted sellers"), reading each one's full history to guarantee correctness (incremental read + full recompute). This carries over to the new pipeline's **aggregates stage**, which runs after the merge transaction commits:

- **Source of `impacted_sellers`**: `merge/merge_participations.py`'s `insert_batch()` already returns the distinct `seller_id`s written into `bid_participations` for this batch — this becomes the aggregates stage's input, replacing today's Phase 1 side-effect of accumulating `impacted_sellers` in Python.
- **`sellers` profile, per impacted seller** — same logic as today's `build_seller_profile()`. Only the fetch query changes: `SELECT ... FROM bid_participations JOIN tenders USING (bid_id) WHERE seller_id = ANY($impacted)` instead of a Mongo `find()`, since `category`/`buyer_name`/`month` now come from the join instead of a denormalized flat doc. Per-seller row counts are small enough that pulling to Python and reusing the existing `Counter`-based top-10 logic is still the simplest approach.
- **`seller_competitors`, two-pass (same as today's Phase 3)** — Pass 1 recomputes competitors for every seller in `impacted_sellers` via a SQL self-join (`bp1 JOIN bp2 USING (bid_id) WHERE bp1.seller_id = $1 AND bp2.seller_id <> $1 GROUP BY bp2.seller_id`), replacing today's chunked Mongo `$in` loop — this can even run as one batched query across all impacted sellers instead of per-seller. Pass 2 recomputes competitors for any newly-discovered co-participant not already in `impacted_sellers`, exactly like today, so both sides of every affected `(seller_id, competitor_id)` pair stay in sync.
- **Materialized views (`seller_categories`, `seller_buyers`) — an important limitation**: unlike `sellers`/`seller_competitors`, `REFRESH MATERIALIZED VIEW` always recomputes the *entire* view — there is no "refresh only these sellers' rows." The per-seller incremental-recompute optimization Phase 2 relies on today does **not** carry over to these two. At an estimated 4.6M/9M rows, refreshing after every single batch merge may be wasteful if batches run frequently; decide between refreshing after every merge (simplest, matches today's per-month cadence) or on a coarser schedule (e.g. once per `pipeline_main` run, or a periodic cron) decoupled from individual batch merges.
- **Ordering**: `sellers → seller_competitors → materialized views`, run sequentially (decided). None of the three actually depends on another's output — all three read only from already-committed `bid_participations`/`tenders` — so this is a chosen simplicity/predictability trade-off, not a correctness requirement.

### Logging

Two tables in `public` (staging gets wiped, logs must live outside it):
```sql
CREATE TABLE pipeline_runs (
    run_id       UUID PRIMARY KEY,
    batch_key    TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'started'   -- started | succeeded | failed
);

CREATE TABLE pipeline_step_log (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES pipeline_runs(run_id),
    step_name    TEXT NOT NULL,   -- extract | validate | merge_begin | merge_commit | cleanup
    status       TEXT NOT NULL,   -- started | succeeded | failed
    detail       JSONB,
    error_message TEXT,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
Critical rule: log writes are their own transactions, separate from the merge transaction. "merge_begin: started" commits before the merge transaction opens; "merge_commit: succeeded/failed" commits right after the merge transaction resolves — so the log stays trustworthy even when the merge itself rolls back.

### Per-batch state machine

`batch_key` moves through:
```
PENDING → EXTRACTING → EXTRACTED → VALIDATING → VALIDATED | VALIDATION_FAILED → MERGING → MERGED → AGGREGATING → CLEANED
```
Crash recovery: a batch stuck in `MERGING` is safe to retry from `VALIDATED` — Postgres already rolled back any partial transaction. A batch stuck in `EXTRACTING` resumes the same multi-pass convergence logic already in place.

---

## 3. Module Plan

```
TenerIntelligencePipeline/
├── common/                     # NEW — shared by every stage
│   ├── db.py
│   ├── config.py
│   ├── logging_db.py
│   └── run_context.py
│
├── pipeline2/                  # EXTRACT (listings) — db.py rewritten, rest unchanged
│   └── db.py
├── pipeline3/                  # EXTRACT (results) — db.py rewritten, rest unchanged
│   └── db.py
│
├── validate/                   # NEW — Stage B
│   ├── rules.py
│   ├── validator.py
│   └── config.py
│
├── merge/                      # NEW — Stage C, the atomic transaction
│   ├── extract_participants.py     # = today's pipeline4/extract.py, moved verbatim
│   ├── merge_tenders.py
│   ├── merge_participations.py
│   ├── merge_orchestrator.py
│   └── config.py
│
├── aggregates/                 # = today's pipeline4 Phase 2/3, restructured
│   ├── builders.py                 # build_seller_categories/buyers DELETED (now MVs)
│   ├── refresh_views.py
│   ├── competitors.py
│   ├── db.py
│   └── run.py
│
├── cleanup/                    # NEW — Stage D
│   └── purge_staging.py
│
└── pipeline_main/
    └── run.py                      # rewritten as the batch state machine
```

### `common/db.py`
Single Postgres connection factory, shared by everything (replaces 3 separate `MongoClient` singletons).
```
get_connection() -> psycopg.Connection      # one pooled connection/txn scope per call site
STAGING_SCHEMA = "staging"
PROD_SCHEMA = "public"
```

### `common/logging_db.py`
Wraps `pipeline_runs` / `pipeline_step_log`. Never writes logs inside the merge transaction.
```
start_run(batch_key: str) -> UUID                     # inserts pipeline_runs row, own commit
log_step(run_id, step_name, status, detail=None, error=None)  # own commit
finish_run(run_id, status: str)                       # updates pipeline_runs.status/finished_at
```

### `common/run_context.py`
```
@dataclass
class RunContext:
    run_id: UUID
    batch_key: str          # e.g. "2026-04"
    def step(self, name):   # context manager: log_step(...,"started") on enter, "succeeded"/"failed" on exit
```

### `pipeline2/db.py` (rewritten)
Same function names as today, new target: `staging.tenders_raw` instead of Mongo `tenders_YYYY`.
```
bulk_upsert_tenders(docs, run_id, batch_key) -> int     # INSERT ... ON CONFLICT (run_id, bid_id) DO NOTHING
get_batch_bid_count(run_id, batch_key) -> int
init_batch_status / start_batch / update_batch_progress / complete_batch / fail_batch(...)  # via common.logging_db, replaces pipeline2_status
```

### `pipeline3/db.py` (rewritten)
```
get_tenders_needing_results(run_id, batch_key) -> list[dict]     # WHERE scraped_at IS NULL
update_tender_results(run_id, bid_id, results: dict) -> bool     # UPDATE staging.tenders_raw SET bid_result_raw=...
mark_tender_failed(run_id, bid_id, error) -> None
```

### `validate/validator.py` — the hook to fill in later
```
@dataclass
class ValidationReport:
    passed: bool
    rows_ok: int
    rows_quarantined: int
    rows_blocking: int
    errors: list[dict]

validate_batch(run_id, batch_key) -> ValidationReport
    # runs validate/rules.py checks over staging.tenders_raw WHERE run_id=..
    # writes validation_errors JSONB + validated=TRUE per row
    # STUB TODAY: marks every row validated=TRUE, returns passed=True
```

### `validate/rules.py`
```
CHECKS: list[Callable[[dict], CheckResult]]   # empty today — real rules added later, each tagged
                                                # severity="blocking" | "quarantine"
```

### `merge/merge_orchestrator.py` — the atomicity-critical piece
```
run_merge(ctx: RunContext) -> MergeResult
    """
    Opens ONE transaction:
      1. merge_tenders.insert_batch(conn, ctx)          -- staging.tenders_raw -> public.tenders
      2. merge_participations.insert_batch(conn, ctx)   -- extract_participants() + bulk insert -> bid_participations
      3. mark staging rows merged_at = now()
    COMMIT on success / ROLLBACK on any exception.
    Logs merge_begin/merge_commit via ctx.step(...).
    Returns which tenders/sellers were touched, for the aggregates stage to consume.
    """
```
```
# merge_tenders.py
insert_batch(conn, ctx) -> list[int]            # returns bid_ids merged, no commit (caller controls txn)

# merge_participations.py
insert_batch(conn, ctx, bid_ids) -> set[str]    # returns impacted seller_ids, no commit
```

### `aggregates/` — unchanged philosophy, split out of merge for non-atomic idempotent recompute
```
# aggregates/db.py
get_participations_for_sellers(seller_ids: set[str]) -> dict[str, list[dict]]
    # was pipeline4/db.py get_all_seller_tenders; now SELECT ... FROM bid_participations
    # JOIN tenders USING (bid_id) WHERE seller_id = ANY(seller_ids) -- category/buyer/month via join
upsert_seller_profile(seller_id, profile)
upsert_seller_competitors(seller_id, competitors)

# aggregates/builders.py
build_seller_profile(seller_id, participations) -> dict   # kept, same Counter-based top-10 logic as today
# build_seller_categories_docs / build_seller_buyers_docs DELETED — replaced by materialized views

# aggregates/competitors.py
recompute_competitors(impacted_sellers: set[str]) -> dict
    # Pass 1: SQL self-join per impacted seller (or batched):
    #   SELECT bp2.seller_id, COUNT(*) FROM bid_participations bp1
    #   JOIN bid_participations bp2 USING (bid_id)
    #   WHERE bp1.seller_id = ANY($impacted) AND bp2.seller_id <> bp1.seller_id
    #   GROUP BY bp1.seller_id, bp2.seller_id
    # Pass 2: recompute for newly-discovered co-participants not already in impacted_sellers
    # (ported from pipeline4/orchestrator.py Phase 3, same two-pass shape)

# aggregates/refresh_views.py
refresh_seller_categories()   # REFRESH MATERIALIZED VIEW CONCURRENTLY seller_categories -- full-view refresh, not scoped to impacted_sellers
refresh_seller_buyers()

# aggregates/run.py
run_aggregates(impacted_sellers: set[str]):
    # Sequential (decided), though no step depends on another's output:
    #   1. rebuild sellers profiles for impacted_sellers
    #   2. recompute_competitors(impacted_sellers)          -- two-pass
    #   3. refresh_seller_categories(); refresh_seller_buyers()   -- consider decoupling cadence from per-batch merges
```
`builders.build_seller_buyers_docs` / `build_seller_categories_docs` are **deleted** — the materialized views replace them entirely.

### `cleanup/purge_staging.py`
```
purge_merged_batches(older_than_days: int = 7) -> int   # DELETE FROM staging.tenders_raw WHERE merged_at < now() - interval
```

### `pipeline_main/run.py` — the state machine
```
BATCH_STATES = ["PENDING","EXTRACTING","EXTRACTED","VALIDATING","VALIDATED",
                "VALIDATION_FAILED","MERGING","MERGED","AGGREGATING","CLEANED","FAILED"]

process_batch(batch_key: str):
    ctx = RunContext(run_id=start_run(batch_key), batch_key=batch_key)
    with ctx.step("extract"):      pipeline2.orchestrator.process_month(ctx)  # pipeline2 + pipeline3 sequentially
    with ctx.step("validate"):
        report = validate.validator.validate_batch(ctx.run_id, batch_key)
        if not report.passed: finish_run(ctx.run_id, "failed"); return
    with ctx.step("merge"):        result = merge.merge_orchestrator.run_merge(ctx)
    with ctx.step("aggregates"):   aggregates.run.run_aggregates(result.impacted_sellers)
    with ctx.step("cleanup"):      cleanup.purge_staging.purge_merged_batches()
    finish_run(ctx.run_id, "succeeded")
```

### What's deleted vs. kept from today's code

| Today | Becomes |
|---|---|
| `pipeline4/db.py` write functions (`upsert_seller_tenders`, `upsert_seller_buyers`, `upsert_seller_categories`) | Split: raw-record writes → `merge/merge_participations.py`; buyer/category aggregation → deleted, replaced by materialized views |
| `pipeline4/orchestrator.py` inline `ReplaceOne` block (lines 138–192) | Gone — folded into `merge/merge_participations.py`, single code path |
| `pipeline4/extract.py` | Moved as-is to `merge/extract_participants.py` — zero logic change |
| `pipeline2_status` / `p3_*` fields | Replaced by `pipeline_runs` + `pipeline_step_log` (via `common/logging_db.py`) |
| `derived_collections/*.py`, `backfill_derived.py` | Not addressed by this plan — retire vs. port is still an open decision |

---

## 4. Open Decisions / Next Steps

1. Decide fate of `derived_collections/*.py` and `backfill_derived.py` — retire or port.
2. Write the actual DDL for `staging.tenders_raw`, `pipeline_runs`, `pipeline_step_log`, plus the missing production-schema pieces (`derived_at`/`tender_year`/`contract_details_raw` columns, `pipeline_refresh_state`-style bookkeeping).
3. Implement `common/` and `merge/` first — everything else depends on them.
4. Design real validation rules for `validate/rules.py` (currently a stub).
</content>
