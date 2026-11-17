# Running the pipeline on Azure: design and required changes

*Status: design agreed, not yet implemented. Analysis done on 2026-11-17.*

This document describes how the GeM tender pipeline will run on Azure on a monthly schedule, and lists every change the repository and the database need for it. Part 1 explains the new design. Part 2 lists the changes.

---

## Part 1: How the new pipeline works

### 1.1 In one paragraph

Once a month, an **Azure Container Apps Job** starts a container that runs **pipeline2** (GeM listing API) and **pipeline3** (result pages and participant data) for the **last 6 months**, writing into the `staging` schema of the existing Azure PostgreSQL server. When scraping finishes, the container writes a single "ready to merge" row into a status table and exits, so container billing stops. Inside the database, a **pg_cron job** checks that status table every 15 minutes. When it finds a run that is ready, it runs **pipeline4** (validation → merge into the main schema → seller aggregates → view refresh → staging cleanup) as stored procedures, entirely inside the database. The container never waits on the merge, and the merge never depends on a client connection staying open for hours.

### 1.2 Architecture

```
 1st of the month (cron, UTC)                           Azure Database for PostgreSQL
                                                        tg-postgres-server-prod (Central India)
┌────────────────────────────────────┐                 ┌────────────────────────────────────────────┐
│ Azure Container Apps Job           │                 │                                            │
│ (Central India, 4 vCPU / 8 GiB)    │                 │  ops.pipeline_runs   ← shared status board │
│                                    │ 1. check status │                                            │
│  a. startup check ─────────────────┼────────────────▶│  status: scraping / pending / running /    │
│  b. insert row "scraping"          │                 │          done / failed                     │
│  c. pipeline2 → pipeline3          │ 2. write        │                                            │
│     for months M-6 … M-1  ─────────┼────────────────▶│  staging.*  (tenders_raw,                  │
│     (8–12 h, writes as it goes)    │                 │              bid_participations_raw …)     │
│  d. set row to "pending"  ─────────┼────────────────▶│                                            │
│  e. exit  → billing stops          │                 │  pg_cron job, every 15 min:                │
└────────────────────────────────────┘                 │    CALL ops.run_pending_merge()            │
                                                       │      validate → merge (01+02+03)           │
                                                       │      → refresh views → cleanup             │
                                                       │      COMMIT after each step                │
                                                       │                                            │
                                                       │  main schema: tenders, bid_participations, │
                                                       │  sellers, seller_competitors, views        │
                                                       └────────────────────────────────────────────┘
```

### 1.3 The components and their jobs

| Component | Where it runs | Its job | What it does **not** do |
|---|---|---|---|
| **Container Apps Job** | Azure Container Apps, Central India, Consumption plan | Startup check, then pipeline2 and pipeline3 for the 6-month window, writing to `staging`. Marks the run `pending` and exits. | Never runs pipeline4. Never waits for the merge. |
| **pg_cron job** | Inside the PostgreSQL server | Every 15 min, calls `ops.run_pending_merge()`. If a run is `pending`, runs pipeline4 in the database. | Never looks at staging to decide anything. Acts only on a `pending` row. |
| **`ops.pipeline_runs`** | Table in the database | The shared status board. The container and pg_cron communicate only through it. Also keeps the history of every run. | |
| **`ops.run_pending_merge()`** | Stored procedure | Runs one merge end to end: lock, pick the run, run the steps, record progress and the outcome. | |
| **pipeline4 procedures** | Stored procedures | Today's `pipeline4/sql` and `validation_sql` scripts, converted to procedures | |

### 1.4 Why the work is split this way

- **The container is only needed for scraping.** pipeline2 and pipeline3 drive headless Chromium (Playwright) and need CPU and memory. pipeline4 is pure SQL that runs on the database server, so a container running it would sit idle for hours.
- **Running the merge inside the database is more reliable.** The merge can take 3–4 hours. If it runs from a client (`psql`), a dropped network connection rolls back the open merge transaction and the work is lost. Inside the database there is no client connection to lose.
- **Cost.** The container is billed only while it scrapes. The merge uses the database server you already pay for, and pg_cron is free.

### 1.5 The status table: `ops.pipeline_runs`

One row per monthly cycle.

| Column | Meaning |
|---|---|
| `id` | Run number |
| `months` | Window covered, e.g. `2026-05..2026-10` |
| `status` | `scraping` → `pending` → `running` → `done` or `failed` |
| `scrape_started`, `scrape_finished` | Set by the container |
| `merge_started`, `merge_finished` | Set by the merge procedure |
| `merge_step` | Current or last merge step: `validate`, `merge`, `refresh`, `cleanup` |
| `error` | Reason for a failure |

Lifecycle:

```
 scraping ──(pipeline2+3 finished)──▶ pending ──(pg_cron picks it up)──▶ running ──▶ done
   ▲                                                                       └──────▶ failed
   └── container inserts it at start
```

| Moment | Who | Status change |
|---|---|---|
| Container starts | Container | Inserts a new row with status `scraping` (or resumes a stuck one, see the rules) |
| pipeline2 + pipeline3 finish | Container | `scraping` → `pending` |
| Next 15-min check | pg_cron | `pending` → `running`; `merge_step` updated at each step |
| Merge finishes | pg_cron | `running` → `done` |
| Merge errors or crashes | pg_cron (next check) | `running` → `failed`, with the step it reached |

What it is used for:
1. **Coordination in both directions.** The container won't scrape during a merge, and a merge can't start during a scrape.
2. **Health check in one query.** The latest rows show whether the last run finished, how long scraping and merging took, and where a failure happened.
3. **Recovery.** A crashed scrape leaves its row at `scraping` and the next run resumes it. A crashed merge leaves `merge_step` showing how far it got.
4. **Timing history.** Real durations for setting the job timeout, the start time and alert thresholds, and for spotting merges that grow over time.
5. **Alerting.** Simple rules, such as "a run is `failed`" or "a run has been `running` for more than 8 hours".

### 1.6 The rules

**Container, at start:**

| Latest relevant row in `ops.pipeline_runs` | Container does |
|---|---|
| `pending` or `running` | A merge is waiting or in progress → **exit without touching staging** |
| `scraping` | A previous container crashed mid-scrape → **resume that row** (the pipeline continues where it stopped) |
| `failed` (and nothing newer) | **Scrape anyway** and log a warning. Leftover staging data is safe to build on (see 1.8); the failure still needs someone to look at it. |
| `done`, or no rows | Normal case → insert a new `scraping` row and scrape |
| A run for **this month** already exists and is `pending`/`running`/`done` | Already handled this month → exit (needed only for the self-healing schedule, see 1.9) |

**Container, at end:**
- Change its row from `scraping` to `pending`, **even if some months partly failed**. Partial data in staging is valid; failed scrapes simply wait in staging for the next run.

**pg_cron / merge procedure:**
- Acts **only** on a `pending` row. A `scraping` row is ignored, so a merge can never start in the middle of a scrape.
- Takes a session-level **advisory lock** first, so only one merge ever runs. If the lock is taken, the check returns immediately.
- If it holds the lock and finds a row still marked `running`, the previous merge crashed → mark that row `failed` with its step.
- Picks the oldest `pending` row, sets it to `running`, then runs the steps, **committing after each one**: `validate` → `merge` (01 + 02 + 03 in **one** transaction, as today) → `refresh` → `cleanup` → `done`.
- A manual merge (calling the procedure by hand) follows the same rules: it does nothing unless a row is `pending`.

### 1.7 Timeline of a normal month

| Time (example, IST) | Container | pg_cron (every 15 min) |
|---|---|---|
| 1st, 20:00 | Starts; startup check passes; inserts `scraping`; begins scraping | Check: no `pending` row → returns in milliseconds |
| 20:00 → 06:00 | pipeline2 + pipeline3 write to staging continuously | ~40 checks; each finds no `pending` row and does nothing, even though staging is filling up |
| 2nd, 06:00 | All 6 months done; sets the row to `pending`; exits (billing stops) | |
| 06:00–06:15 | | Next check finds `pending` → lock → `running` → merge starts |
| 06:15 → ~10:00 | | validate → merge → refresh → cleanup, committing after each step |
| During the merge | | Later checks can't get the lock and return immediately |
| ~10:00 | | Row → `done`; lock released |

The start time is chosen so the merge, which runs in one long transaction on the production server, happens in the early morning rather than during business hours.

### 1.8 Edge cases

| Situation | What happens |
|---|---|
| Container crashes mid-scrape | Its row stays `scraping`; no merge is requested. The retry or next run resumes it. Month status, the permanent per-month `run_id`, and `scraped_at` let pipeline2/3 continue where they stopped. |
| Someone starts a scrape while a merge is pending or running | The startup check exits before anything is written to staging |
| Someone starts a merge while a scrape is running | Nothing happens: the row is `scraping`, not `pending` |
| A merge fails at a step | The row stays `running` until the next check marks it `failed` with that step. Steps already committed are kept. Scraping is not blocked. |
| Staging has leftover unmerged data (e.g. after a failed merge) when a scrape starts | Safe. pipeline2 treats staged rows as already captured. pipeline3 skips rows already scraped and retries rows that failed. Nothing is lost or duplicated. One side effect: a leftover *scraped* row whose stage moved up on GeM is merged at its older stage and corrected by the following month's run (one month of delay, no data loss). |
| A scrape and a merge overlap anyway | Prevented by the rules. If they did overlap: pipeline2's `UPDATE tenders` ([pipeline2/db.py:326](pipeline2/db.py#L326)) would wait hours on the merge's locks while the container keeps billing, and pipeline2 would compare staging against uncommitted state and redo work. |

### 1.9 Schedule and window

**Run monthly, re-crawling the last 6 months.** GeM bids keep changing after they close: buyers move them from technical to financial evaluation to award over several months. Live GeM counts taken on 2026-11-17, by month of bid end date:

| Bids that closed in | Age | Awarded | Have participant data (fin. evaluated + awarded) | Awarded, as % of final level |
|---|---|---:|---:|---:|
| 2026-10 | ~1 month | 30.5% | 58% | ~55% |
| 2026-09 | ~2 months | 42.7% | 69% | ~78% |
| 2026-08 | ~3 months | 48.4% | 74% | ~89% |
| 2026-07 | ~4 months | 51.9% | 76% | ~94% |
| 2026-05 / 06 | 5–6 months | 52–53% | 77% | ~93–94% |
| 2025 (settled) | 11+ months | 54–57% | 76–78% | 100% |

- Participant data is ~95% complete after about **3 months**. Awards (winners, win rates) take about **4–6 months**.
- Each re-crawl does two things: it finds bids that had **no participant data** at the previous crawl (they were still at "not evaluated" or "technical evaluation"), and it **upgrades captured bids** whose stage moved up (e.g. financial evaluation → awarded, filling in the winner).
- The window is therefore **M-6 … M-1** (the month that just ended plus the 5 before it). An optional twice-yearly sweep of months 7–12 back catches the last ~2–4% of late awards.
- Weekly runs add little: very new bids barely have participant data, and GeM rate-limits heavily (50–80% of requests rejected at peak during the analysis).

**Recommended schedule: self-healing.** Trigger the job daily on the **1st–3rd** of the month instead of only the 1st. The startup check exits in seconds if this month's run already exists. A run blocked on the 1st (e.g. by a stuck merge) then happens automatically on the 2nd or 3rd. The cron is in UTC: a start of 20:00 IST is `30 14` UTC.

### 1.10 Region, sizing and cost

- **Region: Central India.** The database resolves to Central India (Pune). Staging writes are constant during scraping, so the container should be in the same region. GeM is an Indian site.
- **Size:** start at 4 vCPU / 8 GiB (the Consumption plan maximum). The real limit on speed is GeM's rate limiting, not CPU, so more machines or parallel containers won't make it faster. Tune `NUM_WORKERS` / `PAGES_PER_BROWSER` against the 429 rate and memory use. Move to a dedicated workload profile only if 8 GiB proves too small after reducing workers.
- **Estimated duration** (extrapolated from one test day, to be confirmed): a new month ~4 h; re-crawling a month ~1 h of listing plus re-scrapes; the 6-month window ~10–12 h; the merge ~3–4 h (in the DB).

| Item | Monthly cost |
|---|---|
| Container Apps Job (~12 h × 4 vCPU / 8 GiB) | ~$0, inside the monthly free grant (180k vCPU-s / 360k GiB-s); about $5 per run if the grant is used elsewhere |
| Merge in the database | No extra charge; uses the server already paid for |
| pg_cron | Free |
| Container Registry (Basic) + Log Analytics | ~$6–8 |

**The database is where real cost or risk can appear:**
- **IOPS set to "Autoscale"** bills per I/O, and a long merge does a lot of I/O. "Pre-provisioned IOPS" has no extra charge.
- **Storage autogrow** can be triggered by the merge's temporary files and WAL. The grown disk can't be shrunk.
- **Backup storage** is free up to the provisioned storage size; check it once after the first runs.
- **Burstable (B-series) tier** doesn't cost more, but a long merge uses up CPU credits and slows the TenderGenie backend that shares this server.

---

## Part 2: Required changes

### 2.1 Database: new objects (one-time setup)

| Change | Notes |
|---|---|
| Enable **pg_cron** on `tg-postgres-server-prod` | Add it to the server parameters `azure.extensions` and `shared_preload_libraries`. **Requires one restart**, so do it in a maintenance window. Then create the extension in the `postgres` database. |
| New **`ops` schema** and **`ops.pipeline_runs`** table | As described in 1.5 |
| New **`ops.run_pending_merge()`** procedure | Lock, crash detection, pick `pending`, run steps with a commit after each, record status and step |
| New **pg_cron job** | Every 15 min, scheduled with `cron.schedule_in_database` against the pipeline database |
| **Run history housekeeping** | A monthly job that trims `cron.job_run_details`, or set `cron.log_run = off` |
| **Permissions** | The scraper's DB user: read/write on `ops.pipeline_runs`. The pg_cron job runs as the user who scheduled it, who must be able to run all pipeline4 objects. |
| **Server settings to check** | `statement_timeout` = 0 (otherwise a 4-hour merge is killed), IOPS not on Autoscale, storage autogrow, tier (see 1.10) |

### 2.2 pipeline4 SQL: from psql scripts to procedures

| File | Current state | Change needed |
|---|---|---|
| [00_run_all.sql](pipeline4/sql/00_run_all.sql) | Joins steps with psql's `\ir` and `BEGIN`/`COMMIT` | Becomes a thin wrapper that calls the procedure, so manual runs and the e2e tests keep working. Step order and transaction boundaries move into the procedure. |
| [01_merge.sql](pipeline4/sql/01_merge.sql) | 215 lines, 4 temporary tables, ~10 `SELECT`s | Wrap as a procedure. A `SELECT` that only displays results must become `PERFORM` or be removed (PL/pgSQL rejects a `SELECT` whose result goes nowhere). `INSERT … SELECT` stays as is. |
| [02_sellers_profile.sql](pipeline4/sql/02_sellers_profile.sql) | 165 lines, ~12 `SELECT`s | Same as 01 |
| [03_seller_competitors.sql](pipeline4/sql/03_seller_competitors.sql) | 304 lines, 5 temporary tables, a `DO` block with `RAISE` | Wrap it, and move the `DO` block's body directly into the procedure |
| 01 + 02 + 03 together | One transaction today | **Keep as one transaction**: one step with a single commit after 03 |
| [04_refresh_views.sql](pipeline4/sql/04_refresh_views.sql) | `REFRESH MATERIALIZED VIEW CONCURRENTLY` for `seller_categories` and `seller_buyers` | Its header says this "cannot run inside a transaction block". **Test it inside a procedure first.** If it fails, the refresh becomes a separate pg_cron command that runs when `merge_step = 'refresh'`. |
| [05_cleanup.sql](pipeline4/sql/05_cleanup.sql) | Has its own `BEGIN`/`COMMIT` | Remove them; the procedure commits after this step |

### 2.3 Validation SQL

| File | Change |
|---|---|
| [00_run_validate.sql](validation_sql/00_run_validate.sql) | Joins steps with `\ir` and `BEGIN`/`COMMIT`. Becomes the `validate` step with its own commit, kept separate from the merge as it is today. |
| [01_helpers.sql](validation_sql/01_helpers.sql) | Already defines a function. Create it once during setup, not on every run. |
| [02_schema.sql](validation_sql/02_schema.sql) | `CREATE TABLE` / `CREATE INDEX IF NOT EXISTS`. Move to a one-time setup script. |
| [03_validate_tenders.sql](validation_sql/03_validate_tenders.sql), [04_validate_participations.sql](validation_sql/04_validate_participations.sql), [06_require_participants.sql](validation_sql/06_require_participants.sql) | Wrap as procedures. `SELECT`s that only show results become `PERFORM`, or rows in a results table if the numbers should be kept. |
| [05_post_merge_checks.sql](validation_sql/05_post_merge_checks.sql) | **No change.** Not part of the automatic run; a manual check using `\echo` that stays a psql script. |

### 2.4 Python: entry point and startup check

| Change | Where | Why |
|---|---|---|
| **Startup check** | New entry script (or a mode of [pipeline_main/run.py](pipeline_main/run.py)) | Applies the rules in 1.6: exit on `pending`/`running`, resume a stuck `scraping` row, otherwise insert a new `scraping` row |
| **Mark `pending` at the end** | Same | Must happen even when some months partly failed. [run.py](pipeline_main/run.py#L248-L252) exits with code 1 on any failure, so the status update must happen before that exit. |
| **Compute the 6-month window** | Same | M-6 … M-1 from the current date, with an override (e.g. `RUN_MONTH`) for manual runs. `run.py` already accepts a month range, so it needs no change for this. |
| **"Already done this month?" check** | Same | Needed for the self-healing 1st–3rd schedule |
| **Worker counts from environment variables** | [pipeline2/config.py](pipeline2/config.py), [pipeline3/config.py](pipeline3/config.py) | `NUM_WORKERS` and `PAGES_PER_BROWSER` are hardcoded. Env vars allow tuning against GeM's 429s and container memory without rebuilding the image. |
| **Connection resilience** | [common/db.py](common/db.py) | One connection per process that reconnects only if it's already marked closed. A connection that drops mid-query during a 10-hour cloud run fails that write. Add TCP keepalives and reconnect-and-retry on connection errors. Use `sslmode=require` in the DSN. |

### 2.5 Container packaging

| Change | Notes |
|---|---|
| `Dockerfile` | Base image `mcr.microsoft.com/playwright/python` at **v1.59.0**, matching the `playwright==1.59.0` pin. `psql` is **not** needed in the image, since the merge runs in the DB. |
| `.dockerignore` | Exclude `venv/`, `.env`, `logs/`, `research/`, `*.log`, `__pycache__/`, so no secrets or large files end up in the image |
| Container requirements file | `pymongo` and `dnspython` are only used by older scripts (`backfill_derived.py`, `test/validate_data_consistency.py`). `rapidfuzz` and `openpyxl` are not imported by pipeline2/3/main. Use a trimmed requirements file for the image. |
| Configuration | `POSTGRES_DSN` comes from a Container Apps secret. The code already falls back to environment variables when there is no `.env` file. |

### 2.6 Azure resources (no code)

| Resource | Settings |
|---|---|
| Resource group, Container Registry (Basic) | Central India |
| Container Apps environment | Central India, Consumption plan |
| Container Apps Job | Schedule trigger (1st–3rd of the month, ~20:00 IST); replica timeout ~24 h; retry limit 1; parallelism 1; 4 vCPU / 8 GiB; DSN as a secret; worker counts as env vars |
| Database network access | Allow the job to reach the server: the "Allow Azure services" firewall rule (quick), or VNet integration with a private endpoint (safer) |
| Monitoring | Log Analytics with a daily cap; alert on failed job executions; database diagnostic logs to Log Analytics with an alert for pipeline4 errors; alert for a run stuck in `running` or marked `failed` |

### 2.7 Tests and documentation

| Change | Notes |
|---|---|
| [tests/e2e/run_e2e.py](tests/e2e/run_e2e.py) | Runs `psql -f 00_run_all.sql`; keeps working if 00_run_all becomes a wrapper. Add cases for the startup rules: scraping blocked by a `pending`/`running` merge, resuming a stuck `scraping` row, merge ignoring a `scraping` row. |
| [tests/e2e/setup_test_db.py](tests/e2e/setup_test_db.py) | The test database needs the `ops` schema and the procedures. Use pg_cron there too, or call the procedure directly. |
| [run.py docstring](pipeline_main/run.py#L21-L26), [DataExtractionPipeline.md](DataExtractionPipeline.md) | Currently say "run pipeline4 with psql after each month". Update to the new flow. |

### 2.8 What stays as it is

- **pipeline2/3 scraping logic.** Re-running a month already re-crawls it and fetches only what is new or changed; leftover staging rows are handled safely (1.8).
- **Crash recovery inside pipeline2/3.** `reset_in_progress_months()`, the permanent per-month `run_id`, and `scraped_at` already resume an interrupted scrape.
- **The SIGTERM handler** in `run.py`. It exits immediately; the stuck `scraping` row plus the existing resume logic covers that.
- **`validation_sql/05_post_merge_checks.sql`, `research/`, `test/`.** None of them are part of the scheduled run.

---

## Part 3: Order of work

### 3.1 Test these first

1. **`REFRESH MATERIALIZED VIEW CONCURRENTLY` inside a procedure.** Decides whether the refresh is an ordinary step or a separate pg_cron command.
2. **A procedure that commits after each step, run by pg_cron on Azure.** Check it on a test server.
3. **Real merge duration on full data** (expected 3–4 h). Sets the check interval, alert thresholds and job start time.

### 3.2 Implementation order

1. Create the `ops` schema and `ops.pipeline_runs`; convert pipeline4 and validation SQL into procedures; write `ops.run_pending_merge()`. Test on a copy or test database.
2. Enable pg_cron on the production server (restart in a maintenance window); create the polling job and the housekeeping job.
3. Python changes: entry script with the startup check, window calculation and status updates; env-configurable workers; connection keepalive and retry.
4. Container: Dockerfile, `.dockerignore`, trimmed requirements; build into the Container Registry.
5. Azure: Container Apps environment and Job, database access, monitoring and alerts.
6. One full end-to-end test on a single month: measure scrape time, memory, 429 rate and merge time. Then set the final timeout, size and start time.
7. Update tests and documentation.
