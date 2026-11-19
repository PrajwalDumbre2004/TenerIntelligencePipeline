# One-time backfill (May 2018 → last month) on Azure Spot: DevOps plan

*Status: plan, nothing built yet. Written 2026-11-18. Numbers marked "est." are estimates (see section 13).*

## 1. Summary

Before the monthly schedule starts, the pipeline (pipeline2 + pipeline3, then the pipeline4 merge) must run once over **every month from 2018-05 to the last full month** (2026-10 if started in November 2026). That's **102 months**.

The plan reuses the Spot setup designed for the monthly run: one Spot VM in a Scale Set, started by the Logic App, resuming after evictions. The backfill is split into **8 batches** (one year or less each, newest first), with a merge after each batch. The core pipeline code (pipeline2, pipeline3, pipeline4 SQL) needs **no changes**. What does need changing:
- one database setting (a blocker today),
- a small mode switch in the entry point `spot_run.py`,
- a way for the VM to receive the batch's month list,
- the Azure setup itself.

Estimates:

| | Estimate |
|---|---|
| Bids to list (pipeline2) | ~5.4 million |
| Bids to scrape (pipeline3) | ~2.3 million |
| VM running time | ~220 h (~9 days); about 2–3 weeks end to end, with merges and gaps |
| VM cost | about $10–20 in total |
| Lasting cost | ~30–50 GB more database storage |

---

## 2. Why a backfill is needed: what production holds today

Checked read-only on `tg-postgres-server-prod` (database `TenderIntelligence`, PostgreSQL 18.6) on 2026-11-18:

| Finding | Detail | Consequence |
|---|---|---|
| Only **awarded** bids are stored | 1% sample of `tenders`: every row has `stage = 3` | Technically and financially evaluated bids (stages 1, 2) were never stored, for any month |
| Cancellation status unknown everywhere | `is_cancelled` is NULL on ~100% of rows | The backfill fills it in for every stored bid (pipeline2's "mark known" step updates it in place) |
| Recent months mostly missing | `tenders_2026`: ~159k rows; GeM lists ~497k awarded bids for 2026 | Most of 2026 must be scraped |
| Stored tenders per year | 2018: 47k · 2019: 139k · 2020: 173k · 2021: 328k · 2022: 448k · 2023: 626k · 2024: 595k · 2025: 589k · 2026: 159k (`reltuples`) | Basis for the size estimates in section 4 |
| The staging pipeline has **never run on prod** | No `staging` schema, no `pipeline2_status`, `error_logs`, `failed_pages`, `staging.bid_participations_*` | All of it is created automatically on the first run (pipeline2/pipeline3 and `validation_sql/02_schema.sql` create their tables) |
| `ops.pipeline_runs` | Exists (migration 004, applied 2026-11-18), 0 rows | Ready |
| Migrations 001, 002, 003 | Applied | Nothing to do |
| Partitions | `tenders_2018` … `tenders_2027` + default; same for `bid_participations` | Every backfill year has a partition, so the merge cannot fail on a missing one |
| Extensions | Only `plpgsql` installed; `azure.extensions` allow-list = `VECTOR, PG_TRGM` | **Blocker**, see C1 in section 7 |
| Database size | 63 GB (largest: `seller_competitors` 22 GB, `seller_buyers` 6.3 GB, `seller_categories` 5.7 GB) | Storage and merge time grow, see section 9 |

---

## 3. What the backfill does per month

Exactly what the pipeline already does (no change):

1. **pipeline2** lists the month's bids from GeM's three lists: technically evaluated, financially evaluated and awarded, including cancelled ones. Every listed bid is written to `staging.tenders_raw`.
2. When the month is complete, bids already in `tenders` at the same stage are **marked `known_at`** and not scraped. Their cancellation status is updated in `tenders` in place. In the backfill this covers almost all stored awarded bids.
3. **pipeline3** scrapes result pages only for the bids left over: new technically and financially evaluated bids, plus awarded bids missing from `tenders`. It then derives participations.
4. **pipeline4** (the merge) validates and merges them into `tenders` / `bid_participations`, updates seller profiles and competitors, refreshes the views, and cleans staging.

Restarts resume from the database: lists already captured are skipped, and scraped bids are not scraped again (tested, 42/42 checks, `tests/e2e/run_resilience.py`).

---

## 4. Size and time estimates

### Volumes

| Period | Bids listed by pipeline2 | Bids to scrape (pipeline3) | Basis |
|---|---:|---:|---|
| 2026-01 … 2026-10 | 843k | ~684k | GeM snapshot 2026-11-17 minus the ~159k stored |
| 2025 | 913k | ~323k | GeM snapshot 2026-11-13: fin 229k + tech 89k + ~6k awarded not stored |
| 2018-05 … 2024 | ~3.61M est. | ~1.26M est. | No GeM snapshot. Stored awarded × 1.53, since 2025 has 0.53 fin+tech bids per awarded bid |
| **Total** | **~5.4M** | **~2.3M** | |

### Speed (measured earlier on this pipeline)
- **Listing:** ~13.5 bids/s (memory test B: a full month listed in ~1 h 45 min).
- **Scraping:** ~0.17 s per bid with the default 10 browsers × 3 pages (test D, full month, ~15% of requests got HTTP 429 and were retried).

**Running time ≈ 5.4M / 13.5 + 2.3M × 0.17 s ≈ 110 h + 107 h ≈ 220 h ≈ 9 days**, plus ~5–10% lost to evictions and VM boots. Older months may list faster or slower; batch B0 (section 5) measures it.

---

## 5. Approach: batches, newest first, merge after each

### Why batches, not one 102-month run

| One run over all 102 months | Batches of ≤ 12 months (chosen) |
|---|---|
| Staging holds everything at once (~2.3M scraped bids with their result JSON, tens of GB) on the production server | Staging holds one batch, then cleanup empties it |
| One giant merge at the end: 2.3M tenders and ~40M participation rows in a single transaction, hours of locks on tables the TenderGenie backend reads | ~8 normal-sized merges, each schedulable at night |
| Nothing visible for 2+ weeks | Each batch is visible right after its merge |
| Hits the 5-day deadline (R5) and would need it disabled | Each batch fits well within 5 days |

### Why newest first
Recent months are what users look at most, and 2026 is the most incomplete. Doing them first delivers the most value earliest. Older years follow.

### Batch plan

| Batch | Months | Listed (est.) | To scrape (est.) | VM time (est.) |
|---|---|---:|---:|---:|
| B0 pilot | 2018-05 | ~9k | ~3k | ~1 h |
| B1 | 2026-06 … 2026-10 | 451k | ~400k | ~28 h |
| B2 | 2026-01 … 2026-05 | 392k | ~285k | ~22 h |
| B3 | 2025-01 … 2025-12 | 913k | ~323k | ~34 h |
| B4 | 2024-01 … 2024-12 | ~912k | ~317k | ~34 h |
| B5 | 2023-01 … 2023-12 | ~959k | ~334k | ~36 h |
| B6 | 2022-01 … 2022-12 | ~686k | ~239k | ~25 h |
| B7 | 2021-01 … 2021-12 | ~503k | ~175k | ~19 h |
| B8 | 2018-06 … 2020-12 | ~540k | ~188k | ~20 h |

- **B0 is a pilot.** It's the smallest month and runs end to end on production, including the merge. It proves the setup (extension, permissions, first-time table creation) and measures the merge time against the real 22 GB seller tables before the big batches.
- **2026 is split in two** so neither half comes near the 5-day deadline even if GeM rate-limits heavily.
- If B0's merge is fast, adjacent batches can be merged together. If it's slow, keep the batches as listed.

---

## 6. How a batch runs

```
operator: set VMSS tags  run_args = "--backfill --months 2025-01 … 2025-12"
                         cycle_done = backfill
            │
Logic App (hourly) or operator: capacity 1
            │
Spot VM boots → cloud-init → gem-runner.sh reads run_args → spot_run.py --backfill --months …
            │      evicted? → Try & restore / Logic App restarts it → resumes from the DB
            ▼
run row 'pending', exit 0 → gem-runner tags cycle_done=<current month>, capacity 0 (VM deleted)
            │
merge (pg_cron, or by hand at night) → row 'done'
            │
operator: next batch's run_args + cycle_done = backfill
```

The `cycle_done = backfill` value doesn't start with any month, so the Logic App's existing rule ("start if the tag does not start with this month and capacity is 0") keeps restarting the VM after evictions while a batch is in progress. When the batch ends, the runner sets the tag to the current month, which stops the Logic App. **The Logic App needs no change.**

If the next batch is queued before the previous merge has finished, `spot_run` exits 75. The VM switches off without a tag, and the Logic App retries hourly. **The next batch therefore starts on its own as soon as the merge is done.** The operator can queue it right after scraping ends.

### Operator runbook, per batch
1. Set the two VMSS tags (portal → Scale Set → Tags, or `az vmss update --set tags.run_args="…" tags.cycle_done=backfill`).
2. Wait for the Logic App (≤ 1 h), or set capacity to 1.
3. Watch progress (section 11). Evictions need no action.
4. When the run row is `pending`: run the merge (section 8), check the results (section 11), set the row to `done` if done by hand.
5. Go to the next batch.

---

## 7. Changes required

The core pipeline (pipeline2, pipeline3, pipeline4 SQL, validation SQL) needs **no change**.

| # | Change | Where | Type | Blocker? |
|---|---|---|---|---|
| C1 | Add `PGCRYPTO` to the server parameter `azure.extensions`, **or** delete `CREATE EXTENSION IF NOT EXISTS pgcrypto` from `pipeline2/db.py`. `gen_random_uuid()` is built into PostgreSQL since v13, so pgcrypto isn't actually needed. | Azure DB server parameters (dynamic, no restart), or one line of code | DB setting | **Yes, for every run on prod, monthly included.** Today the first run would fail creating its tables. |
| C2 | A backfill mode in `spot_run.py`: `--backfill` (a) skips the startup rule "this month's run is already done → exit 0", and (b) stores the run with `kind = 'backfill'`. The monthly rule then only counts scheduled runs. Needs migration 005: `ALTER TABLE ops.pipeline_runs ADD COLUMN kind TEXT NOT NULL DEFAULT 'scheduled'`. | Entry point (orchestration only) + migration | Small code change | **Yes.** Without (a), batch 2 exits immediately when started in the same calendar month as batch 1. Without (b), the first monthly run after the backfill is skipped. |
| C3 | `gem-runner.sh` reads the VMSS tag `run_args` (`az vmss show --query tags.run_args`; the identity already has the rights) and passes it to the container. Empty means the normal monthly run. | Deploy script | Small script change | **Yes.** Without it the VM always runs the default 6-month window. |
| C4 | Part C of the Spot plan: `deploy/cloud-init.yaml`, `deploy/azure/setup.sh` (Scale Set, identity + role, Logic App), image in the registry, DB firewall "Allow Azure services" | `deploy/` | Deploy work | Yes (also needed for the monthly run) |
| C5 | Merge after each batch: pg_cron + `ops.run_pending_merge()` (deploy plan 2.1–2.3), **or** manual merges (section 8) | DB / runbook | DB work or manual steps | No (manual works) |
| C6 | *Optional:* run deadline from an env var (`RUN_DEADLINE_DAYS`, default 5) | `spot_run.py` | Tiny code change | No (batches are sized to fit 5 days) |
| C7 | *Optional:* a batch queue table (`ops.backfill_batches`) that `spot_run --backfill` takes the next batch from, so no tag edits between batches | `spot_run.py` + migration | Code change | No. Only worth it together with pg_cron, for a fully hands-off backfill. |

---

## 8. The merge for each batch

| Option | How | Pros | Cons |
|---|---|---|---|
| **A. pg_cron (recommended if done first)** | Deploy plan 2.1–2.3: pg_cron on the server (one restart), the merge as a procedure, a job every 15 min | Fully automatic; nothing depends on a client connection; also needed for the monthly schedule | Most work; needs a restart window |
| **B. Manual from a workstation** | `psql "$PROD_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql`, then `UPDATE ops.pipeline_runs SET status='done', merge_finished=now() WHERE id=…` | No new work | A person per batch (~9 times). A dropped connection rolls back the merge transaction (safe, but start again). Keep the laptop awake and online for hours. |

With option B:
- **Run it at night (IST).** The merge's single transaction over `tenders`, `bid_participations` and the seller tables competes with the TenderGenie backend.
- **Set `statement_timeout = 0`** for the session if the server has a default timeout.

**B0 measures how long one merge takes on production.** The repo's estimate of 3–4 h was for a 6-month window and is unconfirmed.

---

## 9. Database preparation and impact

| Topic | What to do / expect |
|---|---|
| **Storage growth** | est. +30–50 GB, lasting: ~2.3M tenders (~8 GB at today's ~3.5 KB/row), ~40M participation rows (~20 GB at ~0.5 KB/row), plus growth of the seller aggregate tables. The DB grows from 63 GB to roughly 95–115 GB. **Check the provisioned storage and autogrow before B1**; Azure storage can grow but never shrink. |
| Staging (temporary) | Up to ~1M listing rows + ~400k scraped rows with result JSON per batch, emptied by each merge's cleanup. Space is reused by later batches after autovacuum. |
| IOPS billing | If the server's IOPS are on "Autoscale" (billed per I/O), merges and the scraping writes cost extra. Pre-provisioned IOPS have no extra charge. |
| Writes to `tenders` during scraping | Each month's "mark known" step updates the cancellation status of the month's stored bids (up to ~80k rows in one statement), about 2.4M rows over the whole backfill. Short row locks only. |
| Backups | Note the point-in-time-restore window and the time just before B0's merge, so a bad merge can be rolled back. |
| Merge vs backend | Each merge holds locks for its duration. Schedule merges at night (option B) or schedule the pg_cron job accordingly. |
| Firewall | "Allow public access from any Azure service" so the VM can connect (as for the monthly run). |

---

## 10. Evictions and failures during the backfill

Same mechanisms as the monthly run (`azure_deployment_plan.md`, Spot plan R1–R5):

| Event | Handling | Operator action |
|---|---|---|
| Spot eviction | Try & restore recreates the VM, or the Logic App does within an hour. The batch resumes from the DB, losing at most one listing page or a few result pages. | None |
| DB blip | The app retries for ~5 min | None |
| Crash (bug, DB down for long) | 3 retries per VM, then tag `<month>-failed`, capacity 0 | Fix, set `cycle_done=backfill`, capacity 1. The batch resumes. |
| GeM rate limiting | Failed pages and tenders are retried in later passes (seen in the resilience test) | If the 429 rate stays high, lower `P3_NUM_WORKERS` / `P3_PAGES_PER_BROWSER` |
| Batch takes over 5 days | R5 closes it as `pending` with `incomplete: …` and what was scraped is merged | Re-queue the same batch. Finished lists and scraped bids are skipped. |
| A month fails inside a batch (e.g. GeM's count request fails) | Recorded in the run's `error`; the other months continue | Re-run that month in a later small batch |

---

## 11. Monitoring and checks

All in the database, so nothing is lost when an evicted VM is deleted:

```sql
-- runs
SELECT id, kind, status, months[1] AS first, months[array_length(months,1)] AS last,
       scrape_started, scrape_finished, error
FROM ops.pipeline_runs ORDER BY id DESC LIMIT 10;

-- progress per month of the current batch
SELECT month, status AS p2, p3_status AS p3, total_tenders AS gem_listed,
       unique_captured AS staged, p3_done, p3_pending, completed_at, p3_completed_at
FROM pipeline2_status WHERE month BETWEEN '2025-01' AND '2025-12' ORDER BY month;

-- staging state of the batch
SELECT batch_key, count(*) total,
       count(*) FILTER (WHERE known_at IS NOT NULL)                       AS known,
       count(*) FILTER (WHERE scraped_at IS NOT NULL)                     AS scraped,
       count(*) FILTER (WHERE known_at IS NULL AND scraped_at IS NULL)    AS to_scrape
FROM staging.tenders_raw GROUP BY 1 ORDER BY 1;

-- errors in the last day
SELECT pipeline, count(*) FROM error_logs WHERE logged_at > now() - interval '1 day' GROUP BY 1;
```

After each merge: compare `tenders` per month and stage against GeM's numbers in `pipeline2_status.status_counts`. The share stored should be close to the share with participant data.

Optional: Log Analytics with a small daily cap for the backfill period, if VM logs are wanted after evictions.

---

## 12. Cost (est.)

| Item | Estimate | Note |
|---|---|---|
| Spot VM `Standard_D2ads_v5` | ~240 h × $0.02–0.05/h ≈ **$5–12** | Check the current Central India Spot price in the portal |
| Public IP (per VM) | ~240 h × ~$0.005/h ≈ $1–2 | |
| Logic App | cents | |
| Registry | $0 (Docker Hub) or ~$5/month (ACR) | |
| **One-time total** | **~$10–20** | |
| Database storage | +30–50 GB, lasting | At the server's per-GB storage price; the main lasting cost |
| Database I/O | Only if IOPS are on Autoscale | Pre-provisioned: no extra |

Making it faster costs little more but isn't possible: GeM's rate limiting, not the VM, sets the speed. A second VM in parallel would double the 429s, and the run status board allows only one active run.

---

## 13. Assumptions behind the estimates

- 2025 and 2026 counts come from GeM snapshots (`research/data/counts_2025.json`, 2026-11-13; `counts_2026.json`, 2026-11-17). Counts move as bids progress.
- No GeM snapshot exists for 2018–2024. Their listed and to-scrape counts assume the same fin+tech-to-awarded ratio as 2025 (0.53) and that stored awarded ≈ GeM awarded. Old years may have a different ratio.
- Listing and scraping speeds come from earlier memory tests B and D on recent months, with GeM 429s included.
- Storage per row is taken from today's 2025 partitions.
- Batch B0 replaces these assumptions with measurements: listing speed for an old month, scrape speed, and merge time.

---

## 14. After the backfill: switching to the monthly schedule

1. After the last batch's merge, remove the `run_args` tag.
2. If the backfill covered every month up to last month, set `cycle_done` to the **current** month. The Logic App then starts the first scheduled run on the next 1st (window M-6 … M-1). Otherwise, remove `cycle_done`, and the scheduled run starts within the hour.
3. The scheduled window overlaps the backfill's newest months. That's intended and cheap: those lists are re-listed, stored bids are only marked known, and only bids whose stage moved are re-scraped.

---

## 15. Pre-flight checklist

- [ ] C1: `PGCRYPTO` allow-listed (or the `CREATE EXTENSION` line removed)
- [ ] C2, C3 built and tested on the test DB (extend `tests/e2e/run_resilience.py` with a two-batch case)
- [ ] C4: Scale Set, Logic App, identity and role, registry image, DB firewall rule
- [ ] Merge method chosen (pg_cron or manual) and, for manual, a night window agreed
- [ ] DB storage headroom ≥ 50 GB free or autogrow on; IOPS model checked
- [ ] Spot vCPU quota in Central India ≥ 2
- [ ] Restore point noted before B0's merge
- [ ] B0 run end to end; batch sizes re-checked against its timings

## 16. Decisions needed

- Merge method: build pg_cron first, or merge by hand per batch?
- C1: allow-list `PGCRYPTO` (DevOps) or remove the line (code)?
- Acceptable lasting DB growth (~30–50 GB) and the storage tier
- Who runs the per-batch steps, and when merges may run
- Start date. The backfill's last month is the month before it starts; anything newer is covered by the first scheduled run.
