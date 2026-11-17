# End-to-end test: awarded + financially + technically evaluated bids

This runs the real pipelines (GeM listing, result scraping, validation, merge) for **one day of bids**. Everything is
written to a **separate test database**, `TenderIntelligence_test`, on the same server as production. Production is
only read: the schema is copied with `pg_dump --schema-only`, and the seed bids are copied with read-only queries.

```powershell
# unit tests (no network, no DB)
python -m unittest tests.test_extract_stage -v

# full end-to-end run (about 15–30 min: one day ≈ 2.5k listings + scrapes)
python tests/e2e/run_e2e.py --day 2025-07-15

# resume from a step after a failure
python tests/e2e/run_e2e.py --from-step p4
```

## Steps

| Step | What it does |
|---|---|
| `setup` | Creates `TenderIntelligence_test` with production's schema, then runs migrations 001 (stage columns) and 002 (seller views) |
| `seed` | Seeds three groups of known bids (below) and writes `seed_manifest.json` |
| `run1` | `pipeline_main/run.py <month> --days D-D` (pipeline2 + pipeline3) |
| `staged` | Checks after pipeline2 + pipeline3: listing ≥ 99% of GeM, cancelled counts = GeM `cancelled_bids`, duplicates pruned, seeds behaved, no contract fetch and no winner for stage 1/2 |
| `p4` | `psql -f pipeline4/sql/00_run_all.sql` |
| `merged` | Checks after merge: no duplicates, every tender has participants, no winners on stage 1/2, seeds (a)/(b)/(c), win rates use awarded bids only |
| `run2`, `rerun`, `p4b`, `merged2` | Same day again: nothing already stored is re-staged, and the database stays consistent |

## Seeded cases

| Case | Seeded as | Expected |
|---|---|---|
| (a) 5 awarded bids | stage 3, as in production | Pruned as duplicates; participants unchanged; `is_cancelled` filled in |
| (b) 5 awarded bids | stage 2, plus fake `TEST STALE SELLER` | Re-scraped; stage 3; fake row deleted, its profile zeroed, its competitor pairs removed |
| (c) 3 fin-cancelled bids | stage 2, not cancelled, plus fake `TEST KEEP SELLER` | Pruned; `is_cancelled` TRUE; not re-scraped; participant row kept |

Running against the real database: run migrations 001 and 002 once, then run each month with
`python pipeline_main/run.py YYYY-MM`, followed by pipeline4 **before** re-running that month.
