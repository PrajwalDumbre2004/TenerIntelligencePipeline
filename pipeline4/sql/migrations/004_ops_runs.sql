-- pipeline4/sql/migrations/004_ops_runs.sql
--
-- One-time migration: the run status board shared by the scraper
-- (pipeline_main/spot_run.py) and the merge (pg_cron, see
-- azure_deployment_plan.md 1.5). One row per monthly run.
--
--   status: scraping -> pending -> running -> done | failed
--     scraping  set by spot_run.py when a run starts; a restarted VM resumes it
--     pending   set by spot_run.py when scraping ends (also when the 5-day
--               deadline closes it early; error then says 'incomplete: ...')
--     running / done / failed   set by the merge
--
-- Re-running is a no-op.
--
-- Usage:  psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/migrations/004_ops_runs.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id              BIGSERIAL   PRIMARY KEY,
    months          TEXT[]      NOT NULL,      -- window scraped, e.g. {2026-05,...,2026-10}
    status          TEXT        NOT NULL DEFAULT 'scraping'
                    CHECK (status IN ('scraping', 'pending', 'running', 'done', 'failed')),
    scrape_started  TIMESTAMPTZ NOT NULL DEFAULT now(),
    scrape_finished TIMESTAMPTZ,
    merge_started   TIMESTAMPTZ,
    merge_finished  TIMESTAMPTZ,
    merge_step      TEXT,                      -- validate / merge / refresh / cleanup
    error           TEXT
);

-- At most one active run: scraping and merging never overlap, and two scrapers
-- (e.g. a manual run while the VM is up) can't both start one.
CREATE UNIQUE INDEX IF NOT EXISTS ux_pipeline_runs_one_active
    ON ops.pipeline_runs ((TRUE))
    WHERE status IN ('scraping', 'pending', 'running');

COMMIT;
