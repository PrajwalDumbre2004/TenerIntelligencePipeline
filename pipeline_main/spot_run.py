"""Scheduled entry point for the Spot VM: pipeline2 + pipeline3 for the
monthly window, with the run's status kept in ops.pipeline_runs (see
pipeline4/sql/migrations/004_ops_runs.sql and azure_deployment_plan.md 1.5).

Every start (first start, or a restart after an eviction / crash) runs the
same flow, so resuming needs no saved state:

  1. Startup check against ops.pipeline_runs:
       this month's run already pending/running/done -> exit 0 (nothing to do)
       a merge is pending/running                    -> exit 75 (try later)
       a 'scraping' run older than 5 days            -> close it as pending
                                                        ('incomplete: ...'), exit
       a 'scraping' run                              -> resume it
       otherwise                                     -> new 'scraping' run for
                                                        the window M-6 .. M-1
  2. For each month of the run, in order: pipeline2, then pipeline3 (only if
     pipeline2 finished the month). Both pick up where they stopped: pipeline2
     skips lists already captured, pipeline3 scrapes only unscraped, unknown bids.
  3. Set the run 'pending' (the merge picks it up) and exit.

Exit codes, read by deploy/gem-runner.sh:
  0   done for this month (finished, or nothing to do) -> tag + scale to 0
  75  blocked until a merge finishes                   -> scale to 0, retried later
  other  crash (incl. a DB outage longer than the ~5 min retry) -> restarted

Usage:
    python pipeline_main/spot_run.py                          # window M-6 .. M-1
    python pipeline_main/spot_run.py --months 2026-03         # TEST: own month list
    python pipeline_main/spot_run.py --months 2026-03 --days 16-16   # TEST: one day
(--months / --days only apply when a new run is created; a resumed run keeps
its stored months.)
"""
import argparse
import asyncio
import os
import signal
import sys
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_connection, db_retry  # noqa: E402
from pipeline2.orchestrator import process_month as pipeline2_process_month, gem_date  # noqa: E402
from pipeline3.orchestrator import process_month as pipeline3_process_month  # noqa: E402
from pipeline2.db import (  # noqa: E402
    create_indexes as p2_create_indexes, init_month_status, reset_in_progress_months,
)

EXIT_DONE = 0
EXIT_WAIT_FOR_MERGE = 75
RUN_DEADLINE = timedelta(days=5)   # R5: after this, merge what was scraped
WINDOW_MONTHS = 6                  # M-6 .. M-1

# A DB outage that outlasted db_retry: crash, so gem-runner restarts the run,
# instead of marking every remaining month failed and the run 'pending'.
_DB_DOWN = (psycopg.OperationalError, psycopg.InterfaceError)


def _now():
    return datetime.now(timezone.utc)


def _month_of(ts) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m")


def _signal_handler(sig, frame):
    print("\n  [!] Shutdown signal — exiting (the next start resumes the run).")
    os._exit(1)


def default_window(today) -> list:
    """The WINDOW_MONTHS months before today's month, oldest first."""
    months = []
    for back in range(WINDOW_MONTHS, 0, -1):
        y, m = today.year, today.month - back
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y:04d}-{m:02d}")
    return months


# ═══════════════════════════════════════════════════════════════
# ops.pipeline_runs
# ═══════════════════════════════════════════════════════════════

_RUN_COLUMNS = "id, months, status, scrape_started"


@db_retry
def _this_month_finished_run(this_month: str):
    """This month's run, if it already finished scraping (pending/running/done)."""
    with get_connection().cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {_RUN_COLUMNS} FROM ops.pipeline_runs
            WHERE status IN ('pending', 'running', 'done')
              AND to_char(scrape_started AT TIME ZONE 'UTC', 'YYYY-MM') = %s
            ORDER BY id DESC LIMIT 1
            """,
            (this_month,),
        )
        return cur.fetchone()


@db_retry
def _active_run():
    """The run that is scraping, waiting for the merge, or merging (at most one)."""
    with get_connection().cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {_RUN_COLUMNS} FROM ops.pipeline_runs
            WHERE status IN ('scraping', 'pending', 'running')
            ORDER BY id DESC LIMIT 1
            """
        )
        return cur.fetchone()


@db_retry
def _create_run(months: list):
    with get_connection().cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"INSERT INTO ops.pipeline_runs (months, status) VALUES (%s, 'scraping') "
            f"RETURNING {_RUN_COLUMNS}",
            (months,),
        )
        return cur.fetchone()


@db_retry
def _unfinished_months(run) -> list:
    """Months of the run whose pipeline2 or pipeline3 did not complete during
    this run (completion flags from earlier runs don't count)."""
    with get_connection().cursor() as cur:
        cur.execute(
            """
            SELECT m.month
            FROM unnest(%s::text[]) WITH ORDINALITY AS m(month, ord)
            LEFT JOIN pipeline2_status s ON s.month = m.month
            WHERE s.completed_at IS NULL OR s.completed_at < %s
               OR s.p3_completed_at IS NULL OR s.p3_completed_at < %s
            ORDER BY m.ord
            """,
            (run["months"], run["scrape_started"], run["scrape_started"]),
        )
        return [r[0] for r in cur.fetchall()]


@db_retry
def _set_pending(run_id: int, error):
    with get_connection().cursor() as cur:
        cur.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = 'pending', scrape_finished = now(), error = %s
            WHERE id = %s AND status = 'scraping'
            """,
            (error, run_id),
        )
        return cur.rowcount


def _finish(run, error=None):
    """Hand the run to the merge and exit. Exit 0 if it is this month's run;
    75 if it started in an earlier month, so this month's run is started once
    that merge is done."""
    if not _set_pending(run["id"], error):
        print(f"  [!] Run #{run['id']} was no longer 'scraping'; status left as it is.")
    print(f"  Run #{run['id']} -> pending" + (f" ({error})" if error else ""))
    if _month_of(run["scrape_started"]) == _month_of(_now()):
        sys.exit(EXIT_DONE)
    print("  It started in an earlier month: this month's run starts after its merge.")
    sys.exit(EXIT_WAIT_FOR_MERGE)


def _check_deadline(run):
    """R5: a run still scraping after RUN_DEADLINE is closed with what it has."""
    if _now() - run["scrape_started"] <= RUN_DEADLINE:
        return
    unfinished = _unfinished_months(run)
    print(f"  [!] Run #{run['id']} passed its {RUN_DEADLINE.days}-day deadline.")
    _finish(run, f"incomplete: deadline passed; unfinished months: {', '.join(unfinished) or 'none'}")


def start_or_resume(months_override):
    """Startup check (see module docstring). Returns the run to work on, or exits."""
    this_month = _month_of(_now())

    done = _this_month_finished_run(this_month)
    if done:
        print(f"  This month's run #{done['id']} is already '{done['status']}'. Nothing to do.")
        sys.exit(EXIT_DONE)

    run = _active_run()
    if run and run["status"] in ("pending", "running"):
        print(f"  Run #{run['id']} is '{run['status']}' (merge waiting or in progress). Try later.")
        sys.exit(EXIT_WAIT_FOR_MERGE)

    if run:
        _check_deadline(run)
        print(f"  Resuming run #{run['id']} (started {run['scrape_started']:%Y-%m-%d %H:%M} UTC, "
              f"months {', '.join(run['months'])})")
        if months_override and list(months_override) != list(run["months"]):
            print("  [!] --months ignored: a resumed run keeps its own months.")
        return run

    run = _create_run(months_override or default_window(_now()))
    print(f"  Started run #{run['id']} for months {', '.join(run['months'])}")
    return run


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--months", nargs="+", help="TEST: months to scrape (YYYY-MM ...) for a new run")
    parser.add_argument("--days", help="TEST: day range inside a single month, e.g. 15-16")
    args = parser.parse_args()

    print("=" * 70)
    print("  SPOT RUN — Pipeline2 + Pipeline3")
    print("=" * 70)

    p2_create_indexes()
    reset = reset_in_progress_months()
    if reset:
        print(f"  [!] Reset {reset} in_progress month(s) to pending (previous start was interrupted)")

    run = start_or_resume(args.months)
    months = list(run["months"])

    window_from = window_to = None
    if args.days:
        if len(months) != 1:
            sys.exit("--days needs a run with exactly one month")
        y, m = int(months[0][:4]), int(months[0][5:7])
        d1, d2 = (int(x) for x in args.days.split("-"))
        window_from, window_to = gem_date(y, m, d1), gem_date(y, m, d2)
        print(f"  [TEST] Day window: {window_from} to {window_to}")

    problems = []
    for i, month in enumerate(months, 1):
        print(f"\n{'#' * 70}\n  MONTH {i}/{len(months)}: {month}\n{'#' * 70}")

        # pipeline2: list the month (skips lists already captured)
        p2_ok = False
        try:
            init_month_status(month)
            p2_ok = bool(pipeline2_process_month(month, window_from, window_to))
            if not p2_ok:
                problems.append(f"{month} pipeline2 did not finish")
        except _DB_DOWN:
            raise
        except Exception as e:
            print(f"  ✗ Pipeline2 FAILED: {type(e).__name__}: {str(e)[:200]}")
            problems.append(f"{month} pipeline2: {type(e).__name__}: {str(e)[:150]}")
        _check_deadline(run)

        # pipeline3: only once pipeline2 finished the month (known bids marked)
        if not p2_ok:
            print(f"  Pipeline3 skipped for {month} (pipeline2 did not finish it)")
            continue
        try:
            if not asyncio.run(pipeline3_process_month(month)):
                problems.append(f"{month} pipeline3 left tenders pending")
        except _DB_DOWN:
            raise
        except Exception as e:
            print(f"  ✗ Pipeline3 FAILED: {type(e).__name__}: {str(e)[:200]}")
            problems.append(f"{month} pipeline3: {type(e).__name__}: {str(e)[:150]}")
        _check_deadline(run)

    print(f"\n{'=' * 70}")
    if problems:
        print(f"  Finished with {len(problems)} problem(s):")
        for p in problems:
            print(f"    • {p}")
    else:
        print("  All months finished.")
    print(f"{'=' * 70}")

    # Partial results are still merged; what failed waits in staging for next month.
    _finish(run, ("finished with problems: " + "; ".join(problems))[:2000] if problems else None)


if __name__ == "__main__":
    main()
