"""
Database layer for Pipeline2: PostgreSQL connection, staged tender storage, month status.

Tables:
  - staging.tenders_raw:  This run's listing rows, keyed (run_id, bid_id). Once a
                          month's capture completes, rows already in production at
                          the same stage are marked known_at (see complete_month /
                          mark_known_bid_ids). They stay in staging so a restarted
                          run sees the month as captured, are skipped by pipeline3
                          and validation, and are deleted by pipeline4's cleanup.
  - pipeline2_status:     Per-month multi-pass progress tracking (was pipeline2_status
                          collection). Also carries this month's run_id.
  - failed_pages:         Pages that failed to fetch (for retry/debugging).
  - failed_tenders:       Tenders that failed result/detail fetches.
  - error_logs:           All error events from pipeline2 and pipeline3.
"""
from datetime import datetime, timezone
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json, Jsonb

from common.db import get_connection, db_retry

_SCHEMA_READY = False


def _now():
    return datetime.now(timezone.utc)


def _ensure_schema(conn):
    """Create staging/status/log tables and indexes if they don't exist yet."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.tenders_raw (
                run_id                 UUID        NOT NULL,
                batch_key              TEXT        NOT NULL,
                bid_id                 BIGINT      NOT NULL,
                bid_id_parent          BIGINT,
                bid_number             TEXT,
                bid_number_parent      TEXT,
                ra_number              TEXT,
                bid_type               INT,
                eval_type              INT,
                single_packet          INT,
                buyer_status           INT,
                bid_to_ra              INT,
                ra_to_bid              INT,
                category               TEXT,
                items                  TEXT,
                category_id            TEXT,
                bid_title              TEXT,
                department             TEXT,
                ministry               TEXT,
                start_date             TEXT,
                end_date               TEXT,
                total_quantity         INT,
                is_high_value          BOOLEAN,
                is_rc_bid              INT,
                is_global_tendering    INT,
                is_bunch               INT,
                is_custom_item         INT,
                has_boq                BOOLEAN,
                tender_link            TEXT,
                month                  TEXT,
                buyer_details_raw      JSONB,
                contract_details_raw   JSONB,
                bid_result_raw         JSONB,
                ra_result_raw          JSONB,
                scraped_at             TIMESTAMPTZ,
                listing_scraped_at     TIMESTAMPTZ,
                validated              BOOLEAN     NOT NULL DEFAULT FALSE,
                validation_errors      JSONB,
                merged_at              TIMESTAMPTZ,
                PRIMARY KEY (run_id, bid_id)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_staging_tenders_raw_batch
                ON staging.tenders_raw (run_id, batch_key);
        """)
        # Bid stage / cancellation (see pipeline4/sql/migrations/001_bid_stage.sql):
        #   stage        1 tech / 2 fin / 3 awarded -- the byStatus list it came from
        #   is_cancelled GeM b_status in (3, 5)
        #   bid_status   GeM raw b_status
        cur.execute("""
            ALTER TABLE staging.tenders_raw
                ADD COLUMN IF NOT EXISTS stage        SMALLINT,
                ADD COLUMN IF NOT EXISTS is_cancelled BOOLEAN,
                ADD COLUMN IF NOT EXISTS bid_status   SMALLINT;
        """)
        # known_at: set by mark_known_bid_ids() when the bid is already in
        # production at the same or a higher stage (no scrape, no merge needed).
        cur.execute("ALTER TABLE staging.tenders_raw ADD COLUMN IF NOT EXISTS known_at TIMESTAMPTZ;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline2_status (
                month           TEXT        PRIMARY KEY,
                run_id          UUID        NOT NULL DEFAULT gen_random_uuid(),
                status          TEXT        NOT NULL DEFAULT 'pending',
                total_tenders   INT         NOT NULL DEFAULT 0,
                unique_captured INT         NOT NULL DEFAULT 0,
                passes_done     INT         NOT NULL DEFAULT 0,
                last_pass_new   INT         NOT NULL DEFAULT 0,
                error_message   TEXT,
                started_at      TIMESTAMPTZ,
                completed_at    TIMESTAMPTZ,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        # Per-status listing progress: {"fin_evaluated": {"num_found": N, "captured": M, "passes": P}, ...}
        cur.execute("ALTER TABLE pipeline2_status ADD COLUMN IF NOT EXISTS status_counts JSONB;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS failed_pages (
                id          BIGSERIAL   PRIMARY KEY,
                pipeline    TEXT        NOT NULL DEFAULT 'pipeline2',
                month       TEXT        NOT NULL,
                page_no     INT         NOT NULL,
                worker_id   INT,
                pass_num    INT         NOT NULL DEFAULT 0,
                error       TEXT,
                logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_pages_month ON failed_pages (month);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_pages_page_no ON failed_pages (page_no);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_pages_logged_at ON failed_pages (logged_at);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS failed_tenders (
                id          BIGSERIAL   PRIMARY KEY,
                pipeline    TEXT        NOT NULL,
                bid_id      BIGINT      NOT NULL,
                month       TEXT        NOT NULL,
                worker_id   INT,
                error       TEXT,
                retry_count INT         NOT NULL DEFAULT 0,
                resolved    BOOLEAN     NOT NULL DEFAULT FALSE,
                logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_tenders_bid_id ON failed_tenders (bid_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_tenders_month ON failed_tenders (month);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_tenders_pipeline ON failed_tenders (pipeline);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_failed_tenders_logged_at ON failed_tenders (logged_at);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS error_logs (
                id          BIGSERIAL   PRIMARY KEY,
                pipeline    TEXT        NOT NULL,
                month       TEXT        NOT NULL,
                worker_id   INT         NOT NULL DEFAULT 0,
                bid_id      BIGINT      NOT NULL DEFAULT 0,
                page_no     INT         NOT NULL DEFAULT 0,
                error       TEXT,
                context     TEXT,
                logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_error_logs_pipeline ON error_logs (pipeline);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_error_logs_month ON error_logs (month);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_error_logs_logged_at ON error_logs (logged_at);")

    _SCHEMA_READY = True


@db_retry
def create_indexes():
    """Ensure staging/status/log tables and indexes exist (was Mongo index setup)."""
    conn = get_connection()
    _ensure_schema(conn)


def _get_run_id(month: str):
    """This month's run_id, assigned once by init_month_status()."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT run_id FROM pipeline2_status WHERE month = %s", (month,))
        row = cur.fetchone()
        return row[0] if row else None


# ═══════════════════════════════════════════════════════════════
# Tender Storage (staged bulk upsert with dedup)
# ═══════════════════════════════════════════════════════════════

# Plain (non-JSON) staging columns, in the order they're written.
_STAGING_COLUMNS = [
    "bid_id", "bid_id_parent", "bid_number", "bid_number_parent", "ra_number",
    "bid_type", "eval_type", "single_packet", "buyer_status", "bid_to_ra", "ra_to_bid",
    "category", "items", "category_id", "bid_title", "department", "ministry",
    "start_date", "end_date", "total_quantity", "is_high_value", "is_rc_bid",
    "is_global_tendering", "is_bunch", "is_custom_item", "has_boq", "tender_link",
    "month", "scraped_at", "listing_scraped_at",
    "stage", "is_cancelled", "bid_status",
]

# doc key -> staging column, for the JSONB detail blobs (empty at pipeline2 time,
# filled in later by pipeline3, but the columns exist on this same staging row).
_JSON_DOC_TO_COLUMN = {
    "buyer_details": "buyer_details_raw",
    "contract_details": "contract_details_raw",
    "bid_result": "bid_result_raw",
    "ra_result": "ra_result_raw",
}

_INSERT_COLUMNS = _STAGING_COLUMNS + list(_JSON_DOC_TO_COLUMN.values())

# A bid repeated across pages of one list hits the (run_id, bid_id) key and is
# kept once. A bid seen in a LOWER list and then a HIGHER one during the same
# crawl (it moved up mid-crawl; lists are crawled tech -> fin -> awarded) gets
# its listing status raised -- but only while it is still unscraped, so a row
# pipeline3 already worked on is never silently changed underneath it.
# Raising the stage also clears known_at: the next mark_known_bid_ids() then
# compares the new stage with production, so pipeline3 scrapes it if it moved
# above the stored stage.
# RETURNING (xmax = 0) is TRUE only for a genuine insert, so updates aren't
# counted as "new".
_INSERT_SQL = f"""
    INSERT INTO staging.tenders_raw AS sr (run_id, batch_key, {", ".join(_INSERT_COLUMNS)})
    VALUES ({", ".join(["%s"] * (len(_INSERT_COLUMNS) + 2))})
    ON CONFLICT (run_id, bid_id) DO UPDATE SET
        stage              = EXCLUDED.stage,
        is_cancelled       = EXCLUDED.is_cancelled,
        bid_status         = EXCLUDED.bid_status,
        buyer_status       = EXCLUDED.buyer_status,
        listing_scraped_at = EXCLUDED.listing_scraped_at,
        known_at           = NULL
    WHERE EXCLUDED.stage > COALESCE(sr.stage, 0) AND sr.scraped_at IS NULL
    RETURNING (xmax = 0) AS inserted
"""


def _doc_to_row(run_id, month, doc):
    values = [run_id, month]
    values.extend(doc.get(col) for col in _STAGING_COLUMNS)
    for doc_key in _JSON_DOC_TO_COLUMN:
        val = doc.get(doc_key)
        values.append(Json(val) if val is not None else None)
    return values


# month -> run_id. A month's run_id is permanent once init_month_status() has
# created it, so each process (main or pipeline2 worker) looks it up only once.
_run_id_cache = {}


def _cached_run_id(month: str):
    """Like _get_run_id(), cached, and without the schema DDL: workers call this
    on every page, after the main process has already ensured the schema."""
    run_id = _run_id_cache.get(month)
    if run_id is None:
        with get_connection().cursor() as cur:
            cur.execute("SELECT run_id FROM pipeline2_status WHERE month = %s", (month,))
            row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"No pipeline2_status row for {month} (init_month_status not called)")
        run_id = _run_id_cache[month] = row[0]
    return run_id


@db_retry
def bulk_upsert_tenders(docs: list, month: str) -> int:
    """
    Upsert tender documents into staging.tenders_raw, all in one transaction
    (one round trip and one commit). pipeline2 workers call it once per fetched
    page (10 bids), so an interrupted pass loses at most the page in flight.
    Uses (run_id, bid_id) as the unique key -- see _INSERT_SQL for what a
    repeat does. Raises on failure (nothing of this call is saved then).
    Returns count of newly inserted rows.
    """
    if not docs:
        return 0

    run_id = _cached_run_id(month)
    rows = [_doc_to_row(run_id, month, doc) for doc in docs]
    conn = get_connection()
    newly = 0

    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(_INSERT_SQL, rows, returning=True)
        # One result set per row; empty when the conflict update was skipped.
        while True:
            row = cur.fetchone()
            if row and row[0]:
                newly += 1
            if not cur.nextset():
                break

    return newly


@db_retry
def get_month_bid_count(month: str) -> int:
    """Count of tenders currently staged for this month's run (known rows included)."""
    run_id = _get_run_id(month)
    if run_id is None:
        return 0
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM staging.tenders_raw WHERE run_id = %s AND batch_key = %s",
            (run_id, month),
        )
        return cur.fetchone()[0]


@db_retry
def get_staged_bid_ids(month: str, stage: int) -> set:
    """bid_ids already staged for this month's run at this stage (resume after a crash)."""
    run_id = _get_run_id(month)
    if run_id is None:
        return set()
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bid_id FROM staging.tenders_raw WHERE run_id = %s AND batch_key = %s AND stage = %s",
            (run_id, month, stage),
        )
        return {r[0] for r in cur.fetchall()}


@db_retry
def mark_known_bid_ids(month: str) -> tuple:
    """
    Compare this month's staged bids with production `tenders` (any year) and
    mark the ones that need no work with known_at. Called once capture
    finishes, from complete_month(). Returns (status_updated, marked).

    A staged bid stays unmarked (-> scraped by pipeline3 -> merged by pipeline4) when:
      - its bid_id is not in `tenders` yet (new, or previously rejected for
        having no valid participants), or
      - its stage moved up (1 -> 2 -> 3). Old rows with stage NULL were all
        fetched from the awarded list, so NULL counts as 3.

    A staged bid whose stage did NOT move up is marked known. Before that, if
    only its cancellation changed, `tenders.is_cancelled / bid_status /
    status_updated_at` are updated in place -- no re-scrape, because a cancelled
    bid's page often masks seller names and would overwrite real ones.
    Listings without a b_status (bid_status NULL) never overwrite a known value.

    Known rows are kept, not deleted, so that a restarted run still counts them
    as captured (pipeline2 then skips the lists it already has). pipeline3 and
    validation ignore them; pipeline4/sql/05_cleanup.sql deletes them after the
    merge. Safe to call again: already-marked rows are left as they are.
    """
    run_id = _get_run_id(month)
    if run_id is None:
        return 0, 0
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tenders t
            SET is_cancelled      = sr.is_cancelled,
                bid_status        = sr.bid_status,
                status_updated_at = now()
            FROM staging.tenders_raw sr
            WHERE sr.run_id = %s AND sr.batch_key = %s
              AND t.bid_id = sr.bid_id
              AND COALESCE(sr.stage, 3) <= COALESCE(t.stage, 3)
              AND sr.bid_status IS NOT NULL
              AND (sr.is_cancelled IS DISTINCT FROM t.is_cancelled
                   OR sr.bid_status IS DISTINCT FROM t.bid_status)
            """,
            (run_id, month),
        )
        status_updated = cur.rowcount

        cur.execute(
            """
            UPDATE staging.tenders_raw sr
            SET known_at = now()
            FROM tenders t
            WHERE sr.run_id = %s AND sr.batch_key = %s
              AND sr.bid_id = t.bid_id
              AND COALESCE(sr.stage, 3) <= COALESCE(t.stage, 3)
              AND sr.known_at IS NULL
            """,
            (run_id, month),
        )
        marked = cur.rowcount
    return status_updated, marked


# ═══════════════════════════════════════════════════════════════
# Error & Failure Logging
# ═══════════════════════════════════════════════════════════════

def log_failed_page(month: str, page_no: int, worker_id: int, error: str, pass_num: int = 0):
    """Log a failed page fetch to the failed_pages table."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO failed_pages (pipeline, month, page_no, worker_id, pass_num, error, logged_at)
            VALUES ('pipeline2', %s, %s, %s, %s, %s, %s)
            """,
            (month, page_no, worker_id, pass_num, error[:1000], _now()),
        )


def log_failed_tender(bid_id: int, month: str, worker_id: int, error: str, pipeline: str = "pipeline3"):
    """Log a failed tender result fetch to the failed_tenders table."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO failed_tenders (pipeline, bid_id, month, worker_id, error, retry_count, resolved, logged_at)
            VALUES (%s, %s, %s, %s, %s, 0, FALSE, %s)
            """,
            (pipeline, bid_id, month, worker_id, error[:1000], _now()),
        )


def log_error(pipeline: str, month: str, error: str, context: str = "",
              worker_id: int = 0, bid_id: int = 0, page_no: int = 0):
    """Log any error event to the error_logs table."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO error_logs (pipeline, month, worker_id, bid_id, page_no, error, context, logged_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (pipeline, month, worker_id, bid_id, page_no, error[:2000], context[:500], _now()),
        )


# ═══════════════════════════════════════════════════════════════
# Month Status Tracking
# ═══════════════════════════════════════════════════════════════

@db_retry
def init_month_status(month: str):
    """Initialize a month in pipeline2_status if not exists (also assigns its run_id)."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline2_status (month, status, updated_at)
            VALUES (%s, 'pending', %s)
            ON CONFLICT (month) DO NOTHING
            """,
            (month, _now()),
        )


@db_retry
def start_month(month: str):
    """Mark month as in_progress."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline2_status
            SET status = 'in_progress', started_at = %s, updated_at = %s
            WHERE month = %s
            """,
            (_now(), _now(), month),
        )


@db_retry
def update_month_progress(month: str, pass_num: int, unique_captured: int,
                          last_pass_new: int, total_tenders: int = 0):
    """Update progress after each pass."""
    conn = get_connection()
    with conn.cursor() as cur:
        if total_tenders > 0:
            cur.execute(
                """
                UPDATE pipeline2_status
                SET passes_done = %s, unique_captured = %s, last_pass_new = %s,
                    total_tenders = %s, updated_at = %s
                WHERE month = %s
                """,
                (pass_num, unique_captured, last_pass_new, total_tenders, _now(), month),
            )
        else:
            cur.execute(
                """
                UPDATE pipeline2_status
                SET passes_done = %s, unique_captured = %s, last_pass_new = %s, updated_at = %s
                WHERE month = %s
                """,
                (pass_num, unique_captured, last_pass_new, _now(), month),
            )


@db_retry
def complete_month(month: str, unique_captured: int, passes_done: int,
                   status_counts: Optional[dict] = None, total_tenders: Optional[int] = None):
    """
    Mark month as completed. Capture is finished at this point, so this is also
    where already-known bids are marked (see mark_known_bid_ids): pipeline3/4
    downstream only work on new bids and bids whose stage moved up.
    """
    status_updated, marked = mark_known_bid_ids(month)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline2_status
            SET status = 'completed', unique_captured = %s, passes_done = %s,
                total_tenders = COALESCE(%s, total_tenders),
                status_counts = COALESCE(%s, status_counts),
                completed_at = %s, updated_at = %s
            WHERE month = %s
            """,
            (unique_captured, passes_done, total_tenders,
             Jsonb(status_counts) if status_counts is not None else None,
             _now(), _now(), month),
        )

    if status_updated:
        print(f"  [db] Updated cancellation status of {status_updated} stored tender(s) for {month}.")
    if marked:
        print(f"  [db] Marked {marked} already-known bid_id(s) (stage unchanged) in staging for {month}; "
              f"pipeline3 skips them.")


@db_retry
def fail_month(month: str, error: str):
    """Mark month as failed."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline2_status
            SET status = 'failed', error_message = %s, updated_at = %s
            WHERE month = %s
            """,
            (error, _now(), month),
        )


@db_retry
def get_month_status(month: str) -> Optional[dict]:
    """Get current status for a month."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM pipeline2_status WHERE month = %s", (month,))
        return cur.fetchone()


@db_retry
def reset_in_progress_months():
    """
    Reset any months stuck in 'in_progress' back to 'pending'.
    Call this at startup to recover from mid-run crashes.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline2_status SET status = 'pending', updated_at = %s WHERE status = 'in_progress'",
            (_now(),),
        )
        return cur.rowcount


@db_retry
def get_next_pending_month() -> Optional[str]:
    """Get the next month that needs processing (oldest pending/failed first)."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT month FROM pipeline2_status
            WHERE status IN ('pending', 'failed')
            ORDER BY month ASC LIMIT 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


@db_retry
def get_all_month_statuses() -> list:
    """Get all month statuses for summary display."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM pipeline2_status ORDER BY month ASC")
        return cur.fetchall()
