"""
Database layer for Pipeline3: reads staged tenders from Postgres, enriches them in place.

Reads/writes the same staging.tenders_raw rows pipeline2 wrote (keyed on the month's
run_id from pipeline2_status), plus this pipeline's own progress columns (p3_*) added
onto the shared pipeline2_status table. Reuses pipeline2's schema setup and error
logging rather than duplicating it.
"""
from datetime import datetime, timezone

from psycopg.rows import dict_row
from psycopg.types.json import Json

from common.db import get_connection, db_retry
from pipeline2.db import create_indexes as _ensure_base_schema, log_failed_tender, log_error
from pipeline3.config import DERIVE_BATCH_SIZE

_P3_SCHEMA_READY = False


def _now():
    return datetime.now(timezone.utc)


def _ensure_schema(conn):
    """Extend the shared staging/status tables with pipeline3-specific columns."""
    global _P3_SCHEMA_READY
    if _P3_SCHEMA_READY:
        return

    _ensure_base_schema()

    with conn.cursor() as cur:
        cur.execute("ALTER TABLE staging.tenders_raw ADD COLUMN IF NOT EXISTS scrape_error TEXT;")
        cur.execute("ALTER TABLE staging.tenders_raw ADD COLUMN IF NOT EXISTS derived_at TIMESTAMPTZ;")
        cur.execute("""
            ALTER TABLE pipeline2_status
                ADD COLUMN IF NOT EXISTS p3_status       TEXT        NOT NULL DEFAULT 'pending',
                ADD COLUMN IF NOT EXISTS p3_pending       INT         NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS p3_done          INT         NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS p3_total         INT         NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS p3_passes        INT         NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS p3_updated_at    TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS p3_completed_at  TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS p3_error         TEXT;
        """)

        # Rows not yet flattened into participations: scraped_at IS NOT NULL AND derived_at IS NULL.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_staging_tenders_raw_pending_derive
                ON staging.tenders_raw (run_id, batch_key)
                WHERE scraped_at IS NOT NULL AND derived_at IS NULL;
        """)

        # ─────────────────────────────────────────────────────────────────
        # staging.bid_participations_raw -- one row per seller x tender,
        # produced by pipeline3.derive right after a tender is scraped.
        # Shape mirrors the final `bid_participations` table (per-participant
        # columns only -- category/buyer_name/etc. stay on staging.tenders_raw,
        # merged into `tenders` directly, not duplicated here).
        # ─────────────────────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.bid_participations_raw (
                run_id              UUID        NOT NULL,
                batch_key           TEXT        NOT NULL,
                bid_id              BIGINT      NOT NULL,
                seller_id           TEXT        NOT NULL,
                seller_name         TEXT        NOT NULL DEFAULT '',
                mse_status          TEXT        NOT NULL DEFAULT '',
                rank                TEXT        NOT NULL DEFAULT '',
                rank_num            SMALLINT,
                total_price         NUMERIC(20,2),
                is_winner           BOOLEAN     NOT NULL DEFAULT FALSE,
                winner_id           TEXT        NOT NULL DEFAULT '',
                winner_name         TEXT        NOT NULL DEFAULT '',
                winner_price        NUMERIC(20,2),
                technical_status    TEXT        NOT NULL DEFAULT '',
                outcome             TEXT        NOT NULL DEFAULT 'no_result',
                end_date            TEXT,
                derived_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (run_id, bid_id, seller_id)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_staging_bid_participations_raw_batch
                ON staging.bid_participations_raw (run_id, batch_key);
        """)

    _P3_SCHEMA_READY = True


def _get_run_id(month: str):
    """This month's run_id, assigned by pipeline2's init_month_status()."""
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT run_id FROM pipeline2_status WHERE month = %s", (month,))
        row = cur.fetchone()
        return row[0] if row else None


# ═══════════════════════════════════════════════════════════════
# Pipeline3 Status Tracking (p3_* columns on pipeline2_status)
# ═══════════════════════════════════════════════════════════════

@db_retry
def update_p3_status(month: str, status: str, pending: int = 0, done: int = 0,
                     total: int = 0, passes: int = 0, error: str = ""):
    """Update pipeline3 status fields on the shared pipeline2_status row."""
    conn = get_connection()
    _ensure_schema(conn)
    completed_at = _now() if status == "completed" else None
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline2_status
            SET p3_status = %s, p3_pending = %s, p3_done = %s, p3_total = %s,
                p3_passes = %s, p3_updated_at = %s,
                p3_completed_at = COALESCE(%s, p3_completed_at),
                p3_error = CASE WHEN %s <> '' THEN %s ELSE p3_error END
            WHERE month = %s
            """,
            (status, pending, done, total, passes, _now(), completed_at, error, error, month),
        )


@db_retry
def get_p3_status(month: str) -> dict:
    """Get pipeline3 status for a month."""
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT p3_status AS status, p3_pending AS pending, p3_done AS done,
                   p3_total AS total, p3_passes AS passes
            FROM pipeline2_status WHERE month = %s
            """,
            (month,),
        )
        row = cur.fetchone()
        return row if row else {}


# ═══════════════════════════════════════════════════════════════
# Staged Tender Read / Update
# ═══════════════════════════════════════════════════════════════

_NEEDS_RESULTS_COLUMNS = [
    "bid_id", "bid_number", "bid_id_parent", "bid_type", "eval_type",
    "single_packet", "bid_to_ra", "ra_to_bid", "is_bunch", "has_boq",
    "stage",   # 1 tech / 2 fin / 3 awarded -- contract page is only fetched for 3
]


@db_retry
def get_tenders_needing_results(month: str) -> list:
    """
    Get all tenders staged for this month's run that don't have results yet.
    Bids marked known_at (already in production at the same stage) are skipped.
    Returns list of dicts with fields needed to determine result URLs.
    """
    run_id = _get_run_id(month)
    if run_id is None:
        return []
    conn = get_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {", ".join(_NEEDS_RESULTS_COLUMNS)}
            FROM staging.tenders_raw
            WHERE run_id = %s AND batch_key = %s AND scraped_at IS NULL
              AND known_at IS NULL
            ORDER BY bid_id ASC
            """,
            (run_id, month),
        )
        return cur.fetchall()


@db_retry
def get_month_result_stats(month: str) -> dict:
    """Get counts of processed vs unprocessed staged tenders for a month
    (known rows excluded: pipeline3 never scrapes them)."""
    run_id = _get_run_id(month)
    if run_id is None:
        return {"total": 0, "done": 0, "pending": 0}
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE scraped_at IS NOT NULL)
            FROM staging.tenders_raw
            WHERE run_id = %s AND batch_key = %s AND known_at IS NULL
            """,
            (run_id, month),
        )
        total, done = cur.fetchone()
        return {"total": total, "done": done, "pending": total - done}


@db_retry
def update_tender_results(bid_id: int, month: str, results: dict) -> bool:
    """
    Update a staged tender row with scraped results.
    Sets buyer_details_raw, bid_result_raw, ra_result_raw, ra_number,
    contract_details_raw, and scraped_at. Clears scrape_error, so a tender that
    failed on an earlier attempt and succeeded now isn't blocked by validation.
    """
    run_id = _get_run_id(month)
    if run_id is None:
        return False
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE staging.tenders_raw
            SET buyer_details_raw = %s, bid_result_raw = %s, ra_result_raw = %s,
                ra_number = %s, contract_details_raw = %s, scraped_at = %s,
                scrape_error = NULL
            WHERE run_id = %s AND bid_id = %s
            """,
            (
                Json(results["buyer_details"]) if results.get("buyer_details") is not None else None,
                Json(results["bid_result"]) if results.get("bid_result") is not None else None,
                Json(results["ra_result"]) if results.get("ra_result") is not None else None,
                results.get("ra_number", ""),
                Json(results["contract_details"]) if results.get("contract_details") is not None else None,
                _now(),
                run_id, bid_id,
            ),
        )
        return cur.rowcount > 0


@db_retry
def mark_tender_failed(bid_id: int, error: str, month: str = "", worker_id: int = 0):
    """Record a failed scrape on the staged tender (scrape_error only).
    scraped_at is deliberately left NULL so the tender stays pending: the next
    pipeline3 pass (or a later re-run of the month) retries it, and validation
    refuses to merge it until a scrape actually succeeds.
    Also logs to failed_tenders and error_logs tables via pipeline2's helpers."""
    if month:
        run_id = _get_run_id(month)
        if run_id is not None:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE staging.tenders_raw
                    SET scrape_error = %s
                    WHERE run_id = %s AND bid_id = %s
                    """,
                    (error, run_id, bid_id),
                )

    log_failed_tender(bid_id, month, worker_id, error, pipeline="pipeline3")
    log_error("pipeline3", month, error, context="tender result extraction failed",
              worker_id=worker_id, bid_id=bid_id)


# ═══════════════════════════════════════════════════════════════
# Derivation: flatten scraped tenders into staging.bid_participations_raw
# ═══════════════════════════════════════════════════════════════

_DERIVE_SOURCE_COLUMNS = [
    "bid_id", "bid_id_parent", "bid_number", "bid_number_parent", "ra_number",
    "category", "department", "ministry", "month", "end_date",
    "buyer_details_raw", "bid_result_raw", "ra_result_raw",
    "stage",   # winners are only marked on awarded (stage 3) bids
]


@db_retry
def get_tenders_needing_derivation(month: str, batch_size: int = DERIVE_BATCH_SIZE) -> list:
    """
    Fetch staged tenders that have been scraped but not yet flattened into
    participations: scraped_at IS NOT NULL AND derived_at IS NULL.
    """
    run_id = _get_run_id(month)
    if run_id is None:
        return []
    conn = get_connection()
    _ensure_schema(conn)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {", ".join(_DERIVE_SOURCE_COLUMNS)}
            FROM staging.tenders_raw
            WHERE run_id = %s AND batch_key = %s
              AND scraped_at IS NOT NULL AND derived_at IS NULL
            ORDER BY bid_id ASC
            LIMIT %s
            """,
            (run_id, month, batch_size),
        )
        return cur.fetchall()


_PARTICIPATION_INSERT_COLUMNS = [
    "seller_id", "seller_name", "mse_status", "rank", "rank_num", "total_price",
    "is_winner", "winner_id", "winner_name", "winner_price", "technical_status",
    "outcome", "end_date",
]


@db_retry
def bulk_insert_participations(month: str, records: list) -> int:
    """
    Bulk insert flattened participation records into staging.bid_participations_raw.
    Each record is a dict with a "bid_id" key plus the _PARTICIPATION_INSERT_COLUMNS fields
    (as produced by pipeline3.extract.extract_participants).
    """
    if not records:
        return 0
    run_id = _get_run_id(month)
    if run_id is None:
        return 0
    conn = get_connection()
    _ensure_schema(conn)

    rows = [
        (run_id, month, rec["bid_id"], *(rec.get(col) for col in _PARTICIPATION_INSERT_COLUMNS))
        for rec in records
    ]
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO staging.bid_participations_raw
                (run_id, batch_key, bid_id, {", ".join(_PARTICIPATION_INSERT_COLUMNS)})
            VALUES ({", ".join(["%s"] * (len(_PARTICIPATION_INSERT_COLUMNS) + 3))})
            ON CONFLICT (run_id, bid_id, seller_id) DO NOTHING
            """,
            rows,
        )
    return len(rows)


@db_retry
def mark_tenders_derived(month: str, bid_ids: list) -> None:
    """Mark a batch of staged tenders as derived (participants extracted)."""
    if not bid_ids:
        return
    run_id = _get_run_id(month)
    if run_id is None:
        return
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE staging.tenders_raw
            SET derived_at = %s
            WHERE run_id = %s AND bid_id = ANY(%s)
            """,
            (_now(), run_id, bid_ids),
        )
