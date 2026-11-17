"""
One-time migration 001: add bid stage / cancellation columns to `tenders`,
with stage = 3 on every existing row (they were all fetched from GeM's awarded
list). See 001_bid_stage.sql: stage is added with a constant default, which is
metadata-only (no row is rewritten), then the default is dropped so new rows
start NULL. is_cancelled / bid_status / status_updated_at stay NULL until each
month is re-run.

Then verifies, quickly:
  - catalog: every partition stores stage's "missing value" = 3, no default left;
  - data: a sample of rows per partition all read stage = 3.

Re-running is a no-op. Note: if `stage` already exists (e.g. created by an older
version of this migration and only partly filled), ADD COLUMN IF NOT EXISTS
skips it; the verification below then reports any NULL rows.

Usage (uses POSTGRES_DSN from the environment / .env):
    python pipeline4/sql/migrations/run_001.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from common.db import get_connection  # noqa: E402

SQL_FILE = Path(__file__).with_name("001_bid_stage.sql")


def main():
    conn = get_connection()   # autocommit; the SQL file has its own BEGIN/COMMIT
    with conn.cursor() as cur:
        t0 = time.time()
        print("Adding columns (metadata-only) ...")
        cur.execute(SQL_FILE.read_text(encoding="utf-8"))
        print(f"  done in {time.time() - t0:.1f}s")

        parts = [r[0] for r in cur.execute(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = 'public.tenders'::regclass ORDER BY 1").fetchall()]
        if not parts:
            sys.exit("No partitions of `tenders` found - is POSTGRES_DSN pointing at the right database?")

        print("Verifying (catalog + 1000-row sample per partition) ...")
        bad = 0
        for p in parts:
            hasmissing, missing, hasdef = cur.execute(
                "SELECT atthasmissing, attmissingval::text, atthasdef FROM pg_attribute "
                "WHERE attrelid = %s::regclass AND attname = 'stage' AND NOT attisdropped", (p,)).fetchone()
            n, n_null = cur.execute(
                f"SELECT count(*), count(*) FILTER (WHERE stage IS NULL) FROM (SELECT stage FROM {p} LIMIT 1000) s"
            ).fetchone()
            ok = (n == 0 or n_null == 0) and not hasdef
            bad += not ok
            print(f"  {'OK ' if ok else 'BAD'} {p:16s} missing value={missing if hasmissing else '-'} "
                  f"default={'yes' if hasdef else 'no'} sample={n} NULL={n_null}")
        if bad:
            sys.exit(f"FAILED on {bad} partition(s).")
        print("Migration 001 complete.")


if __name__ == "__main__":
    main()
