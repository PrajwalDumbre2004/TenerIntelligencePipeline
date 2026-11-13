"""
Shared PostgreSQL connection factory, used by every pipeline package.
"""
import psycopg

from common.config import POSTGRES_DSN

_conn: psycopg.Connection = None


def get_connection() -> psycopg.Connection:
    """
    Lazy singleton Postgres connection (autocommit).

    Each pipeline2 worker runs in its own spawned process (ProcessPoolExecutor),
    so module state -- including this singleton -- is naturally per-process,
    the same way the old MongoClient singleton was.
    """
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(POSTGRES_DSN, autocommit=True)
    return _conn
