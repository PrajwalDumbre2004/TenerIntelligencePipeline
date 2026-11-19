"""
Shared PostgreSQL connection factory, used by every pipeline package.
"""
import functools
import time

import psycopg

from common.config import POSTGRES_DSN

_conn: psycopg.Connection = None

# TCP keepalives: a connection that silently died (Azure PG maintenance,
# network blip) is detected in ~80 s instead of a call hanging indefinitely.
_CONNECT_KWARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}
# SSL comes from the DSN: add sslmode=require to the Azure DSN (a local
# Postgres has no SSL, so it can't be forced here).

# Waits between retries of a DB call that lost its connection (~5 min total).
_RETRY_DELAYS = (2, 5, 10, 30, 60, 60, 60, 60)
_retry_depth = 0


def get_connection() -> psycopg.Connection:
    """
    Lazy singleton Postgres connection (autocommit).

    Each pipeline2 worker runs in its own spawned process (ProcessPoolExecutor),
    so module state -- including this singleton -- is naturally per-process,
    the same way the old MongoClient singleton was.
    """
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(POSTGRES_DSN, autocommit=True, **_CONNECT_KWARGS)
    return _conn


def _drop_connection():
    """Close the current connection (if any); the next get_connection() reconnects."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None


def db_retry(fn):
    """
    Retry a DB function when the connection fails: drop the connection, wait
    2 -> 5 -> 10 -> 30 -> 60 s ..., reconnect and call it again; after ~5 min
    the error is raised. Only for functions that are safe to run twice
    (upserts, idempotent updates, reads).

    Only the outermost decorated call retries: a decorated function called from
    another one just raises, so retries don't multiply.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _retry_depth
        if _retry_depth:
            return fn(*args, **kwargs)
        _retry_depth += 1
        try:
            for delay in _RETRY_DELAYS:
                try:
                    return fn(*args, **kwargs)
                except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                    print(f"  [db] {fn.__name__} failed ({str(e).strip()[:150]}); "
                          f"reconnecting in {delay}s")
                    _drop_connection()
                    time.sleep(delay)
            return fn(*args, **kwargs)
        finally:
            _retry_depth -= 1
    return wrapper
