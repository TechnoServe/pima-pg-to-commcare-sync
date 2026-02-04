from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection as PgConn

from app.config import PG_DSN


@contextmanager
def get_conn() -> Iterator[PgConn]:
    conn = psycopg2.connect(PG_DSN)
    try:
        yield conn
    finally:
        conn.close()
