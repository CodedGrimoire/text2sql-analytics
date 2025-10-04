"""
src/database.py

Database layer (SQLAlchemy):
- Creates engine from .env
- Executes SELECT with 5s statement timeout
- Enforces SELECT-only at runtime
- Returns pandas DataFrame
"""

from __future__ import annotations
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("db")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

DB_USER = os.getenv("DB_USER", "readonly_user")
DB_PASS = os.getenv("DB_PASS", "pass")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
_engine: Engine | None = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(ENGINE_URL, pool_pre_ping=True, future=True)
        logger.info("Engine created for %s", DB_NAME)
    return _engine

def _assert_select_only(query: str) -> None:
    q = query.strip().lower()
    if not (q.startswith("select") or q.startswith("with")):
        raise ValueError("Only SELECT/WITH statements are allowed in execute_query().")

def execute_query(query: str, timeout_ms: int = 5000) -> pd.DataFrame:
    """
    Execute a SELECT/CTE query with per-transaction server-side timeout.
    Returns a pandas DataFrame.
    """
    _assert_select_only(query)
    engine = get_engine()
    try:
        with engine.begin() as conn:
            # per-transaction safeguards
            conn.execute(text("SET LOCAL statement_timeout = :t"), {"t": timeout_ms})
            conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = :t"), {"t": timeout_ms})
            conn.execute(text("SET LOCAL transaction_read_only = true"))

            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        logger.error("Database error: %s", e)
        raise RuntimeError(f"Database error: {e}") from e
