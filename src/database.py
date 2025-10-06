"""
src/database.py

Centralized database utilities:
- get_engine: build SQLAlchemy engine from DB_URL
- execute_query: run SQL with optional LIMIT + timeout
- reset_database: drop & recreate a database (for tests/seeding)
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd

# ---------------- Load .env ----------------
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------------- Engine ----------------
def get_engine(url: str = DB_URL):
    """Return a SQLAlchemy engine with connection pooling."""
    return create_engine(
        url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        future=True
    )

# Global engine
engine = get_engine()

# ---------------- Query Execution ----------------
def execute_query(sql: str, params=None, limit: int = 1000, timeout: int = 5):
    """
    Run SQL safely with LIMIT and timeout.
    - If query is SELECT -> returns DataFrame
    - Otherwise -> returns affected row count
    - Raises TimeoutError if execution exceeds timeout
    """
    sql_clean = sql.strip().rstrip(";")

    # Add LIMIT if SELECT without one
    if sql_clean.lower().startswith("select") and "limit" not in sql_clean.lower():
        sql_clean = f"{sql_clean} LIMIT {limit}"

    start = time.time()
    with engine.connect() as conn:
        conn = conn.execution_options(timeout=timeout)
        try:
            stmt = text(sql_clean)  # ✅ always wrap in text()
            if sql_clean.lower().startswith("select"):
                df = pd.read_sql(stmt, conn, params=params)
            else:
                result = conn.execute(stmt, params or {})
                df = result.rowcount
        finally:
            elapsed = time.time() - start
            if elapsed > timeout:
                raise TimeoutError(f"Query exceeded timeout of {timeout}s (took {elapsed:.2f}s)")
    return df

# ---------------- Reset DB (used in tests) ----------------
def reset_database(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, db_name=DB_NAME):
    """
    Drops and recreates a database. Requires superuser rights.
    WARNING: This destroys data, only use in tests/seeding.
    """
    super_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    eng = create_engine(super_url, isolation_level="AUTOCOMMIT")
    with eng.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
        conn.execute(text(f"CREATE DATABASE {db_name}"))
    print(f"Database {db_name} reset successfully.")
