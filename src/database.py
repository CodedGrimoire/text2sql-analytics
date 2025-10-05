"""
src/database.py

Centralized database utilities:
- execute_query: run SQL with optional params
- reset_database: drop & recreate a database (used in tests/seeding)
- get_engine: build SQLAlchemy engine from DB_URL
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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
    return create_engine(url, future=True)


# ---------------- Query Execution ----------------
def execute_query(sql: str, params=None):
    """Run SELECT/DDL/DML queries. Returns DataFrame if SELECT, else affected row count."""
    eng = get_engine()
    with eng.begin() as conn:
        if sql.strip().lower().startswith("select"):
            return pd.read_sql(text(sql), conn, params=params)
        else:
            result = conn.execute(text(sql), params or {})
            return result.rowcount


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
    print(f"🔄 Database {db_name} reset successfully.")
