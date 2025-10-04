"""
src/database.py

Database layer for PostgreSQL.
Handles connection pooling and safe query execution (read-only SELECT only).
"""

import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables (.env should contain DB creds)
load_dotenv()

DB_USER = os.getenv("DB_USER", "readonly_user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

# PostgreSQL connection string
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
)


def execute_query(query: str, limit: int = 1000, timeout: int = 5) -> pd.DataFrame:
    """
    Execute a SQL SELECT query safely and return results as DataFrame.

    Args:
        query (str): SQL query (must be SELECT).
        limit (int): Max number of rows to fetch.
        timeout (int): Timeout in seconds for query execution.

    Returns:
        pd.DataFrame: Query results.

    Raises:
        ValueError: If non-SELECT query detected.
        SQLAlchemyError: For DB errors.
    """

    # Enforce SELECT-only
    if not re.match(r"^\s*SELECT", query, re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed!")

    # Append LIMIT if not present
    if re.search(r"\bLIMIT\b", query, re.IGNORECASE) is None:
        query = f"{query.rstrip(';')} LIMIT {limit}"

    try:
        with engine.connect() as conn:
            # Enforce statement timeout
            conn.execute(text(f"SET statement_timeout = {timeout * 1000}"))
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database error: {e}") from e


if __name__ == "__main__":
    # Example usage
    try:
        df = execute_query("SELECT * FROM customers;")
        print(df.head())
    except Exception as e:
        print("❌ Error:", e)
