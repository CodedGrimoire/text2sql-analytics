# tests/conftest.py
import pytest
import pandas as pd

from src.database import execute_query
from src.text2sql_engine import Text2SQLEngine

@pytest.fixture(scope="session")
def db_conn():
    """Fixture to provide a test database connection."""
    return execute_query

@pytest.fixture(scope="session")
def text2sql_engine():
    """Fixture to provide a shared Text2SQL engine instance."""
    return Text2SQLEngine(debug=True)

@pytest.fixture
def run_sql():
    """Helper to run raw SQL in tests and return DataFrame."""
    def _run(sql: str) -> pd.DataFrame:
        return execute_query(sql)
    return _run
