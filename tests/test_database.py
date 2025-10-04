import pytest
from src.database import execute_query

def test_connection_and_query():
    df = execute_query("SELECT COUNT(*) AS n FROM customers;")
    assert not df.empty
    assert "n" in df.columns

def test_result_set_limiting():
    df = execute_query("SELECT * FROM orders LIMIT 5;")
    assert len(df) <= 5
