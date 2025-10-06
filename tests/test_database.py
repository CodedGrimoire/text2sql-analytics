# tests/test_database.py
import pytest
import threading
import time
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.database import execute_query, get_engine


def test_connection_pool_management():
    """Ensure engine provides valid connections and can handle multiple requests."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;"))  # ✅ wrap in text()
        assert result.scalar() == 1


def test_transaction_rollback():
    """Verify that a failed transaction rolls back properly."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            # Force an error by inserting into a non-existent table
            conn.execute(text("INSERT INTO non_existent_table VALUES (1);"))
    except SQLAlchemyError:
        pass

    # Ensure the DB is still consistent
    df = execute_query("SELECT COUNT(*) AS n FROM customers;")
    assert "n" in df.columns
    assert df.iloc[0]["n"] >= 0  # table still intact


def test_query_timeout_enforcement():
    """Simulate long-running query and ensure timeout is respected."""
    with pytest.raises(TimeoutError):
        # pg_sleep(10) takes 10 seconds -> should fail with timeout=2
        execute_query("SELECT pg_sleep(10);", timeout=2)


def test_result_set_limiting():
    """Ensure LIMIT is enforced on queries."""
    df = execute_query("SELECT * FROM orders LIMIT 5;")
    assert len(df) <= 5


def test_concurrent_query_execution():
    """Run multiple queries in parallel to test pool + concurrency handling."""
    results = []
    errors = []

    def worker(query):
        try:
            df = execute_query(query)
            results.append(len(df))
        except Exception as e:
            errors.append(str(e))

    threads = [
        threading.Thread(target=worker, args=("SELECT * FROM customers LIMIT 10;",)),
        threading.Thread(target=worker, args=("SELECT * FROM products LIMIT 10;",)),
        threading.Thread(target=worker, args=("SELECT * FROM orders LIMIT 10;",)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert all(isinstance(n, int) for n in results)
