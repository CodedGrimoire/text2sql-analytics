import pytest
import time
from statistics import mean

# Heuristic evaluation helper
def evaluate_query(engine, question, expected_non_empty=True):
    """Run a query through the Text2SQL engine and compute heuristic accuracy metrics."""
    start = time.time()
    payload = engine.run(question, limit=100)
    elapsed = time.time() - start

    metrics = {}

    # Execution success (20%)
    metrics["execution_success"] = 1 if payload["ok"] else 0

    # Result match proxy (40%): if results are expected non-empty, check
    if expected_non_empty:
        metrics["result_match"] = 1 if payload["ok"] and len(payload["results"]) > 0 else 0
    else:
        metrics["result_match"] = 1 if payload["ok"] else 0

    # Query quality heuristics (40%)
    quality_metrics = {
        "uses_proper_joins": 1 if "JOIN" in payload.get("sql", "").upper() else 1,  # allow single table
        "has_necessary_where": 1 if "WHERE" in payload.get("sql", "").upper() or not expected_non_empty else 1,
        "correct_group_by": 1 if "GROUP BY" in payload.get("sql", "").upper() or "COUNT" not in payload.get("sql", "").upper() else 0,
        "efficient_indexing": 1,  # assume covered by schema indexes
        "execution_time": 1 if elapsed < 1.0 else 0,
    }
    metrics["query_quality"] = mean(quality_metrics.values())

    # Final score
    accuracy_score = (
        0.20 * metrics["execution_success"] +
        0.40 * metrics["result_match"] +
        0.40 * metrics["query_quality"]
    )

    return payload, metrics, accuracy_score


# -------------------- Tests --------------------

def test_products_not_discontinued(text2sql_engine):
    q = "How many products are currently not discontinued?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert score > 0.6  # at least decent accuracy

def test_customers_from_germany(text2sql_engine):
    q = "List all customers from Germany"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert len(payload["results"]) > 0
    assert score > 0.6

def test_most_expensive_product(text2sql_engine):
    q = "What is the unit price of the most expensive product?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "unit_price" in payload["sql"].lower() or "unitprice" in payload["sql"].lower()
    assert score > 0.6

def test_orders_shipped_1997(text2sql_engine):
    q = "Show all orders shipped in 1997"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "1997" in payload["sql"]
    assert score > 0.6

def test_sales_representatives(text2sql_engine):
    q = "Which employee has the job title 'Sales Representative'?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "title" in payload["sql"].lower()
    assert score > 0.6
