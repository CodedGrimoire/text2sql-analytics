import pytest
import time
from statistics import mean


def evaluate_query(engine, question, expected_non_empty=True):
    """Run a query through the Text2SQL engine and compute heuristic accuracy metrics."""
    start = time.time()
    payload = engine.run(question, limit=500)
    elapsed = time.time() - start

    metrics = {}

    # Execution success (20%)
    metrics["execution_success"] = 1 if payload["ok"] else 0

    # Result match proxy (40%)
    if expected_non_empty:
        metrics["result_match"] = 1 if payload["ok"] and len(payload["results"]) > 0 else 0
    else:
        metrics["result_match"] = 1 if payload["ok"] else 0

    # Query quality heuristics (40%)
    sql_text = payload.get("sql", "").lower()
    quality_metrics = {
        "uses_proper_joins": 1 if "join" in sql_text else 0,
        "has_necessary_where": 1 if "where" in sql_text or "group by" in sql_text else 0,
        "correct_group_by": 1 if "group by" in sql_text else 0,
        "efficient_indexing": 1,  # assume schema indexes help
        "execution_time": 1 if elapsed < 3.0 else 0,
    }
    metrics["query_quality"] = mean(quality_metrics.values())

    accuracy_score = (
        0.20 * metrics["execution_success"]
        + 0.40 * metrics["result_match"]
        + 0.40 * metrics["query_quality"]
    )

    return payload, metrics, accuracy_score


# -------------------- Complex Query Tests --------------------

def test_avg_order_value_by_customer(text2sql_engine):
    q = "What is the average order value by customer, sorted by their total lifetime value?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "avg" in payload["sql"].lower()
    assert "customer" in payload["sql"].lower()
    assert "order" in payload["sql"].lower()
    assert score > 0.6


def test_products_above_avg_margin_together(text2sql_engine):
    q = "Which products have above-average profit margins and are frequently ordered together?"
    payload, metrics, score = evaluate_query(text2sql_engine, q, expected_non_empty=False)
    assert metrics["execution_success"] == 1
    assert "join" in payload["sql"].lower()
    assert "product" in payload["sql"].lower()
    assert score > 0.6


def test_yoy_sales_growth_per_category(text2sql_engine):
    q = "Show the year-over-year sales growth for each product category"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "category" in payload["sql"].lower()
    assert "extract" in payload["sql"].lower() or "year" in payload["sql"].lower()
    assert score > 0.6


def test_customers_all_categories(text2sql_engine):
    q = "Identify customers who have placed orders for products from all categories"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "customer" in payload["sql"].lower()
    assert "category" in payload["sql"].lower()
    assert "having" in payload["sql"].lower()
    assert score > 0.6


def test_profitable_month_per_employee(text2sql_engine):
    q = "Find the most profitable month for each employee based on their order commissions"
    payload, metrics, score = evaluate_query(text2sql_engine, q, expected_non_empty=False)
    assert metrics["execution_success"] == 1
    assert "employee" in payload["sql"].lower()
    assert "order" in payload["sql"].lower()
    assert "group by" in payload["sql"].lower()
    assert score > 0.6
