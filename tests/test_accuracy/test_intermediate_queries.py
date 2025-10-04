import pytest
import time
from statistics import mean


def evaluate_query(engine, question, expected_non_empty=True):
    """Run a query through the Text2SQL engine and compute heuristic accuracy metrics."""
    start = time.time()
    payload = engine.run(question, limit=200)
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
    quality_metrics = {
        "uses_proper_joins": 1 if "JOIN" in payload.get("sql", "").upper() else 0,
        "has_necessary_where": 1 if "WHERE" in payload.get("sql", "").upper() or "GROUP BY" in payload.get("sql", "").upper() else 0,
        "correct_group_by": 1 if "GROUP BY" in payload.get("sql", "").upper() else 0,
        "efficient_indexing": 1,  # assume schema indexes cover basics
        "execution_time": 1 if elapsed < 2.0 else 0,
    }
    metrics["query_quality"] = mean(quality_metrics.values())

    accuracy_score = (
        0.20 * metrics["execution_success"]
        + 0.40 * metrics["result_match"]
        + 0.40 * metrics["query_quality"]
    )

    return payload, metrics, accuracy_score


# -------------------- Intermediate Query Tests --------------------

def test_total_revenue_per_category(text2sql_engine):
    q = "What is the total revenue per product category?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "group by" in payload["sql"].lower()
    assert score > 0.6


def test_employee_most_orders(text2sql_engine):
    q = "Which employee has processed the most orders?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "employee" in payload["sql"].lower()
    assert "order" in payload["sql"].lower()
    assert score > 0.6


def test_monthly_sales_trends_1997(text2sql_engine):
    q = "Show monthly sales trends for 1997"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "1997" in payload["sql"]
    assert "group by" in payload["sql"].lower()
    assert score > 0.6


def test_top5_customers_total_value(text2sql_engine):
    q = "List the top 5 customers by total order value"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "limit" in payload["sql"].lower()
    assert "customer" in payload["sql"].lower()
    assert score > 0.6


def test_avg_order_value_by_country(text2sql_engine):
    q = "What is the average order value by country?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "avg" in payload["sql"].lower()
    assert "group by" in payload["sql"].lower()
    assert score > 0.6


def test_products_out_of_stock_not_discontinued(text2sql_engine):
    q = "Which products are out of stock but not discontinued?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "where" in payload["sql"].lower()
    assert "unitsinstock" in payload["sql"].lower() or "units_in_stock" in payload["sql"].lower()
    assert score > 0.6


def test_orders_per_shipper(text2sql_engine):
    q = "Show the number of orders per shipper company"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "shipper" in payload["sql"].lower()
    assert "group by" in payload["sql"].lower()
    assert score > 0.6


def test_revenue_contribution_suppliers(text2sql_engine):
    q = "What is the revenue contribution of each supplier?"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "supplier" in payload["sql"].lower()
    assert "group by" in payload["sql"].lower()
    assert score > 0.6


def test_customers_orders_every_quarter_1997(text2sql_engine):
    q = "Find customers who placed orders in every quarter of 1997"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "1997" in payload["sql"]
    assert score > 0.6


def test_avg_delivery_time_by_shipper(text2sql_engine):
    q = "Calculate average delivery time by shipping company"
    payload, metrics, score = evaluate_query(text2sql_engine, q)
    assert metrics["execution_success"] == 1
    assert "avg" in payload["sql"].lower()
    assert "ship" in payload["sql"].lower()
    assert score > 0.6
