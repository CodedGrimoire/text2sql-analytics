import pytest
from src.text2sql_engine import Text2SQLEngine

@pytest.fixture(scope="module")
def text2sql_engine():
    return Text2SQLEngine(debug=True)

def test_end_to_end_simple_query(text2sql_engine):
    question = "List all customers from Germany"
    payload = text2sql_engine.run(question, limit=10)
    assert payload["ok"]
    assert isinstance(payload["results"], list)
    assert all("country" in row or "Country" in row for row in payload["results"])

def test_multi_table_join_query(text2sql_engine):
    question = "Show top 5 products with their category names"
    payload = text2sql_engine.run(question, limit=5)
    assert payload["ok"]
    assert "join" in payload["sql"].lower()
    assert len(payload["results"]) <= 5
    for row in payload["results"]:
        assert "product" in " ".join(row.keys()).lower()
        assert "category" in " ".join(row.keys()).lower()

def test_aggregate_query_generation(text2sql_engine):
    question = "What is the total revenue per category?"
    payload = text2sql_engine.run(question, limit=20)
    assert payload["ok"]
    sql_lower = payload["sql"].lower()
    assert "sum" in sql_lower or "count" in sql_lower or "avg" in sql_lower
    assert isinstance(payload["results"], list)

def test_error_recovery_mechanism(text2sql_engine):
    # Give a vague question to trigger retry/error handling
    question = "Tell me about stuff"
    payload = text2sql_engine.run(question, limit=5)
    # Should fail gracefully instead of crashing
    assert "ok" in payload
    if not payload["ok"]:
        assert "error" in payload

def test_invalid_question_handling(text2sql_engine):
    # Nonsense / invalid input
    question = "DROP DATABASE northwind;"
    payload = text2sql_engine.run(question, limit=5)
    assert not payload["ok"]
    assert "error" in payload
