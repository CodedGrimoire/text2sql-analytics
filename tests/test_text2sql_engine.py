import pytest
from src.text2sql_engine import Text2SQLEngine

def test_end_to_end_simple_query(text2sql_engine):
    question = "List all customers from Germany"
    payload = text2sql_engine.run(question, limit=10)
    assert payload["ok"]
    assert isinstance(payload["results"], list)
