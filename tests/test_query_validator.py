import pytest
from src.query_validator import sanitize_query

def test_block_insert_statements():
    with pytest.raises(ValueError):
        sanitize_query("INSERT INTO customers VALUES (1, 'Test');")

def test_block_drop_statements():
    with pytest.raises(ValueError):
        sanitize_query("DROP TABLE customers;")

def test_allow_select_statements():
    sql = "SELECT * FROM customers LIMIT 10;"
    result = sanitize_query(sql)
    assert result.strip().upper().startswith("SELECT")

def test_sql_injection_prevention():
    with pytest.raises(ValueError):
        sanitize_query("SELECT * FROM customers; DROP TABLE orders;")
