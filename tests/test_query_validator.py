import pytest
from src.query_validator import validate_sql

def test_allow_select():
    assert validate_sql("SELECT * FROM customers") is True

def test_block_insert():
    assert validate_sql("INSERT INTO customers VALUES (1)") is False
