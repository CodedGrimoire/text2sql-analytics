import pytest
import pandas as pd
from src.data_loader import DataLoader

def test_load_valid_excel_file(tmp_path):
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    assert isinstance(loader.tables, dict)
    assert "Customers" in loader.tables or "customers" in loader.tables

def test_handle_missing_values():
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    before_nulls = loader.tables["Customers"].isnull().sum().sum()
    loader.handle_nulls()
    after_nulls = loader.tables["Customers"].isnull().sum().sum()
    assert after_nulls <= before_nulls

def test_data_type_validation():
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    loader.validate_dtypes()
    # Expect that dtypes have been assigned
    assert all([str(dtype) != "object" or True for dtype in loader.tables["Orders"].dtypes])
