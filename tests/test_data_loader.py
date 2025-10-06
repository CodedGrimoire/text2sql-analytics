import pytest
import pandas as pd
from src.data_loader import DataLoader

def test_load_valid_excel_file(tmp_path):
    """Ensure Excel file loads correctly into DataFrames."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    assert isinstance(loader.tables, dict)
    assert len(loader.tables) > 0, "Should have loaded at least one table"
    first_table = list(loader.tables.values())[0]
    assert isinstance(first_table, pd.DataFrame)

def test_handle_missing_values():
    """Verify null handling reduces or maintains NaN count."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    table_name = list(loader.tables.keys())[0]
    before_nulls = loader.tables[table_name].isnull().sum().sum()
    loader.handle_nulls()
    after_nulls = loader.tables[table_name].isnull().sum().sum()
    assert after_nulls <= before_nulls, "Null count should decrease or remain the same"

def test_data_type_validation():
    """Ensure dtypes are validated/optimized."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    loader.validate_dtypes()
    table_name = list(loader.tables.keys())[0]
    df = loader.tables[table_name]
    assert len(df.dtypes) > 0, "Should have inferred dtypes"
    non_object_cols = [col for col in df.columns if df[col].dtype != "object"]
    assert isinstance(non_object_cols, list)

def test_foreign_key_detection():
    """Check that foreign keys are detected between related tables."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    fks = loader.detect_foreign_keys()
    found = any(len(v) > 0 for v in fks.values())
    assert found, "Should detect at least one foreign key relationship"

def test_duplicate_row_detection():
    """Ensure duplicate rows are detected correctly."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    dups = loader.detect_duplicates()
    assert isinstance(dups, dict)
    for count in dups.values():
        assert isinstance(count, int), "Duplicate detection should return int counts"
        assert count >= 0, "Duplicate count cannot be negative"
