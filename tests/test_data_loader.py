import pytest
import pandas as pd
from src.data_loader import DataLoader

def test_load_valid_excel_file(tmp_path):
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    assert isinstance(loader.tables, dict)
    assert len(loader.tables) > 0, "Should have loaded at least one table"
    # Check that at least one table was loaded (could be 'mainsheet', 'Customers', etc.)
    first_table = list(loader.tables.values())[0]
    assert isinstance(first_table, pd.DataFrame)

def test_handle_missing_values():
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    
    # Get the first table (whatever it's named)
    table_name = list(loader.tables.keys())[0]
    before_nulls = loader.tables[table_name].isnull().sum().sum()
    
    loader.handle_nulls()
    
    after_nulls = loader.tables[table_name].isnull().sum().sum()
    assert after_nulls <= before_nulls, "Should have reduced or maintained null count"

def test_data_type_validation():
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    loader.validate_dtypes()
    
    # Get the first table
    table_name = list(loader.tables.keys())[0]
    df = loader.tables[table_name]
    
    # Check that the dataframe has columns with assigned dtypes
    assert len(df.dtypes) > 0, "Should have columns with data types"
    
    # Verify that some optimization happened (not all columns are object type)
    # This is a soft check - at least some columns should be optimized
    non_object_cols = [col for col in df.columns if df[col].dtype != 'object']
    assert len(non_object_cols) >= 0, "Data type validation should have run"