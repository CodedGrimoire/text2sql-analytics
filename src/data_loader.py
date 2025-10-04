"""
src/data_loader.py

Handles loading of Northwind Excel data into pandas DataFrames,
validates data quality, and generates a normalized PostgreSQL schema.
"""

import os
import pandas as pd
from pathlib import Path

# Path configuration
DATA_PATH = Path("data/raw/northwind.xlsx")
SCHEMA_PATH = Path("data/schema/schema.sql")


class DataLoader:
    def __init__(self, filepath: Path = DATA_PATH):
        self.filepath = filepath
        if not self.filepath.exists():
            raise FileNotFoundError(f"Excel file not found at {self.filepath}")

        self.tables: dict[str, pd.DataFrame] = {}

    def load_excel(self) -> None:
        """Load all sheets from Excel into pandas DataFrames."""
        self.tables = pd.read_excel(self.filepath, sheet_name=None)
        print(f"Loaded {len(self.tables)} sheets: {list(self.tables.keys())}")

    def validate_tables(self) -> None:
        """Perform simple validation checks on each table."""
        for name, df in self.tables.items():
            print(f"\nValidating {name}:")

            # Check for NULLs
            null_counts = df.isnull().sum()
            if null_counts.any():
                print(f"  ⚠ NULL values detected:\n{null_counts[null_counts > 0]}")

            # Check for duplicate rows
            dup_count = df.duplicated().sum()
            if dup_count > 0:
                print(f"  ⚠ {dup_count} duplicate rows found in {name}")

            # Check dtypes
            print(f"  Column types:\n{df.dtypes}")

    def normalize_schema(self) -> str:
        """
        Generate SQL schema for PostgreSQL (simplified 3NF).
        Note: Adjust datatypes as per actual Excel content.
        """
        schema_sql = """
        CREATE TABLE Customers (
            CustomerID VARCHAR PRIMARY KEY,
            CompanyName VARCHAR NOT NULL,
            ContactName VARCHAR,
            Country VARCHAR
        );

        CREATE TABLE Employees (
            EmployeeID SERIAL PRIMARY KEY,
            LastName VARCHAR,
            FirstName VARCHAR,
            Title VARCHAR
        );

        CREATE TABLE Orders (
            OrderID SERIAL PRIMARY KEY,
            CustomerID VARCHAR REFERENCES Customers(CustomerID),
            EmployeeID INT REFERENCES Employees(EmployeeID),
            OrderDate DATE,
            ShippedDate DATE,
            ShipVia INT,
            Freight NUMERIC
        );

        CREATE TABLE Products (
            ProductID SERIAL PRIMARY KEY,
            ProductName VARCHAR NOT NULL,
            SupplierID INT,
            CategoryID INT,
            QuantityPerUnit VARCHAR,
            UnitPrice NUMERIC,
            UnitsInStock INT,
            Discontinued BOOLEAN
        );

        CREATE TABLE OrderDetails (
            OrderID INT REFERENCES Orders(OrderID),
            ProductID INT REFERENCES Products(ProductID),
            UnitPrice NUMERIC,
            Quantity INT,
            Discount NUMERIC,
            PRIMARY KEY (OrderID, ProductID)
        );

        CREATE INDEX idx_orders_customer ON Orders(CustomerID);
        CREATE INDEX idx_orders_employee ON Orders(EmployeeID);
        CREATE INDEX idx_orderdetails_product ON OrderDetails(ProductID);
        """
        return schema_sql.strip()

    def export_schema(self, out_path: Path = SCHEMA_PATH) -> None:
        """Write normalized schema to SQL file."""
        schema_sql = self.normalize_schema()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            f.write(schema_sql)
        print(f"Schema exported to {out_path}")


if __name__ == "__main__":
    loader = DataLoader()
    loader.load_excel()
    loader.validate_tables()
    loader.export_schema()
