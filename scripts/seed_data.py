"""
scripts/seed_data.py

Seed the normalized Postgres Northwind DB using the raw Excel file.
"""

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

EXCEL_PATH = Path("data/raw/northwind.xlsx")

# Map Excel sheet names to DB table names
SHEET_TO_TABLE = {
    "Customers": "customers",
    "Suppliers": "suppliers",
    "Categories": "categories",
    "Shippers": "shippers",
    "Employees": "employees",
    "Orders": "orders",
    "Order Details": "orderdetails",
    "Products": "products",
    "Region": "region",
    "Territories": "territories",
    "EmployeeTerritories": "employeeterritories",
    "CustomerDemographics": "customerdemographics",
    "CustomerCustomerDemo": "customercustomerdemo",
}

def main():
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"❌ Excel not found: {EXCEL_PATH}")

    engine = create_engine(DB_URL)

    # Load workbook
    xls = pd.ExcelFile(EXCEL_PATH)
    print(f"✅ Found sheets: {xls.sheet_names}")

    with engine.begin() as conn:
        for sheet, table in SHEET_TO_TABLE.items():
            if sheet not in xls.sheet_names:
                print(f"⚠️ Skipping missing sheet: {sheet}")
                continue

            df = pd.read_excel(xls, sheet_name=sheet)

            # Normalize column names (strip spaces, lowercase)
            df.columns = [c.strip().lower().replace(" ", "") for c in df.columns]

            # Push data into table (append mode)
            print(f"📤 Loading {sheet} → {table} ({len(df)} rows)")
            df.to_sql(table, conn, if_exists="append", index=False)

    print("🎉 Seeding complete!")

if __name__ == "__main__":
    main()
