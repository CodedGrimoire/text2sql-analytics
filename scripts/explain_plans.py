# scripts/explain_plans.py
"""
Generate EXPLAIN plans for representative queries
to demonstrate index usage and performance justification.
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load DB creds
load_dotenv()
DB_USER = os.getenv("DB_USER", "readonly_user")
DB_PASS = os.getenv("DB_PASS", "pass")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Representative queries to stress indexes
QUERIES = [
    ("Top 5 products by total sales",
     """
     SELECT p.productid, p.productname,
            SUM(od.unitprice * od.quantity * (1 - od.discount)) AS total_sales
     FROM orderdetails od
     JOIN products p ON p.productid = od.productid
     GROUP BY p.productid, p.productname
     ORDER BY total_sales DESC
     LIMIT 5;
     """),

    ("Orders per year per country",
     """
     SELECT c.country, DATE_PART('year', o.orderdate) AS year, COUNT(*) AS order_count
     FROM orders o
     JOIN customers c ON c.customerid = o.customerid
     GROUP BY c.country, year
     ORDER BY year DESC, order_count DESC
     LIMIT 10;
     """),

    ("Employee order counts (top 5)",
     """
     SELECT e.employeeid, e.firstname, e.lastname, COUNT(o.orderid) AS order_count
     FROM employees e
     JOIN orders o ON e.employeeid = o.employeeid
     GROUP BY e.employeeid, e.firstname, e.lastname
     ORDER BY order_count DESC
     LIMIT 5;
     """),

    ("Customer search by name (GIN index check)",
     """
     SELECT customerid, companyname
     FROM customers
     WHERE companyname ILIKE '%market%';
     """)
]

def main():
    engine = create_engine(DB_URL)
    results = []

    with engine.begin() as conn:
        for label, q in QUERIES:
            try:
                explain_sql = f"EXPLAIN (FORMAT JSON) {q}"
                plan = conn.execute(text(explain_sql)).scalar()
                results.append({"query": label, "plan": plan})
                print(f"✅ {label}")
            except Exception as e:
                results.append({"query": label, "error": str(e)})
                print(f"❌ {label}: {e}")

    out_path = Path("data/outputs/explain_plans.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"🧠 Plans written to {out_path}")

if __name__ == "__main__":
    main()
