"""
scripts/setup_database.py
Apply schema SQL files in order using SQLAlchemy/psycopg2.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SQL_DIR = Path("data/schema")
ORDER = [
    "00_extensions.sql",
    "01_tables.sql",
    # "02_triggers.sql",  # optional
    "03_indexes.sql",
    "99_security.sql",
]

def apply_sql(engine, path: Path):
    with open(path, "r") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))

def main():
    engine = create_engine(ENGINE_URL)
    for fname in ORDER:
        fpath = SQL_DIR / fname
        if not fpath.exists():
            print(f"⚠️  Missing {fpath}, skipping")
            continue
        print(f"▶️  Applying {fpath} ...")
        apply_sql(engine, fpath)
    print("✅ Schema & security applied")

if __name__ == "__main__":
    main()
