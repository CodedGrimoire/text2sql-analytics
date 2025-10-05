import os
from sqlalchemy import create_engine, text

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"

def reset_and_seed():
    engine = create_engine(DB_URL, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    print(f"✅ Database {DB_NAME} recreated.")

    # Now load schema + data
    os.system(f"docker exec -i pg-northwind psql -U {DB_USER} -d {DB_NAME} < northwind_psql/northwind.sql")
    print("📦 Northwind schema & data loaded.")

if __name__ == "__main__":
    reset_and_seed()
