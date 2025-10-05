# src/monitor.py
import psutil
from sqlalchemy import create_engine, text
from src.config import DB_URL

def get_db_stats():
    """Return CPU, memory and DB stats from PostgreSQL."""
    eng = create_engine(DB_URL, future=True)
    with eng.connect() as conn:
        stats = conn.execute(
            "SELECT datname, numbackends, xact_commit, blks_hit FROM pg_stat_database;"
        ).fetchall()
    return {
        "cpu": psutil.cpu_percent(),
        "memory": psutil.virtual_memory().percent,
        "db_stats": [dict(row._mapping) for row in stats],
    }

def get_query_plan(sql: str):
    """Return EXPLAIN ANALYZE plan for a query in JSON format."""
    eng = create_engine(DB_URL, future=True)
    with eng.connect() as conn:
        plan = conn.execute(
            text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
        ).fetchone()
        return plan[0] if plan else None
