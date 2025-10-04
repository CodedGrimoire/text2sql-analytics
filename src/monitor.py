# src/monitor.py
import psutil
from sqlalchemy import create_engine

def get_db_stats(engine):
    with engine.connect() as conn:
        stats = conn.execute("SELECT datname, numbackends, xact_commit, blks_hit FROM pg_stat_database;").fetchall()
    return {
        "cpu": psutil.cpu_percent(),
        "memory": psutil.virtual_memory().percent,
        "db_stats": [dict(row) for row in stats]
    }
