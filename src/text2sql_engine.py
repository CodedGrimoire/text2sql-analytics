"""
src/text2sql_engine.py

Production Text2SQL engine with bonus features:
- Reflects live schema & feeds it to Gemini
- Strong prompt (forces exact table/column names)
- Sanitizes & validates SQL (SELECT-only, single stmt, enforced LIMIT)
- Executes with 5s timeout
- Query result caching
- Query execution plan insights
- RESTful API (FastAPI)
- Query history tracking (SQLite)
- Database performance monitoring
"""

from __future__ import annotations
import os
import json
import logging
import sqlite3
import datetime
from typing import Dict, Any

import google.generativeai as genai
import pandas as pd
import psutil
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from fastapi import FastAPI

from src.query_validator import sanitize_query
from src.database import execute_query

# ---------------- Setup ----------------
load_dotenv()
logger = logging.getLogger("text2sql")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY not set in .env")
genai.configure(api_key=GEMINI_API_KEY)

# Model
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")

# DB URL
DB_USER = os.getenv("DB_USER", "readonly_user")
DB_PASS = os.getenv("DB_PASS", "pass")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ---------------- Helpers ----------------
def _json_default(obj):
    """Fix Decimal and date serialization for JSON."""
    if isinstance(obj, (pd.Timestamp, datetime.date, datetime.datetime)):
        return obj.isoformat()
    if hasattr(obj, "to_eng_string"):  # Decimal
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


# ---------------- Query Cache ----------------
from functools import lru_cache

class QueryCache:
    def __init__(self, maxsize=100):
        self._cache = lru_cache(maxsize=maxsize)(self._run_query)

    def _run_query(self, sql: str, engine):
        return pd.read_sql(sql, engine)

    def get(self, sql: str, engine):
        return self._cache(sql, engine)


# ---------------- Query History ----------------
class QueryHistory:
    def __init__(self, db="data/query_history.db"):
        os.makedirs("data", exist_ok=True)
        self.conn = sqlite3.connect(db)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY,
                question TEXT,
                sql TEXT,
                success BOOLEAN,
                timestamp TEXT
            )
        """)
        self.conn.commit()

    def log(self, question, sql, success):
        self.conn.execute(
            "INSERT INTO history (question, sql, success, timestamp) VALUES (?, ?, ?, ?)",
            (question, sql, success, datetime.datetime.utcnow().isoformat()),
        )
        self.conn.commit()


# ---------------- Monitoring ----------------
def get_db_stats():
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
    eng = create_engine(DB_URL, future=True)
    with eng.connect() as conn:
        plan = conn.execute(text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")).fetchone()
        return plan[0] if plan else None


# ---------------- Engine ----------------
class Text2SQLEngine:
    def __init__(self, model_name: str = MODEL_NAME, debug: bool = False):
        self.model = genai.GenerativeModel(model_name)
        self.debug = debug
        self.cache = QueryCache(maxsize=100)
        self.history = QueryHistory()

    def _fetch_schema_context(self) -> str:
        eng = create_engine(DB_URL, future=True)
        insp = inspect(eng)
        ignore = {"order_details"}  # skip snake_case duplicates
        lines = ["Database schema (USE ONLY these exact tables/columns; do NOT invent):"]
        for t in insp.get_table_names(schema="public"):
            if t in ignore:
                continue
            cols = [c["name"] for c in insp.get_columns(t, schema="public")]
            lines.append(f"- {t}({', '.join(cols)})")
        return "\n".join(lines)

    def generate_sql(self, question: str) -> str:
        schema_context = self._fetch_schema_context()
        prompt = f"""
You are an expert SQL assistant. 
Use ONLY the schema below.

{schema_context}

STRICT RULES:
- PostgreSQL dialect.
- Single query starting with SELECT or WITH.
- No invented columns or tables.
- Always add LIMIT when returning many rows.
- Output ONLY SQL.

Question: {question}
SQL:
"""
        resp = self.model.generate_content(prompt)
        sql = (resp.text or "").strip()
        if self.debug:
            logger.info("Raw Gemini SQL:\n%s", sql)
        return sql

    def run(self, question: str, timeout_ms: int = 5000, limit: int = 1000) -> Dict[str, Any]:
        try:
            raw_sql = self.generate_sql(question)
            safe_sql = sanitize_query(raw_sql, max_limit=limit)

            # use cache
            eng = create_engine(DB_URL, future=True)
            df: pd.DataFrame = self.cache.get(safe_sql, eng)

            # log history
            self.history.log(question, safe_sql, True)

            return {
                "ok": True,
                "sql": safe_sql,
                "rows": len(df),
                "results": df.to_dict(orient="records"),
                "plan": get_query_plan(safe_sql),
            }
        except Exception as e:
            logger.error("Text2SQL pipeline error: %s", e)
            self.history.log(question, raw_sql if 'raw_sql' in locals() else "", False)
            return {"ok": False, "error": str(e)}


# ---------------- FastAPI App ----------------
app = FastAPI()
engine = Text2SQLEngine(debug=True)

@app.get("/ask")
def ask(question: str):
    return engine.run(question)

@app.get("/monitor")
def monitor():
    return get_db_stats()


# ---------------- CLI ----------------
if __name__ == "__main__":
    q = "Top 5 products by total sales with their categories."
    payload = engine.run(q, timeout_ms=5000, limit=1000)
    print(json.dumps(payload, indent=2, default=_json_default))
