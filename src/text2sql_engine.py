"""
src/text2sql_engine.py

Production Text2SQL engine (modular):
- Reflects live schema from DB
- Uses Gemini to generate SQL
- Sanitizes & validates SQL (via query_validator)
- Executes with caching + logging
- Provides query plan + monitoring
"""
from __future__ import annotations

import json
import logging
import datetime
from typing import Dict, Any

import google.generativeai as genai
import pandas as pd
from sqlalchemy import create_engine, inspect

from src.config import DB_URL, MODEL_NAME, GEMINI_API_KEY
from src.cache import QueryCache
from src.history import QueryHistory
from src.monitor import get_db_stats, get_query_plan
from src.query_validator import sanitize_query
from src.database import execute_query

# Configure Gemini with the API key
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    raise ValueError("GOOGLE_API_KEY not found in environment variables")

# ---------------- Setup ----------------
logger = logging.getLogger("text2sql")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


# ---------------- Helpers ----------------
def _json_default(obj):
    """Fix Decimal and date serialization for JSON dumps."""
    if isinstance(obj, (pd.Timestamp, datetime.date, datetime.datetime)):
        return obj.isoformat()
    if hasattr(obj, "to_eng_string"):  # Decimal
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


# ---------------- Engine ----------------
class Text2SQLEngine:
    def __init__(self, model_name: str = MODEL_NAME, debug: bool = False):
        self.model = genai.GenerativeModel(model_name)
        self.debug = debug
        self.cache = QueryCache(maxsize=100)
        self.history = QueryHistory()

    def _fetch_schema_context(self) -> str:
        """Reflect schema from live DB and build context string."""
        eng = create_engine(DB_URL, future=True)
        insp = inspect(eng)
        ignore = {"order_details"}  # avoid duplicates
        lines = ["Database schema (USE ONLY these exact tables/columns):"]
        for t in insp.get_table_names(schema="public"):
            if t in ignore:
                continue
            cols = [c["name"] for c in insp.get_columns(t, schema="public")]
            lines.append(f"- {t}({', '.join(cols)})")
        return "\n".join(lines)

    def generate_sql(self, question: str) -> str:
        """Generate SQL query using Gemini model."""
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
            logger.info("Gemini raw SQL:\n%s", sql)
        return sql

    def run(self, question: str, timeout_ms: int = 5000, limit: int = 1000) -> Dict[str, Any]:
        """Main entry: generate, sanitize, execute, log, return results."""
        raw_sql = ""
        try:
            raw_sql = self.generate_sql(question)
            safe_sql = sanitize_query(raw_sql, max_limit=limit)

            eng = create_engine(DB_URL, future=True)
            df: pd.DataFrame = self.cache.get(safe_sql, eng)

            # log success
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
            self.history.log(question, raw_sql, False)
            return {"ok": False, "error": str(e)}


# ---------------- CLI ----------------
if __name__ == "__main__":
    engine = Text2SQLEngine(debug=True)
    q = "Top 5 products by total sales with their categories."
    payload = engine.run(q, timeout_ms=5000, limit=1000)
    print(json.dumps(payload, indent=2, default=_json_default))