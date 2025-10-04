"""
src/text2sql_engine.py

Production Text2SQL engine:
- Reflects live schema & feeds it to Gemini
- Strong prompt (forces exact table/column names)
- Sanitizes & validates SQL (SELECT-only, single stmt, enforced LIMIT)
- Executes with 5s timeout
- Returns JSON payload (and prints pretty JSON if run as script)
"""

from __future__ import annotations
import os
import json
import logging
import decimal

import datetime

from typing import Dict, Any

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

from src.query_validator import sanitize_query
from src.database import execute_query

# ----------------- setup -----------------
load_dotenv()
logger = logging.getLogger("text2sql")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY not set in .env")
genai.configure(api_key=GEMINI_API_KEY)

# Model (use one that your key supports)
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")

# DB URL for reflection only (read creds from .env)
DB_USER = os.getenv("DB_USER", "readonly_user")
DB_PASS = os.getenv("DB_PASS", "pass")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "northwind")
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# -------------- engine -------------------

class Text2SQLEngine:
    def __init__(self, model_name: str = MODEL_NAME, debug: bool = False):
        self.model = genai.GenerativeModel(model_name)
        self.debug = debug

    def _fetch_schema_context(self) -> str:
        """
        Reflect tables & columns from Postgres and return a concise schema block.
        """
        eng = create_engine(DB_URL, future=True)
        insp = inspect(eng)

        lines = ["Database schema (USE ONLY these exact tables/columns; do NOT invent):"]
        for t in insp.get_table_names(schema="public"):
            cols = [c["name"] for c in insp.get_columns(t, schema="public")]
            lines.append(f"- {t}({', '.join(cols)})")

        return "\n".join(lines)

    def generate_sql(self, question: str) -> str:
        schema_context = self._fetch_schema_context()

        prompt = f"""
You are an expert SQL assistant. 
You MUST generate a PostgreSQL query using ONLY the tables/columns below.

{schema_context}

STRICT RULES:
- Do NOT invent or assume column names.
- Do NOT pluralize or change names: use EXACTLY what is listed above.
- If uncertain, choose only from the provided schema.
- Query must be a single statement starting with SELECT or WITH.
- PostgreSQL dialect ONLY (never SQLite).
- Use EXTRACT(YEAR FROM column) or DATE_PART('year', column) for year extraction.
- Never use STRFTIME or SQLite functions.
- Use table aliases for complex queries.
- Always include a LIMIT when many rows could be returned.
- Do not explain, do not add comments, output ONLY pure SQL.
- Always use exactly the listed table/column names.
- Do not invent synonyms.


Question: {question}
SQL:
"""
        resp = self.model.generate_content(prompt)
        sql = (resp.text or "").strip()
        if self.debug:
            logger.info("Raw Gemini SQL:\n%s", sql)
        return sql

    def run(self, question: str, timeout_ms: int = 5000, limit: int = 1000) -> Dict[str, Any]:
        """
        NL -> SQL -> sanitize -> execute (timeout) -> JSON payload
        """
        try:
            raw_sql = self.generate_sql(question)
            safe_sql = sanitize_query(raw_sql, max_limit=limit)
            df: pd.DataFrame = execute_query(safe_sql, timeout_ms=timeout_ms)

            return {
                "ok": True,
                "sql": safe_sql,
                "rows": len(df),
                "results": df.to_dict(orient="records"),
            }
        except Exception as e:
            logger.error("Text2SQL pipeline error: %s", e)
            return {
                "ok": False,
                "error": str(e),
            }


# ---------- JSON helper for Decimal ----------
# ---------- JSON helper for Decimal/Date ----------
def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")



if __name__ == "__main__":
    engine = Text2SQLEngine(debug=True)

    # Example run
    question = "Which products are out of stock but not discontinued?"
    payload = engine.run(question, timeout_ms=5000, limit=1000)
    print(json.dumps(payload, indent=2, default=_json_default))
