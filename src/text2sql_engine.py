"""
src/text2sql_engine.py

Converts natural language questions into SQL queries using Gemini API,
validates them, executes against PostgreSQL, and returns results.
"""

import os
import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv

from src.query_validator import sanitize_query
from src.database import execute_query

# Load environment variables
load_dotenv()

# Gemini API key from .env
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY not set in .env")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Use Gemini Pro for SQL generation
MODEL_NAME = "gemini-pro"


class Text2SQLEngine:
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = genai.GenerativeModel(model_name)

    def generate_sql(self, question: str, schema_context: str = "") -> str:
        """
        Generate SQL query from natural language using Gemini.

        Args:
            question (str): Natural language input.
            schema_context (str): Optional DB schema description.

        Returns:
            str: Generated SQL query.
        """
        prompt = f"""
        You are an expert SQL assistant.
        Given the following database schema:
        {schema_context}

        Convert this question into a SQL SELECT query (PostgreSQL dialect).
        Question: {question}
        SQL:
        """

        response = self.model.generate_content(prompt)
        sql = response.text.strip()
        return sql

    def run_query(self, question: str, schema_context: str = "") -> dict:
        """
        Full pipeline: NL question -> SQL -> validation -> execution -> results.

        Args:
            question (str): Natural language query.
            schema_context (str): Optional schema.

        Returns:
            dict: {"sql": str, "results": list of dicts}
        """
        # Step 1: Generate SQL
        raw_sql = self.generate_sql(question, schema_context)

        # Step 2: Sanitize SQL
        safe_sql = sanitize_query(raw_sql)

        # Step 3: Execute SQL
        df: pd.DataFrame = execute_query(safe_sql)

        # Step 4: Convert to JSON
        return {"sql": safe_sql, "results": df.to_dict(orient="records")}


if __name__ == "__main__":
    engine = Text2SQLEngine()

    # Example run
    question = "Which employee has processed the most orders?"
    try:
        result = engine.run_query(question)
        print("✅ Generated SQL:", result["sql"])
        print("📊 Sample results:", result["results"][:3])
    except Exception as e:
        print("❌ Error:", e)
