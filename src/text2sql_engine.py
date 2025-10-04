"""
src/text2sql_engine.py

Debug version: show raw Gemini response without query validation.
"""

import os
import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv

# Commenting out validator for now
# from src.query_validator import sanitize_query
from src.database import execute_query

# Load environment variables
load_dotenv()

# Gemini API key from .env
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise EnvironmentError("❌ GEMINI_API_KEY not set in .env")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Updated Gemini model
MODEL_NAME = "gemini-2.5-flash-lite"


class Text2SQLEngine:
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = genai.GenerativeModel(model_name)

    def generate_sql(self, question: str, schema_context: str = "") -> str:
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
        # Step 1: Generate SQL
        raw_sql = self.generate_sql(question, schema_context)
        print("🤖 Raw Gemini response:\n", raw_sql)

        # Step 2: Directly run it (⚠️ no validation for now!)
        df: pd.DataFrame = execute_query(raw_sql)

        return {"sql": raw_sql, "results": df.to_dict(orient="records")}


if __name__ == "__main__":
    engine = Text2SQLEngine()

    question = "Which employee has processed the most orders?"
    try:
        result = engine.run_query(question)
        print("✅ Generated SQL:", result["sql"])
        print("📊 Sample results:", result["results"][:3])
    except Exception as e:
        print("❌ Error:", e)
