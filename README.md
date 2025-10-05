# 📘 Text2SQL Analytics Engine

##  Project Overview  
`text2sql-analytics` is a **Natural Language to SQL pipeline** built on top of **Google Gemini**, **PostgreSQL**, and **FastAPI**.  
It enables non-technical users to query structured databases using plain English questions.  

Key features:  
- 🔄 **Dynamic Schema Reflection**: fetches live database schema before each query  
- 🧠 **LLM-powered SQL Generation** (Gemini)  
- ✅ **Query Validation**: enforces `SELECT`-only, single-statement, safe LIMITs  
- ⚡ **Execution Pipeline**: queries run with caching + 5s timeout  
- 📊 **Monitoring Dashboard**: CPU, memory, and DB stats from `pg_stat_database`  
- 📝 **Query History Tracking**: SQLite log of natural language questions + SQL + result  
- 🔍 **Explain/Analyze Plans** for query optimization insights  
- 🧪 **Pytest-based Testing Suite** with accuracy metrics  

---
Main Features of Text2SQL-Analytics

This project provides a production-ready Text2SQL system with dynamic schema normalization, query validation, execution engine, and database seeding. Below are the main components:

1. Dynamic Normalization Pipeline (dynamic_normalization_pipeline.py)

Automatically transforms raw Excel/CSV data into a clean, relational schema:

Column Normalization: Cleans up inconsistent column names → snake_case.

Type Inference: Detects appropriate SQL column types (INTEGER, DATE, VARCHAR, NUMERIC, etc.).

Primary Key Detection: Identifies or synthesizes primary keys based on uniqueness and naming hints.

Foreign Key Detection: Uses heuristics + value overlap to infer relationships.

Metrics & Reports: Exports normalization results in JSON and Markdown for transparency.

Schema & Index Export: Generates SQL DDL files (01_tables.sql, 03_indexes.sql) for easy migration.

FK-Safe Seeding: Inserts normalized data into Postgres respecting dependencies.

This ensures even a single messy sheet can be split and converted into multiple relational tables automatically.

2. Text2SQL Engine (text2sql_engine.py)

Core engine that converts natural language → SQL → results:

Schema Reflection: Inspects live Postgres schema and builds context for prompting.

Gemini LLM Integration: Uses Google Gemini to generate syntactically correct, schema-aware SQL.

Sanitization & Validation: Ensures only SELECT/WITH queries, single-statement, with enforced LIMIT.

Query Execution: Runs validated SQL against the database with a safe timeout.

Caching: LRU cache avoids redundant queries and improves performance.

History Tracking: Logs questions, generated SQL, execution status in a local SQLite DB.

Result Formatting: Returns both JSON for API/tests and table-style text (for CLI usage, like psql).

Execution Plan: Retrieves EXPLAIN ANALYZE for optimization insights.

Example:

$ python -m src.text2sql_engine
Question: "What is the total revenue per category?"


→ Gemini generates SQL → executes → outputs JSON + human-readable table.

3. Query Validator (query_validator.py)

Security and correctness enforcement:

Blocks DDL/DML: Prevents DROP, INSERT, UPDATE, DELETE.

Single Statement Only: Rejects multi-statement queries.

SQL Injection Protection: Strips suspicious patterns (;, --, /* */).

Automatic LIMIT: Enforces result cap (configurable) to prevent runaway queries.

This guarantees read-only, controlled SQL execution.

4. Data Loader (data_loader.py)

CLI utility to load raw datasets into Postgres:

Accepts Excel/CSV input (--excel file.xlsx or --csvdir folder/).

Runs the dynamic normalization pipeline.

Exports schema and index SQL files.

Optionally seeds normalized data directly into Postgres.

One command → normalized schema + populated DB.

5. Database Utilities (database.py)

Helper functions for database management:

reset_database(): Drops/recreates the database (safe for tests/dev).

execute_query(): Runs validated queries with SQLAlchemy engine.

Connection Pooling: Efficient Postgres access.

6. Configuration (config.py)

Centralized configuration via .env + defaults:

DB Connection: DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME

Gemini API: GEMINI_API_KEY, MODEL_NAME

Paths: OUTPUT_DIR for reports, SCHEMA_DIR for generated DDL

Keeps secrets out of code and allows per-environment setup.

## 🏗️ Architecture Diagram  

```mermaid
flowchart TD
    A[User Question] -->|FastAPI / CLI| B[Text2SQL Engine]
    B --> C[Schema Reflection (SQLAlchemy)]
    B --> D[Gemini Model (LLM)]
    D --> E[Generated SQL]
    E --> F[Query Validator]
    F -->|Safe SQL| G[PostgreSQL Database]
    G --> H[Execution + Cache]
    H --> I[Results + JSON]
    G --> J[Query Plan (EXPLAIN)]
    I --> K[FastAPI Response / CLI Output]
    K --> L[Query History Logger (SQLite)]
    J --> M[Monitoring Dashboard]
```

---

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/text2sql-analytics.git
cd text2sql-analytics
```

### 2. Create and activate virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Environment variables (`.env`)
Create a `.env` file in the `src/` directory:
```
GEMINI_API_KEY=your_google_api_key_here
GEMINI_MODEL=gemini-1.5-flash
DB_USER=postgres
DB_PASS=postgres
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=northwind
```

---

## 🗄️ Database Initialization Guide

### Using Docker PostgreSQL
```bash
docker run --name pg-northwind -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15
```

### Load Northwind sample schema
```bash
docker cp northwind.sql pg-northwind:/northwind.sql
docker exec -it pg-northwind psql -U postgres -d postgres -f /northwind.sql
```

Verify:
```bash
docker exec -it pg-northwind psql -U postgres -d northwind -c "\dt"
```

---

## ▶️ Running the Engine

### CLI Example
```bash
python -m src.text2sql_engine
```

### FastAPI Server
```bash
uvicorn src.text2sql_engine:app --reload
```

Visit:
- `http://127.0.0.1:8000/ask?question=top+5+products`
- `http://127.0.0.1:8000/monitor`

---

## 🧪 Running Tests

```bash
pytest -v
```

Example output:
```
tests/test_accuracy/test_simple_queries.py::test_customers_from_germany PASSED
tests/test_accuracy/test_sales_representatives PASSED
...
```

---

## 📊 Accuracy Metrics

| Metric                | Weight | Example Score |
|------------------------|--------|---------------|
| Execution Accuracy     | 20%    | 0.8           |
| Result Match           | 40%    | 0.7           |
| Query Quality          | 40%    | 0.75          |
| **Total**              | 100%   | 0.75          |

**Query Quality Sub-metrics**:  
- Proper Joins  
- Necessary WHERE clauses  
- Correct GROUP BY usage  
- Efficient indexing  
- Execution < 1s  

---

## ⚠️ Known Limitations
- Gemini API quotas (free tier has strict rate limits)  
- PostgreSQL-only dialect supported  
- Limited to `SELECT` queries (no UPDATE/DELETE/INSERT)  
- Heuristic PK/FK detection may misclassify in messy Excel dumps  

---

## 🌱 Future Improvements
- Fine-tune model on schema-specific prompts  
- Add role-based query access control  
- Auto-generate schema diagrams with dbdiagram.io  
- Advanced caching (Redis or Memcached)  
- Web UI with query builder + visualization  

---

## 📚 Additional Docs
- Schema diagram: [dbdiagram.io](https://dbdiagram.io)  
- API docs: `/docs` (FastAPI Swagger UI)  
- ## Test Coverage Report
To generate a test coverage report in HTML format:

```bash
pytest --cov=src --cov-report=html
open htmlcov/index.html   # macOS
xdg-open htmlcov/index.html  # Linux

