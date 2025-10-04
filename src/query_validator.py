"""
src/query_validator.py

Hardened SQL sanitizer/validator for Text2SQL:
- Strip code fences / comments / markdown
- Ensure single statement
- Allow only SELECT / WITH (CTEs)
- Block DDL/DML (INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE/etc.)
- Enforce LIMIT (default 1000)
"""

from __future__ import annotations
import re

FORBIDDEN = re.compile(
    r"""
    \b(INSERT|UPDATE|DELETE|MERGE|UPSERT|REPLACE|TRUNCATE|
       CREATE|ALTER|DROP|RENAME|GRANT|REVOKE|COMMENT|ANALYZE|
       VACUUM|COPY|CALL|DO|EXECUTE|SET\s+ROLE|SET\s+SESSION\s+AUTHORIZATION|
       SECURITY\s+DEFINER|SECURITY\s+INVOKER)
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

MULTI_STMT_SEMI = re.compile(r";\s*[^;\s]", re.DOTALL)  # semicolon followed by more SQL

def _strip_code_fences(sql: str) -> str:
    s = sql.strip()
    # remove ```sql ... ``` or ``` ... ```
    if s.startswith("```"):
        s = s.replace("```sql", "```")
        parts = s.split("```")
        # take the first fenced block content if present
        if len(parts) >= 3:
            s = parts[1].strip()
        else:
            s = s.replace("```", "").strip()
    return s

def _strip_sql_comments(sql: str) -> str:
    # remove -- line comments
    s = re.sub(r"--[^\n]*", "", sql)
    # remove /* ... */ block comments
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    return s

def _collapse_ws(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()

def _ensure_single_statement(sql: str) -> str:
    # allow optional trailing semicolon; but not multiple statements
    s = sql.strip()
    if MULTI_STMT_SEMI.search(s):
        raise ValueError("Only a single SQL statement is allowed.")
    return s.rstrip(" ;")

def _enforce_limit(sql: str, max_limit: int = 1000) -> str:
    """
    Ensure a LIMIT clause exists and is <= max_limit.
    - Appends LIMIT if missing
    - Tightens LIMIT if it's larger than max_limit
    """
    # simple detection; handle with/without OFFSET
    m = re.search(r"\bLIMIT\s+(\d+)\b", sql, flags=re.IGNORECASE)
    if not m:
        return f"{sql} LIMIT {max_limit}"
    else:
        current = int(m.group(1))
        if current > max_limit:
            sql = re.sub(r"\bLIMIT\s+\d+\b", f"LIMIT {max_limit}", sql, flags=re.IGNORECASE)
        return sql

def clean_llm_sql(raw: str) -> str:
    """
    Clean typical LLM formatting before validation.
    """
    s = _strip_code_fences(raw)
    s = _strip_sql_comments(s)
    s = _collapse_ws(s)
    # remove leading "SQL:" or similar prefixes if present
    s = re.sub(r"^\s*(SQL\s*:\s*)", "", s, flags=re.IGNORECASE)
    return s

def sanitize_query(raw_sql: str, max_limit: int = 1000) -> str:
    """
    Validate & return a safe SELECT/WITH SQL query with enforced LIMIT.
    Raises ValueError on unsafe input.
    """
    if not raw_sql or not raw_sql.strip():
        raise ValueError("Empty SQL.")

    s = clean_llm_sql(raw_sql)
    s = _ensure_single_statement(s)

    # Must start with SELECT or WITH
    if not re.match(r"^\s*(SELECT|WITH)\b", s, flags=re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed!")

    # Must not contain forbidden verbs
    if FORBIDDEN.search(s):
        raise ValueError("Detected forbidden SQL keywords.")

    # Enforce LIMIT <= max_limit
    s = _enforce_limit(s, max_limit=max_limit)

    return s
