"""
src/query_validator.py

Regex-based SQL validator and sanitizer.
Ensures queries are safe before execution (SELECT-only).
"""

import re


FORBIDDEN_KEYWORDS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bCREATE\b",
    r"\bTRUNCATE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCOMMIT\b",
    r"\bROLLBACK\b",
]


def validate_sql(query: str) -> bool:
    """
    Validate SQL query to allow only SELECT statements.

    Args:
        query (str): Input SQL query.

    Returns:
        bool: True if safe, False otherwise.
    """
    query = query.strip().rstrip(";")

    # Must start with SELECT
    if not re.match(r"^SELECT", query, re.IGNORECASE):
        return False

    # Reject forbidden keywords
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(keyword, query, re.IGNORECASE):
            return False

    return True


def enforce_limit(query: str, limit: int = 1000) -> str:
    """
    Ensure a LIMIT clause exists in the query.

    Args:
        query (str): SQL query.
        limit (int): Default row limit.

    Returns:
        str: Query with enforced LIMIT.
    """
    query = query.strip().rstrip(";")

    if re.search(r"\bLIMIT\b", query, re.IGNORECASE):
        return query + ";"

    return f"{query} LIMIT {limit};"


def sanitize_query(query: str, limit: int = 1000) -> str:
    """
    Full sanitization pipeline:
    - Validate SQL
    - Enforce LIMIT

    Args:
        query (str): SQL query.
        limit (int): Row limit.

    Returns:
        str: Sanitized query.

    Raises:
        ValueError: If query is unsafe.
    """
    if not validate_sql(query):
        raise ValueError("Unsafe or non-SELECT SQL query detected.")

    return enforce_limit(query, limit)


if __name__ == "__main__":
    # Demo tests
    safe_query = "SELECT * FROM Customers"
    print("Safe:", sanitize_query(safe_query))

    bad_query = "DELETE FROM Customers"
    try:
        print(sanitize_query(bad_query))
    except ValueError as e:
        print("❌ Blocked:", e)
