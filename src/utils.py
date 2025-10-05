# src/utils.py
import re
import pandas as pd

_ID_HINTS = re.compile(r"(?:^|_)(id|.*_id)$")

def SAFE_NAME(s: str) -> str:
    """Normalize strings into safe SQL identifiers."""
    return re.sub(r"[^a-z0-9_]", "_", str(s).lower().strip())

def is_int_like(series: pd.Series) -> bool:
    if pd.api.types.is_integer_dtype(series):
        return True
    if pd.api.types.is_float_dtype(series):
        s = series.dropna()
        return (s == s.round()).all()
    return False

def infer_sql_type(series: pd.Series) -> str:
    """Infer SQL type from pandas Series."""
    s = series.dropna()
    if s.empty:
        return "TEXT"
    if pd.api.types.is_bool_dtype(s):
        return "BOOLEAN"
    if is_int_like(s):
        try:
            vmin, vmax = int(s.min()), int(s.max())
            if -(2**31) <= vmin and vmax < 2**31:
                return "INTEGER"
            return "BIGINT"
        except Exception:
            return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "NUMERIC(18,6)"
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if not dt.isna().all():
            if dt.dt.time.eq(pd.Timestamp(0).time()).all():
                return "DATE"
            return "TIMESTAMPTZ"
    except Exception:
        pass
    s2 = s.astype(str)
    maxlen = int(s2.str.len().max())
    if maxlen <= 255:
        bucket = 50 if maxlen <= 50 else (100 if maxlen <= 100 else 255)
        return f"VARCHAR({bucket})"
    return "TEXT"

def pk_score(colname: str, unique: bool, notnull: bool, table: str) -> int:
    score = 0
    if unique and notnull: score += 3
    if colname == "id": score += 3
    if colname.endswith("_id"): score += 2
    if colname == f"{table}id": score += 2
    return score
