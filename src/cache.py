# src/cache.py
from functools import lru_cache
import pandas as pd

class QueryCache:
    def __init__(self, maxsize=100):
        self._cache = lru_cache(maxsize=maxsize)(self._run_query)

    def _run_query(self, sql: str, engine):
        return pd.read_sql(sql, engine)

    def get(self, sql: str, engine):
        return self._cache(sql, engine)
