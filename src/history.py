# src/history.py
import sqlite3, datetime

class QueryHistory:
    def __init__(self, db="data/query_history.db"):
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
        self.conn.execute("INSERT INTO history (question, sql, success, timestamp) VALUES (?, ?, ?, ?)",
                          (question, sql, success, datetime.datetime.utcnow().isoformat()))
        self.conn.commit()
