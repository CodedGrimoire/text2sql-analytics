# src/api.py
from fastapi import FastAPI, Query
from src.text2sql_engine import Text2SQLEngine

app = FastAPI()
engine = Text2SQLEngine(debug=True)

@app.get("/ask")
def ask(question: str):
    return engine.run(question)
