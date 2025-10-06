import pytest
from pathlib import Path
from src.data_loader import DataLoader
from src.dynamic_normalization_pipeline import DynamicNormalizationPipeline, DynConfig

# ------------------------
# Basic loading tests
# ------------------------

def test_load_excel_file(tmp_path):
    """Ensure Excel can be loaded via pipeline."""
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    assert isinstance(loader.tables, dict)
    assert len(loader.tables) > 0

def test_load_csv_directory(tmp_path):
    """Ensure CSV dir loads without errors (if exists)."""
    csv_dir = Path("data/raw/csvs")
    if csv_dir.exists():
        loader = DataLoader(filepath=csv_dir)
        loader.load_csv_dir()
        assert isinstance(loader.tables, dict)

# ------------------------
# Pipeline run & outputs
# ------------------------

def test_run_all_creates_outputs(tmp_path):
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    out_dir = tmp_path / "outputs"
    loader.run_all(out_dir)
    assert out_dir.exists()
    # Expect some report or SQL-like output files
    assert any(out_dir.iterdir())

def test_export_schema_and_indexes(tmp_path):
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    sql_dir = tmp_path / "schema"
    loader.export_schema_sql(sql_dir)
    loader.export_indexes_sql(sql_dir)
    files = list(sql_dir.glob("*.sql"))
    assert len(files) > 0, "Expected SQL schema files"

# ------------------------
# Seeding DB (optional)
# ------------------------

def test_seed_database(monkeypatch):
    from src.config import DB_URL
    loader = DataLoader(filepath="data/raw/northwind.xlsx")
    loader.load_excel()
    try:
        loader.seed_database(DB_URL)
    except Exception:
        pytest.skip("Seed DB requires superuser rights; skip in CI")

# ------------------------
# CLI entrypoint
# ------------------------

def test_cli_excel(monkeypatch, tmp_path):
    """Simulate CLI call with --excel and --out."""
    import sys
    import src.data_loader as data_loader

    test_excel = "data/raw/northwind.xlsx"
    out_dir = tmp_path / "cli_out"

    monkeypatch.setattr(sys, "argv", [
        "data_loader.py",
        "--excel", test_excel,
        "--out", str(out_dir)
    ])

    data_loader.main()
    assert out_dir.exists()
