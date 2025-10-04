"""
CLI wrapper around NormalizationPipeline
Usage:
  python src/data_loader.py --excel data/raw/northwind.xlsx --out data/outputs
  python src/data_loader.py --csvdir data/raw/csvs --out data/outputs
"""

import argparse
from pathlib import Path   # ✅ FIXED
from src.normalization_pipeline import NormalizationPipeline, default_config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", type=Path, help="Path to Excel workbook")
    ap.add_argument("--csvdir", type=Path, help="Path to folder with CSVs")
    ap.add_argument("--out", type=Path, default=Path("data/outputs"), help="Output folder for reports/sql")
    ap.add_argument("--export-sql", action="store_true", help="Export normalized schema & indexes SQL")
    args = ap.parse_args()

    cfg = default_config()
    pipe = NormalizationPipeline(cfg)

    if args.excel:
        pipe.load_excel(args.excel)
    elif args.csvdir:
        pipe.load_csv_dir(args.csvdir)
    else:
        ap.error("Provide --excel or --csvdir")

    pipe.coerce_types()
    pipe.handle_nulls()
    pipe.validate_and_collect_metrics()
    pipe.check_referential_integrity()

    pipe.write_report(args.out)

    if args.export_sql:
        sql_dir = Path("data/schema")
        pipe.export_schema_sql(sql_dir)
        pipe.export_indexes_sql(sql_dir)

    print("✅ Normalization pipeline complete.")


if __name__ == "__main__":
    main()


# ---------------- Backward-compatible alias for tests ----------------
class DataLoader(NormalizationPipeline):
    """
    Backward compatibility for tests expecting a DataLoader class.
    Wraps NormalizationPipeline with default_config().
    """
    def __init__(self, **kwargs):
        cfg = default_config()
        for k, v in kwargs.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        super().__init__(config=cfg)

    def load_excel(self, path=None):
        """
        Backward-compatible load_excel wrapper for tests.
        """
        if path is None:
            path = Path("data/raw/northwind.xlsx")  # ✅ FIXED: Path imported
        return super().load_excel(path)
