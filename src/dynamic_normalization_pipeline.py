# src/dynamic_normalization_pipeline.py
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from dataclasses import dataclass
from src.utils import SAFE_NAME, infer_sql_type, pk_score, _ID_HINTS

try:
    from sqlalchemy import create_engine
except Exception:
    create_engine = None  # optional dependency

@dataclass
class DynConfig:
    normalize_columns: bool = True
    fk_match_threshold: float = 0.8
    build_gin_for_text: bool = True
    add_audit_cols: bool = True

class DynamicNormalizationPipeline:
    def __init__(self, config: Optional[DynConfig] = None):
        self.cfg = config or DynConfig()
        self.tables: Dict[str, pd.DataFrame] = {}
        self.col_types: Dict[str, Dict[str, str]] = {}
        self.pks: Dict[str, str] = {}
        self.fks: Dict[str, List[Tuple[str, str, str]]] = {}
        self.metrics: Dict[str, dict] = {}

    # ---------- Loaders ----------
    def load_excel(self, path: Path) -> None:
        wb = pd.ExcelFile(path)
        for sheet in wb.sheet_names:
            df = pd.read_excel(wb, sheet_name=sheet)
            tname = SAFE_NAME(sheet)
            if self.cfg.normalize_columns:
                df.columns = [SAFE_NAME(c) for c in df.columns]
            self.tables[tname] = df
        print(f"🟢 Loaded sheets → tables: {list(self.tables.keys())}")

    # ---------- Inference ----------
    def infer_column_types(self) -> None:
        for t, df in self.tables.items():
            self.col_types[t] = {c: infer_sql_type(df[c]) for c in df.columns}

    def infer_primary_keys(self) -> None:
        for t, df in self.tables.items():
            best = (-1, None)
            for c in df.columns:
                unique, notnull = df[c].is_unique, not df[c].isna().any()
                score = pk_score(c, unique, notnull, t)
                if score > best[0]:
                    best = (score, c)
            if best[1]:
                self.pks[t] = best[1]
            else:
                # add surrogate key
                synth = "id"
                i = 1
                while synth in df.columns:
                    i += 1
                    synth = f"id{i}"
                df.insert(0, synth, range(1, len(df)+1))
                self.col_types[t][synth] = "BIGSERIAL"
                self.pks[t] = synth

    def infer_foreign_keys(self) -> None:
        self.fks = {t: [] for t in self.tables}
        pk_sets: Dict[str, Set] = {
            pt: set(self.tables[pt][pk].dropna().unique().tolist())
            for pt, pk in self.pks.items()
        }
        for ct, df in self.tables.items():
            for col in df.columns:
                if col == self.pks[ct]:
                    continue
                if not _ID_HINTS.search(col):
                    continue
                for pt, parent_pk in self.pks.items():
                    if pt == ct:
                        continue
                    child_vals = set(df[col].dropna().unique().tolist())
                    if not child_vals:
                        continue
                    parent_vals = pk_sets[pt]
                    hit = len(child_vals & parent_vals)
                    frac = hit / max(1, len(child_vals))
                    if frac >= self.cfg.fk_match_threshold:
                        self.fks[ct].append((col, pt, parent_pk))
                        break

    # ---------- Reports ----------
    def collect_metrics(self) -> None:
        for t, df in self.tables.items():
            self.metrics[t] = {
                "rows": len(df),
                "cols": len(df.columns),
                "pk": self.pks.get(t),
                "fks": self.fks.get(t, []),
            }

    def write_report(self, out_dir: Path, name="dynamic_report") -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{name}.json").write_text(
            json.dumps({"metrics": self.metrics, "pks": self.pks, "fks": self.fks, "types": self.col_types}, indent=2),
            encoding="utf-8"
        )
        print(f"🧾 Wrote reports to {out_dir}/{name}.json")

    # ---------- SQL Emit ----------
    def _emit_table_sql(self, t: str) -> str:
        df = self.tables[t]
        pk = self.pks[t]
        cols = []
        for c in df.columns:
            sqlt = self.col_types[t].get(c, "TEXT")
            line = f'  {c} {sqlt}'
            if not df[c].isna().any():
                line += " NOT NULL"
            cols.append(line)
        if self.cfg.add_audit_cols:
            cols.append("  created_at TIMESTAMPTZ NOT NULL DEFAULT now()")
            cols.append("  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()")
        cols.append(f"  PRIMARY KEY ({pk})")
        for (col, pt, parent_pk) in self.fks.get(t, []):
            cols.append(f"  ,FOREIGN KEY ({col}) REFERENCES {pt}({parent_pk}) ON UPDATE CASCADE ON DELETE RESTRICT")
        return f"CREATE TABLE IF NOT EXISTS {t} (\n{',\n'.join(cols)}\n);"

    def export_schema_sql(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        ddl = "-- Auto-generated schema (dynamic)\n" + "\n\n".join([self._emit_table_sql(t) for t in self.tables]) + "\n"
        (out_dir / "01_tables.sql").write_text(ddl, encoding="utf-8")
        print(f"📦 Wrote {out_dir / '01_tables.sql'}")

    def export_indexes_sql(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        lines = ["-- Auto-generated indexes (dynamic)"]
        for t, fks in self.fks.items():
            for (col, pt, pk) in fks:
                lines.append(f"CREATE INDEX IF NOT EXISTS idx_{t}_{col} ON {t}({col});")
        (out_dir / "03_indexes.sql").write_text("\n".join(lines), encoding="utf-8")
        print(f"📦 Wrote {out_dir / '03_indexes.sql'}")

    # ---------- Orchestrator ----------
    def run_all(self, out_dir: Path) -> None:
        self.infer_column_types()
        self.infer_primary_keys()
        self.infer_foreign_keys()
        self.collect_metrics()
        self.write_report(out_dir)
        self.export_schema_sql(Path("data/schema"))
        self.export_indexes_sql(Path("data/schema"))
        print("✅ Dynamic normalization complete.")

    # ---------- Seeding ----------
    def seed_database(self, db_url: str) -> None:
        if create_engine is None:
            raise RuntimeError("sqlalchemy required")
        eng = create_engine(db_url, future=True)
        with eng.begin() as conn:
            for t, df in self.tables.items():
                df.to_sql(t, conn, if_exists="append", index=False)
                print(f"  • {t}: {len(df)} rows inserted")
