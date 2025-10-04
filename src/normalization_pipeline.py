"""
Normalization Pipeline for Northwind
- Loads Excel/CSV into DataFrames
- Validates datatypes & constraints
- Handles NULLs per strategy
- Checks referential integrity (FKs)
- Exports normalized DDL & indexes to SQL
- (Optional) Seeds DB in dependency order
- Generates metrics report (json + md)
"""

from __future__ import annotations
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    from sqlalchemy import create_engine, text
except Exception:
    create_engine = None  # optional dependency

# ---------------------------
# Config models
# ---------------------------

@dataclass
class NullPolicy:
    # Per-column null policy: "drop_row", "fill_default", "leave"
    column_policy: Dict[str, str] = field(default_factory=dict)
    # Defaults applied when policy == "fill_default"
    defaults: Dict[str, object] = field(default_factory=dict)
    # Fallback for unspecified columns: "leave" | "drop_row" | "fill_default"
    fallback: str = "leave"


@dataclass
class TypeCoercionRule:
    # Target dtype per column (pandas dtype strings)
    target_dtypes: Dict[str, str] = field(default_factory=dict)
    # If True, coerce silently; else record failures
    strict: bool = False


@dataclass
class Relationship:
    # Simple FK relationship (for RI checks)
    child_table: str
    child_col: str
    parent_table: str
    parent_col: str


@dataclass
class PipelineConfig:
    # Map of source → canonical table name
    sheet_to_table: Dict[str, str]
    # Desired final column casing (lowercase, strip spaces)
    normalize_columns: bool = True
    # Null handling policy
    null_policy: NullPolicy = field(default_factory=NullPolicy)
    # Type coercion rule
    type_rule: TypeCoercionRule = field(default_factory=TypeCoercionRule)
    # Relationships to verify (RI)
    relationships: List[Relationship] = field(default_factory=list)
    # Dependency order for seeding
    seed_order: List[str] = field(default_factory=list)
    # Representative queries for EXPLAIN / index justification
    rep_queries: List[str] = field(default_factory=list)


# ---------------------------
# Pipeline
# ---------------------------

class NormalizationPipeline:
    def __init__(self, config: PipelineConfig):
        self.cfg = config
        self.tables: Dict[str, pd.DataFrame] = {}
        self.metrics: Dict[str, dict] = {}
        self.ri_findings: List[str] = []
        self.type_errors: Dict[str, List[str]] = {}

    # ---------- Loaders ----------

    def load_excel(self, path: Path) -> None:
        if not path.exists():
            raise FileNotFoundError(f"Excel not found: {path}")
        wb = pd.ExcelFile(path)
        seen = []
        for sheet, canon in self.cfg.sheet_to_table.items():
            if sheet in wb.sheet_names:
                df = pd.read_excel(wb, sheet_name=sheet)
                self.tables[canon] = df
                seen.append(sheet)
        print(f"🟢 Loaded sheets → tables: {seen} -> {list(self.tables.keys())}")

        if self.cfg.normalize_columns:
            for t, df in self.tables.items():
                df.columns = [re.sub(r"\s+", "", c).lower() for c in df.columns]
                self.tables[t] = df

    def load_csv_dir(self, folder: Path, pattern: str = "*.csv") -> None:
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder}")
        for p in folder.glob(pattern):
            canon = self.cfg.sheet_to_table.get(p.stem, p.stem.lower())
            df = pd.read_csv(p)
            if self.cfg.normalize_columns:
                df.columns = [re.sub(r"\s+", "", c).lower() for c in df.columns]
            self.tables[canon] = df
        print(f"🟢 Loaded CSVs: {list(self.tables.keys())}")

    # ---------- Validation ----------

    def _summarize(self, name: str, df: pd.DataFrame) -> dict:
        summary = {
            "rows": int(len(df)),
            "cols": int(len(df.columns)),
            "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
            "dtypes": {c: str(df[c].dtype) for c in df.columns},
            "duplicates": int(df.duplicated().sum()),
            "pk_candidates": self._pk_candidates(df),
        }
        return summary

    def _pk_candidates(self, df: pd.DataFrame) -> List[str]:
        cands = []
        for c in df.columns:
            if df[c].isna().any():
                continue
            if df[c].is_unique:
                cands.append(c)
        return cands

    def validate_and_collect_metrics(self) -> None:
        print("🔎 Collecting normalization metrics…")
        self.metrics = {}
        for name, df in self.tables.items():
            self.metrics[name] = self._summarize(name, df)

    # ---------- Type Coercion ----------

    def coerce_types(self) -> None:
        if not self.cfg.type_rule.target_dtypes:
            return
        print("🔧 Coercing dtypes…")
        for tname, df in self.tables.items():
            errs: List[str] = []
            for col, target in self.cfg.type_rule.target_dtypes.items():
                if col in df.columns:
                    try:
                        if target.startswith("datetime"):
                            self.tables[tname][col] = pd.to_datetime(df[col], errors="raise", utc=False)
                        else:
                            self.tables[tname][col] = df[col].astype(target)
                    except Exception as e:
                        msg = f"{tname}.{col} -> {target} FAILED: {e}"
                        errs.append(msg)
                        if self.cfg.type_rule.strict:
                            raise
            if errs:
                self.type_errors.setdefault(tname, []).extend(errs)

    # ---------- NULL Handling ----------

    def handle_nulls(self) -> None:
        policy = self.cfg.null_policy
        if not policy:
            return
        print("🧼 Handling NULLs…")
        for tname, df in self.tables.items():
            drop_idx = set()
            for col in df.columns:
                pol = policy.column_policy.get(col, policy.fallback)
                if pol == "leave":
                    continue
                mask = df[col].isna()
                if not mask.any():
                    continue
                if pol == "drop_row":
                    drop_idx.update(df[mask].index.tolist())
                elif pol == "fill_default":
                    val = policy.defaults.get(col, "")
                    df.loc[mask, col] = val
            if drop_idx:
                self.tables[tname] = df.drop(index=list(drop_idx)).reset_index(drop=True)

    # ---------- Referential Integrity Checks ----------

    def check_referential_integrity(self) -> None:
        self.ri_findings = []
        print("🧭 Checking referential integrity…")
        for rel in self.cfg.relationships:
            child = self.tables.get(rel.child_table)
            parent = self.tables.get(rel.parent_table)
            if child is None or parent is None or rel.child_col not in child.columns or rel.parent_col not in parent.columns:
                self.ri_findings.append(f"SKIP: {rel.child_table}.{rel.child_col} -> {rel.parent_table}.{rel.parent_col}")
                continue
            missing = ~child[rel.child_col].isin(parent[rel.parent_col])
            count_missing = int(missing.sum())
            if count_missing > 0:
                self.ri_findings.append(
                    f"❌ RI: {rel.child_table}.{rel.child_col} has {count_missing} values not in {rel.parent_table}.{rel.parent_col}"
                )
            else:
                self.ri_findings.append(
                    f"✅ RI: {rel.child_table}.{rel.child_col} → {rel.parent_table}.{rel.parent_col} OK"
                )

    # ---------- Export DDL & Indexes ----------

    def export_schema_sql(self, out_dir: Path) -> None:
        """
        Writes normalized schema (3NF) DDL similar to your PDF to:
        - 01_tables.sql
        """
        out_dir.mkdir(parents=True, exist_ok=True)
        ddl = _DDL_01_TABLES.strip() + "\n"
        (out_dir / "01_tables.sql").write_text(ddl, encoding="utf-8")
        print(f"📦 Wrote {out_dir / '01_tables.sql'}")

    def export_indexes_sql(self, out_dir: Path) -> None:
        """
        Writes index SQL (B-tree/GIN) to:
        - 03_indexes.sql
        """
        out_dir.mkdir(parents=True, exist_ok=True)
        idx = _DDL_03_INDEXES.strip() + "\n"
        (out_dir / "03_indexes.sql").write_text(idx, encoding="utf-8")
        print(f"📦 Wrote {out_dir / '03_indexes.sql'}")

    # ---------- Seed DB (Optional) ----------

    def seed_database(self, db_url: str) -> None:
        """
        Inserts data into DB in FK-safe order using pandas.to_sql (append).
        Requires user with INSERT rights.
        """
        if create_engine is None:
            raise RuntimeError("sqlalchemy is required for seeding")
        engine = create_engine(db_url)
        order = self.cfg.seed_order or list(self.tables.keys())
        print(f"🚚 Seeding tables (order): {order}")
        with engine.begin() as conn:
            for t in order:
                if t not in self.tables:
                    print(f"skip: {t}")
                    continue
                df = self.tables[t].copy()
                # pandas -> to_sql assumes matching columns
                # FK errors will raise from DB
                df.to_sql(t, conn, if_exists="append", index=False)
                print(f"  • {t}: {len(df)} rows")

    # ---------- Metrics Report ----------

    def report(self) -> dict:
        return {
            "metrics": self.metrics,
            "ri_findings": self.ri_findings,
            "type_errors": self.type_errors,
        }

    def write_report(self, out_dir: Path, name: str = "normalization_report") -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = self.report()
        (out_dir / f"{name}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        # lightweight Markdown summary
        lines = ["# Normalization Report\n"]
        for t, m in self.metrics.items():
            lines.append(f"## {t}\n- rows: {m['rows']}\n- cols: {m['cols']}\n- dups: {m['duplicates']}")
            lines.append(f"- pk_candidates: {', '.join(m['pk_candidates']) or 'none'}")
            nonzero_nulls = {k:v for k,v in m["null_counts"].items() if v>0}
            lines.append(f"- nulls: {nonzero_nulls or 'none'}\n")
        if self.ri_findings:
            lines.append("## RI Checks\n")
            lines.extend([f"- {x}" for x in self.ri_findings])
        if self.type_errors:
            lines.append("\n## Type Coercion Errors\n")
            for t, errs in self.type_errors.items():
                for e in errs:
                    lines.append(f"- {e}")
        (out_dir / f"{name}.md").write_text("\n".join(lines), encoding="utf-8")
        print(f"🧾 Wrote reports to {out_dir}/{name}.json and {name}.md")

    # ---------- (Optional) EXPLAIN helper ----------

    def explain_queries(self, db_url: str, out_dir: Path, filename: str = "explain_plans.json") -> None:
        if not self.cfg.rep_queries or create_engine is None:
            return
        engine = create_engine(db_url)
        plans = []
        with engine.begin() as conn:
            for q in self.cfg.rep_queries:
                try:
                    res = conn.execute(text(f"EXPLAIN (FORMAT JSON) {q}"))
                    plan_json = res.fetchone()[0][0]  # EXPLAIN JSON returns a one-element array
                    plans.append({"query": q, "plan": plan_json})
                except Exception as e:
                    plans.append({"query": q, "error": str(e)})
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / filename).write_text(json.dumps(plans, indent=2), encoding="utf-8")
        print(f"🧠 Saved EXPLAIN plans to {out_dir/filename}")


# ---------------------------
# SQL Text (DDL & Indexes)
# ---------------------------

_DDL_01_TABLES = r"""
-- Normalized Northwind-ish schema (3NF), idempotent
CREATE TABLE IF NOT EXISTS customers (
  customerid        VARCHAR(10) PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  contactname       VARCHAR(100),
  contacttitle      VARCHAR(50),
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  phone             VARCHAR(30),
  fax               VARCHAR(30),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS suppliers (
  supplierid        SERIAL PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  contactname       VARCHAR(100),
  contacttitle      VARCHAR(50),
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  phone             VARCHAR(30),
  fax               VARCHAR(30),
  homepage          TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS categories (
  categoryid        SERIAL PRIMARY KEY,
  categoryname      VARCHAR(50)  NOT NULL UNIQUE,
  description       TEXT,
  picture           BYTEA,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shippers (
  shipperid         SERIAL PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  phone             VARCHAR(30),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employees (
  employeeid        SERIAL PRIMARY KEY,
  lastname          VARCHAR(20)  NOT NULL,
  firstname         VARCHAR(10)  NOT NULL,
  title             VARCHAR(30),
  titleofcourtesy   VARCHAR(25),
  birthdate         DATE,
  hiredate          DATE,
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  homephone         VARCHAR(30),
  extension         VARCHAR(10),
  notes             TEXT,
  reportsto         INT,
  photopath         VARCHAR(255),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_employees_reportsto
    FOREIGN KEY (reportsto) REFERENCES employees(employeeid)
    ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS products (
  productid         SERIAL PRIMARY KEY,
  productname       VARCHAR(100) NOT NULL,
  supplierid        INT REFERENCES suppliers(supplierid) ON UPDATE CASCADE ON DELETE RESTRICT,
  categoryid        INT REFERENCES categories(categoryid) ON UPDATE CASCADE ON DELETE SET NULL,
  quantityperunit   VARCHAR(50),
  unitprice         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (unitprice >= 0),
  unitsinstock      INT NOT NULL DEFAULT 0 CHECK (unitsinstock >= 0),
  unitsonorder      INT NOT NULL DEFAULT 0 CHECK (unitsonorder >= 0),
  reorderlevel      INT NOT NULL DEFAULT 0 CHECK (reorderlevel >= 0),
  discontinued      BOOLEAN NOT NULL DEFAULT FALSE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  orderid           SERIAL PRIMARY KEY,
  customerid        VARCHAR(10) REFERENCES customers(customerid) ON UPDATE CASCADE ON DELETE RESTRICT,
  employeeid        INT REFERENCES employees(employeeid)       ON UPDATE CASCADE ON DELETE SET NULL,
  orderdate         DATE,
  requireddate      DATE,
  shippeddate       DATE,
  shipperid         INT REFERENCES shippers(shipperid)         ON UPDATE CASCADE ON DELETE SET NULL,
  freight           NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (freight >= 0),
  shipname          VARCHAR(100),
  shipaddress       VARCHAR(120),
  shipcity          VARCHAR(50),
  shipregion        VARCHAR(50),
  shippostalcode    VARCHAR(20),
  shipcountry       VARCHAR(50),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orderdetails (
  orderid           INT NOT NULL,
  productid         INT NOT NULL,
  unitprice         NUMERIC(12,2) NOT NULL CHECK (unitprice >= 0),
  quantity          INT NOT NULL CHECK (quantity > 0),
  discount          NUMERIC(4,3) NOT NULL DEFAULT 0 CHECK (discount >= 0 AND discount <= 1),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (orderid, productid),
  CONSTRAINT fk_orderdetails_order
    FOREIGN KEY (orderid)  REFERENCES orders(orderid)   ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_orderdetails_product
    FOREIGN KEY (productid) REFERENCES products(productid) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS region (
  regionid          INT PRIMARY KEY,
  regiondescription VARCHAR(50) NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS territories (
  territoryid       VARCHAR(20) PRIMARY KEY,
  territorydescription VARCHAR(50) NOT NULL,
  regionid          INT NOT NULL REFERENCES region(regionid) ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employeeterritories (
  employeeid        INT NOT NULL REFERENCES employees(employeeid) ON UPDATE CASCADE ON DELETE CASCADE,
  territoryid       VARCHAR(20) NOT NULL REFERENCES territories(territoryid) ON UPDATE CASCADE ON DELETE CASCADE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (employeeid, territoryid)
);

CREATE TABLE IF NOT EXISTS customerdemographics (
  customertypeid    VARCHAR(10) PRIMARY KEY,
  customerdesc      TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customercustomerdemo (
  customerid        VARCHAR(10) NOT NULL REFERENCES customers(customerid) ON DELETE CASCADE,
  customertypeid    VARCHAR(10) NOT NULL REFERENCES customerdemographics(customertypeid) ON DELETE CASCADE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (customerid, customertypeid)
);
"""

_DDL_03_INDEXES = r"""
-- B-tree on common joins/filters
CREATE INDEX IF NOT EXISTS idx_orders_customerid       ON orders(customerid);
CREATE INDEX IF NOT EXISTS idx_orders_employeeid       ON orders(employeeid);
CREATE INDEX IF NOT EXISTS idx_orders_orderdate        ON orders(orderdate);
CREATE INDEX IF NOT EXISTS idx_orderdetails_productid  ON orderdetails(productid);
CREATE INDEX IF NOT EXISTS idx_products_categoryid     ON products(categoryid);
CREATE INDEX IF NOT EXISTS idx_products_supplierid     ON products(supplierid);

-- Optional text search helpers (requires pg_trgm)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS gin_customers_companyname ON customers USING GIN (companyname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_products_productname  ON products  USING GIN (productname  gin_trgm_ops);
"""


# ---------------------------
# Canonical config helper
# ---------------------------

def default_config() -> PipelineConfig:
    return PipelineConfig(
        sheet_to_table={
            "Customers": "customers",
            "Suppliers": "suppliers",
            "Categories": "categories",
            "Shippers": "shippers",
            "Employees": "employees",
            "Orders": "orders",
            "Order Details": "orderdetails",
            "Products": "products",
            "Region": "region",
            "Territories": "territories",
            "EmployeeTerritories": "employeeterritories",
            "CustomerDemographics": "customerdemographics",
            "CustomerCustomerDemo": "customercustomerdemo",
        },
        null_policy=NullPolicy(
            column_policy={
                # examples: fill missing freight/discount to 0, productname leave, etc.
                "freight": "fill_default", "discount": "fill_default",
            },
            defaults={"freight": 0, "discount": 0},
            fallback="leave",
        ),
        type_rule=TypeCoercionRule(
            target_dtypes={
                "orderdate": "datetime64[ns]",
                "requireddate": "datetime64[ns]",
                "shippeddate": "datetime64[ns]",
                "unitprice": "float64",
                "quantity": "int64",
                "discount": "float64",
            },
            strict=False,
        ),
        relationships=[
            Relationship("orders", "customerid", "customers", "customerid"),
            Relationship("orders", "employeeid", "employees", "employeeid"),
            Relationship("orders", "shipperid", "shippers", "shipperid"),
            Relationship("orderdetails", "orderid", "orders", "orderid"),
            Relationship("orderdetails", "productid", "products", "productid"),
            Relationship("products", "supplierid", "suppliers", "supplierid"),
            Relationship("products", "categoryid", "categories", "categoryid"),
            Relationship("territories", "regionid", "region", "regionid"),
            Relationship("employeeterritories", "employeeid", "employees", "employeeid"),
            Relationship("employeeterritories", "territoryid", "territories", "territoryid"),
            Relationship("customercustomerdemo", "customerid", "customers", "customerid"),
            Relationship("customercustomerdemo", "customertypeid", "customerdemographics", "customertypeid"),
        ],
        seed_order=[
            "customers", "suppliers", "categories", "shippers", "employees",
            "region", "territories", "employeeterritories", "customerdemographics",
            "customercustomerdemo", "products", "orders", "orderdetails",
        ],
        rep_queries=[
            "SELECT c.country, COUNT(*) FROM customers c GROUP BY c.country ORDER BY COUNT(*) DESC LIMIT 10",
            "SELECT e.employeeid, COUNT(*) FROM orders o JOIN employees e ON e.employeeid=o.employeeid GROUP BY e.employeeid ORDER BY COUNT(*) DESC LIMIT 5",
            "SELECT p.productid, SUM(od.unitprice*od.quantity*(1-discount)) AS sales FROM orderdetails od JOIN products p ON p.productid=od.productid GROUP BY p.productid ORDER BY sales DESC LIMIT 5"
        ],
    )
