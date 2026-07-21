# Ecommerce Data Platform — Complete Build Reference Guide

**End-to-end reference for the modern data stack portfolio project.**

Source (PostgreSQL, 14 tables) → Bronze / Silver / Gold in Snowflake → Airflow orchestration → Power BI dashboards.

This guide covers every step, every command, and every code artifact needed to reproduce the build from an empty folder to a published GitHub repository. It is organized as a linear build script — follow top-to-bottom.

---

## Table of Contents

**Phase 1 — Setup**
- Step 1: Project Architecture & Environment Setup
- Step 2: Snowflake Setup (Warehouse, Database, Schemas, Service User)

**Phase 2 — Pipeline**
- Step 3: Bronze Layer — Postgres to Snowflake Raw Ingestion
- Step 4: Silver Layer — Cleansing, Type-Conformance, Deduplication

**Phase 3 — Model**
- Step 5: Gold Layer — Dimensional Model Design
- Step 6: Gold Layer — Dimensions
- Step 7: Gold Layer — Fact Tables
- Step 8: Pipeline Consolidation + Semantic Views

**Phase 4 — Orchestration**
- Step 9: Airflow Orchestration (Dockerized)

**Phase 5 — BI**
- Step 10: Power BI — Connect, Import the Star, Build the Model
- Step 11: DAX Measures
- Step 12: Dashboard Pages

**Phase 6 — Packaging**
- Step 13: Portfolio Packaging

**Appendix**
- Troubleshooting & Gotchas
- Key architectural decisions
- Full file & folder tree (final state)

---

## Architecture at a glance

```
PostgreSQL (source, 14 tables)
        │
        ▼  Python EL (pandas + SQLAlchemy)
BRONZE  ─ raw mirror + audit columns
        │
        ▼  SQL transforms
SILVER  ─ cleansed, typed, deduplicated
        │
        ▼  SQL transforms + SCD Type 2
GOLD    ─ Kimball star (5 dims + 6 facts + views)
        │
        ▼  Import mode
Power BI ─ star-schema model + DAX + 4 dashboards

Apache Airflow (Dockerized) orchestrates the entire pipeline.
```

**Tech stack**: Python 3.10, PostgreSQL, Snowflake, Docker, Apache Airflow 2.10.5, Power BI Desktop, DAX.

**Prerequisites**: WSL2 Ubuntu (or Linux/Mac), Python 3.10+, PostgreSQL with 14 source tables loaded, a Snowflake trial account (Step 2 sets this up), Docker Desktop with WSL integration (Step 9), Power BI Desktop on Windows (Step 10).

**Estimated effort**: ~47 hours across 6 phases (roughly one focused working week).

---

## Step 1: Project Architecture & Environment Setup

Establish the project skeleton, Python environment, and connectivity to the existing PostgreSQL source.

### 1.1 Project skeleton

```bash
mkdir ecommerce-data-platform && cd ecommerce-data-platform
mkdir -p airflow/{dags,plugins,logs} \
  src/{ingestion,transformations,quality,utils} \
  sql/ddl/{bronze,silver,gold} sql/dml \
  powerbi/{model,measures} tests docker
```

**WSL note**: Create the project in the native WSL filesystem (`~/ecommerce-data-platform`) instead of `/mnt/c/...` for 5-10x faster pip installs and file I/O.

### 1.2 `.env.example` — the template

Create `.env.example` at the project root:

```
# PostgreSQL (source) — your existing DB
PG_HOST=localhost
PG_PORT=5432
PG_USER=
PG_PASSWORD=
PG_DB=
PG_SCHEMA=public

# Snowflake (warehouse) — fill in at Step 2
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DW
```

Copy it to `.env` and fill in your real PostgreSQL credentials. **Never commit `.env`** — Step 13 sets up `.gitignore` to keep it out of git.

### 1.3 `requirements.txt`

```
psycopg2-binary==2.9.9
sqlalchemy==2.0.30
snowflake-connector-python==3.10.1
snowflake-sqlalchemy==1.6.1
pandas==2.2.2
pyarrow==16.0.0
python-dotenv==1.0.1
loguru==0.7.2
pyyaml==6.0.1
```

### 1.4 Python virtual environment

```bash
python -m venv .venv
source .venv/bin/activate                # Linux/Mac/WSL
# .venv\Scripts\activate                # Windows PowerShell
pip install --upgrade pip
pip install -r requirements.txt
```

### 1.5 Create package `__init__.py` files

```bash
touch src/__init__.py src/utils/__init__.py
```

These make the `-m` module invocation work.

### 1.6 `src/utils/db.py` — PostgreSQL connection helper

```python
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_postgres_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )
    return create_engine(url, pool_pre_ping=True)
```

### 1.7 `src/utils/inventory_source.py` — connectivity + row-count sanity check

```python
from sqlalchemy import text
from src.utils.db import get_postgres_engine

EXPECTED_TABLES = [
    "categories", "suppliers", "products", "inventory",
    "customers", "marketing_campaigns", "coupons", "orders",
    "order_items", "payments", "shipments", "returns",
    "reviews", "web_sessions",
]

def main():
    engine = get_postgres_engine()
    schema = "public"

    print(f"{'Table':<25} {'Rows':>12}")
    print("-" * 40)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """), {"schema": schema})
        present = {r[0] for r in result}

        missing = [t for t in EXPECTED_TABLES if t not in present]
        if missing:
            print(f"WARNING — missing tables: {missing}")

        for tbl in EXPECTED_TABLES:
            if tbl not in present:
                continue
            cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{tbl}";')).scalar()
            print(f"{tbl:<25} {cnt:>12,}")

if __name__ == "__main__":
    main()
```

Run it:

```bash
python -m src.utils.inventory_source
```

Expected output: a table listing all 14 source tables and their row counts (categories=15, suppliers=20, products=300, inventory=300, customers=2000, marketing_campaigns=30, coupons=50, orders=5000, order_items=15000, payments=5000, shipments=3277, returns=1000, reviews=1979, web_sessions=8000).

### Checklist before Step 2

1. Folder structure created.
2. `.venv` active and dependencies installed.
3. `.env` filled in with real PostgreSQL credentials.
4. `python -m src.utils.inventory_source` runs cleanly, prints row counts for all 14 tables.

---

## Step 2: Snowflake Setup (Warehouse, Database, Schemas, Service User)

Provision the cloud warehouse with proper RBAC: dedicated warehouse, database, three medallion schemas, custom role, and least-privilege service user.

### 2.1 Sign up for the Snowflake free trial

1. Go to https://signup.snowflake.com/
2. Choose **Standard** edition, any cloud/region.
3. Save your **account locator** — the string before `.snowflakecomputing.com`. Format: `<locator>.<region>.<cloud>`.
4. Trial gives 30 days + ~$400 in credits — more than enough.

### 2.2 Provision warehouse, database, schemas, role, user

In Snowsight, run this as `ACCOUNTADMIN`:

```sql
-- Warehouse (dedicated compute, auto-suspend to save credits)
CREATE OR REPLACE WAREHOUSE WH_ECOM_XS
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE;

-- Database and medallion schemas
CREATE OR REPLACE DATABASE ECOMMERCE_DW;
CREATE OR REPLACE SCHEMA ECOMMERCE_DW.BRONZE;
CREATE OR REPLACE SCHEMA ECOMMERCE_DW.SILVER;
CREATE OR REPLACE SCHEMA ECOMMERCE_DW.GOLD;
DROP SCHEMA IF EXISTS ECOMMERCE_DW.PUBLIC;

-- Custom role (least privilege)
CREATE OR REPLACE ROLE ECOM_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_ECOM_XS TO ROLE ECOM_ENGINEER;
GRANT USAGE ON DATABASE ECOMMERCE_DW TO ROLE ECOM_ENGINEER;

GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE STAGE
    ON SCHEMA ECOMMERCE_DW.BRONZE TO ROLE ECOM_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE STAGE
    ON SCHEMA ECOMMERCE_DW.SILVER TO ROLE ECOM_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE STAGE
    ON SCHEMA ECOMMERCE_DW.GOLD TO ROLE ECOM_ENGINEER;

-- Future grants (auto-apply to objects created later by the pipeline)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES
    IN SCHEMA ECOMMERCE_DW.BRONZE TO ROLE ECOM_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES
    IN SCHEMA ECOMMERCE_DW.SILVER TO ROLE ECOM_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES
    IN SCHEMA ECOMMERCE_DW.GOLD TO ROLE ECOM_ENGINEER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECOMMERCE_DW.GOLD TO ROLE ECOM_ENGINEER;

-- Service user for the Python pipeline
CREATE OR REPLACE USER ECOM_PIPELINE
    PASSWORD = 'ChangeMe_Strong123!'
    DEFAULT_ROLE = ECOM_ENGINEER
    DEFAULT_WAREHOUSE = WH_ECOM_XS
    DEFAULT_NAMESPACE = ECOMMERCE_DW.BRONZE
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE ECOM_ENGINEER TO USER ECOM_PIPELINE;

-- Wire the custom role into the admin hierarchy
GRANT ROLE ECOM_ENGINEER TO ROLE SYSADMIN;
```

### 2.3 Update `.env` with Snowflake credentials

```
# Snowflake
SNOWFLAKE_ACCOUNT=<locator>.<region>.<cloud>
SNOWFLAKE_USER=ECOM_PIPELINE
SNOWFLAKE_PASSWORD=ChangeMe_Strong123!
SNOWFLAKE_ROLE=ECOM_ENGINEER
SNOWFLAKE_WAREHOUSE=WH_ECOM_XS
SNOWFLAKE_DATABASE=ECOMMERCE_DW
```

### 2.4 Extend `src/utils/db.py` with Snowflake helpers

Overwrite `src/utils/db.py`:

```python
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL as SF_URL
import snowflake.connector

load_dotenv()

# ---------- PostgreSQL (source) ----------
def get_postgres_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )
    return create_engine(url, pool_pre_ping=True)

# ---------- Snowflake (warehouse) ----------
def _sf_kwargs(schema=None):
    return dict(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema or "PUBLIC",
    )

def get_snowflake_connection(schema=None):
    """Raw snowflake-connector cursor. Best for DDL / execute_string."""
    return snowflake.connector.connect(**_sf_kwargs(schema))

def get_snowflake_engine(schema=None):
    """SQLAlchemy engine. Best for pandas.to_sql / read_sql."""
    return create_engine(SF_URL(**_sf_kwargs(schema)))
```

### 2.5 `src/utils/test_snowflake.py` — connection sanity check

```python
from src.utils.db import get_snowflake_connection

def main():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(),
               CURRENT_ACCOUNT(), CURRENT_VERSION();
    """)
    print(cur.fetchone())

    cur.execute("""
        SELECT SCHEMA_NAME FROM ECOMMERCE_DW.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME IN ('BRONZE','SILVER','GOLD') ORDER BY SCHEMA_NAME;
    """)
    print("Schemas present:", [r[0] for r in cur.fetchall()])
    cur.close(); conn.close()
    print("Snowflake connectivity OK.")

if __name__ == "__main__":
    main()
```

Run it:

```bash
python -m src.utils.test_snowflake
```

Expected: prints `(ECOM_ENGINEER, WH_ECOM_XS, ECOMMERCE_DW, ...)` followed by `Schemas present: ['BRONZE', 'GOLD', 'SILVER']`, then `Snowflake connectivity OK.`

### Checklist before Step 3

1. Snowflake trial active.
2. Warehouse, database, schemas, role, user provisioned.
3. `.env` has real Snowflake values.
4. `python -m src.utils.test_snowflake` prints `Snowflake connectivity OK.`
5. `CURRENT_ROLE()` in the test output shows `ECOM_ENGINEER` (not `ACCOUNTADMIN`).

---



## Step 3: Bronze Layer — Postgres → Snowflake Raw Ingestion

## What we're building

A single command — `python -m src.ingestion.run_bronze` — that:

1. Reads all 14 source tables from Postgres into pandas DataFrames.
2. Adds three audit columns to each: `_LOADED_AT`, `_INGESTION_ID`, `_SOURCE_SYSTEM`.
3. Loads each DataFrame to Snowflake `BRONZE.<TABLE_NAME>`, full refresh per run (truncate-and-load).
4. Logs every table's outcome to `BRONZE._INGESTION_LOG` — start time, end time, rows extracted, rows loaded, status, error message if any.
5. Is idempotent: re-running produces the same final state, no duplicates.

The Bronze layer's only job is to be a faithful, append-only raw mirror. We don't clean, dedupe, or cast types here — that's Silver's job in Step 5. Bronze just needs to be reliable and auditable.

## Architecture decisions

- **Truncate-and-load, not incremental.** With 42K rows total across 14 tables, full refresh runs in seconds and is dramatically simpler. We'll add incremental logic later if it becomes a portfolio talking point (Step 4.5 optional).
- **pandas + snowflake-sqlalchemy for loads.** Snowflake's `write_pandas` is faster but requires explicit table creation; `df.to_sql(..., if_exists="replace")` handles DDL inference automatically — perfectly fine for Bronze volumes.
- **Audit columns on every row.** This is what makes Bronze trustworthy. We can always answer "which run produced this row?" — essential for debugging.
- **Centralized config.** A single `config.py` lists every table being ingested, so adding/removing one is a one-line change.

## File layout

You're creating three new files under `src/ingestion/`:

```
src/ingestion/
├── __init__.py
├── config.py
├── bronze.py
├── run_bronze.py
└── verify_bronze.py
```

```bash
cd ~/ecommerce-data-platform
touch src/ingestion/__init__.py
touch src/ingestion/config.py
touch src/ingestion/bronze.py
touch src/ingestion/run_bronze.py
touch src/ingestion/verify_bronze.py
```

## 3.1 `src/ingestion/config.py`

```python
"""
Ingestion configuration. Single source of truth for which tables
move from the Postgres source to the Snowflake BRONZE schema.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class IngestionTable:
    source_schema: str
    source_table: str
    target_table: str            # in BRONZE schema (uppercase, Snowflake convention)
    primary_key: str | None = None


SOURCE_SCHEMA = os.getenv("PG_SCHEMA", "public")

# Ordered parent → child. Order doesn't matter for full-refresh Bronze,
# but it'll matter later for Silver/Gold dependency-aware runs.
INGESTION_TABLES: list[IngestionTable] = [
    IngestionTable(SOURCE_SCHEMA, "categories",          "CATEGORIES",          "category_id"),
    IngestionTable(SOURCE_SCHEMA, "suppliers",           "SUPPLIERS",           "supplier_id"),
    IngestionTable(SOURCE_SCHEMA, "products",            "PRODUCTS",            "product_id"),
    IngestionTable(SOURCE_SCHEMA, "inventory",           "INVENTORY",           "inventory_id"),
    IngestionTable(SOURCE_SCHEMA, "customers",           "CUSTOMERS",           "customer_id"),
    IngestionTable(SOURCE_SCHEMA, "marketing_campaigns", "MARKETING_CAMPAIGNS", "campaign_id"),
    IngestionTable(SOURCE_SCHEMA, "coupons",             "COUPONS",             "coupon_id"),
    IngestionTable(SOURCE_SCHEMA, "orders",              "ORDERS",              "order_id"),
    IngestionTable(SOURCE_SCHEMA, "order_items",         "ORDER_ITEMS",         "order_item_id"),
    IngestionTable(SOURCE_SCHEMA, "payments",            "PAYMENTS",            "payment_id"),
    IngestionTable(SOURCE_SCHEMA, "shipments",           "SHIPMENTS",           "shipment_id"),
    IngestionTable(SOURCE_SCHEMA, "returns",             "RETURNS",             "return_id"),
    IngestionTable(SOURCE_SCHEMA, "reviews",             "REVIEWS",             "review_id"),
    IngestionTable(SOURCE_SCHEMA, "web_sessions",        "WEB_SESSIONS",        "session_id"),
]
```

## 3.2 `src/ingestion/bronze.py`

```python
"""
Bronze layer ingestion: Postgres source → Snowflake BRONZE.

Strategy: truncate-and-load per run. Adds audit columns to every row.
Every run is logged to BRONZE._INGESTION_LOG.
"""
import uuid
from datetime import datetime

import pandas as pd
from loguru import logger
from sqlalchemy import text

from src.utils.db import (
    get_postgres_engine,
    get_snowflake_connection,
    get_snowflake_engine,
)
from src.ingestion.config import INGESTION_TABLES, IngestionTable

SOURCE_SYSTEM = "postgres_ecommerce"
BRONZE_SCHEMA = "BRONZE"
LOG_TABLE = "_INGESTION_LOG"


# ---------- Audit log ----------

def ensure_log_table(sf_conn) -> None:
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{LOG_TABLE} (
                INGESTION_ID     VARCHAR        NOT NULL,
                SOURCE_SYSTEM    VARCHAR        NOT NULL,
                TABLE_NAME       VARCHAR        NOT NULL,
                STARTED_AT       TIMESTAMP_NTZ  NOT NULL,
                ENDED_AT         TIMESTAMP_NTZ,
                ROWS_EXTRACTED   NUMBER,
                ROWS_LOADED      NUMBER,
                STATUS           VARCHAR        NOT NULL,
                ERROR_MESSAGE    VARCHAR
            );
        """)
    finally:
        cur.close()


def log_run(sf_conn, ingestion_id, table_name, started_at, ended_at,
            rows_extracted, rows_loaded, status, error=None):
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {BRONZE_SCHEMA}.{LOG_TABLE}
            (INGESTION_ID, SOURCE_SYSTEM, TABLE_NAME, STARTED_AT, ENDED_AT,
             ROWS_EXTRACTED, ROWS_LOADED, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (ingestion_id, SOURCE_SYSTEM, table_name, started_at, ended_at,
              rows_extracted, rows_loaded, status, error))
    finally:
        cur.close()


# ---------- Extract & Load ----------

def extract_from_postgres(table: IngestionTable) -> pd.DataFrame:
    pg = get_postgres_engine()
    try:
        query = f'SELECT * FROM "{table.source_schema}"."{table.source_table}";'
        df = pd.read_sql(query, pg)
        return df
    finally:
        pg.dispose()


def load_to_snowflake(df: pd.DataFrame, target_table: str,
                      ingestion_id: str, loaded_at: datetime) -> int:
    # Uppercase column names — Snowflake convention
    df.columns = [c.upper() for c in df.columns]

    # Audit columns
    df["_LOADED_AT"] = loaded_at
    df["_INGESTION_ID"] = ingestion_id
    df["_SOURCE_SYSTEM"] = SOURCE_SYSTEM

    engine = get_snowflake_engine(schema=BRONZE_SCHEMA)
    try:
        df.to_sql(
            name=target_table.lower(),  # SQLAlchemy will quote; Snowflake stores uppercase by default
            con=engine,
            schema=BRONZE_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=10_000,
            method="multi",
        )
    finally:
        engine.dispose()
    return len(df)


# ---------- Orchestration ----------

def ingest_table(table: IngestionTable, ingestion_id: str, sf_conn) -> bool:
    started = datetime.utcnow()
    rows_extracted = rows_loaded = 0
    status, error = "RUNNING", None
    success = False
    try:
        logger.info(f"[{table.target_table}] Extracting from Postgres ...")
        df = extract_from_postgres(table)
        rows_extracted = len(df)
        logger.info(f"[{table.target_table}] Extracted {rows_extracted:,} rows.")

        logger.info(f"[{table.target_table}] Loading to Snowflake BRONZE ...")
        rows_loaded = load_to_snowflake(df, table.target_table, ingestion_id, started)
        logger.success(f"[{table.target_table}] Loaded {rows_loaded:,} rows.")
        status, success = "SUCCESS", True
    except Exception as e:
        status, error = "FAILED", str(e)[:1000]
        logger.exception(f"[{table.target_table}] Failed: {e}")
    finally:
        log_run(sf_conn, ingestion_id, table.target_table,
                started, datetime.utcnow(),
                rows_extracted, rows_loaded, status, error)
    return success


def run_bronze_ingestion() -> str:
    ingestion_id = (
        f"bronze_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    logger.info(f"=== Bronze ingestion start: {ingestion_id} ===")

    sf_conn = get_snowflake_connection(schema=BRONZE_SCHEMA)
    successes, failures = 0, 0
    try:
        ensure_log_table(sf_conn)
        for table in INGESTION_TABLES:
            if ingest_table(table, ingestion_id, sf_conn):
                successes += 1
            else:
                failures += 1
    finally:
        sf_conn.close()

    logger.info(
        f"=== Bronze ingestion done: {ingestion_id} | "
        f"success={successes} failed={failures} ==="
    )
    return ingestion_id
```

## 3.3 `src/ingestion/run_bronze.py`

```python
"""CLI entry point: `python -m src.ingestion.run_bronze`"""
from src.ingestion.bronze import run_bronze_ingestion

if __name__ == "__main__":
    run_bronze_ingestion()
```

## 3.4 `src/ingestion/verify_bronze.py`

```python
"""
Reconciliation: Postgres source row counts vs Snowflake BRONZE row counts.
Run after `python -m src.ingestion.run_bronze` to confirm parity.
"""
from sqlalchemy import text

from src.utils.db import get_postgres_engine, get_snowflake_connection
from src.ingestion.config import INGESTION_TABLES

BRONZE_SCHEMA = "BRONZE"


def main():
    pg_engine = get_postgres_engine()
    sf_conn = get_snowflake_connection(schema=BRONZE_SCHEMA)
    sf_cur = sf_conn.cursor()

    print(f"{'Table':<22} {'Postgres':>10} {'Snowflake':>11} {'Diff':>8} {'Status':>8}")
    print("-" * 64)
    all_match = True
    try:
        with pg_engine.connect() as pg_conn:
            for t in INGESTION_TABLES:
                pg_count = pg_conn.execute(
                    text(f'SELECT COUNT(*) FROM "{t.source_schema}"."{t.source_table}";')
                ).scalar()

                sf_cur.execute(f'SELECT COUNT(*) FROM {BRONZE_SCHEMA}."{t.target_table}";')
                sf_count = sf_cur.fetchone()[0]

                diff = sf_count - pg_count
                ok = diff == 0
                all_match = all_match and ok
                print(f"{t.target_table:<22} {pg_count:>10,} {sf_count:>11,} "
                      f"{diff:>+8,} {'OK' if ok else 'MISMATCH':>8}")
    finally:
        sf_cur.close()
        sf_conn.close()
        pg_engine.dispose()

    print("-" * 64)
    print("All tables match." if all_match else "One or more mismatches — investigate above.")


if __name__ == "__main__":
    main()
```

## 3.5 Run it

```bash
cd ~/ecommerce-data-platform
python -m src.ingestion.run_bronze
```

You'll see streaming logs like:

```
2026-05-19 ... | INFO  | === Bronze ingestion start: bronze_20260519_... ===
2026-05-19 ... | INFO  | [CATEGORIES] Extracting from Postgres ...
2026-05-19 ... | INFO  | [CATEGORIES] Extracted 15 rows.
2026-05-19 ... | INFO  | [CATEGORIES] Loading to Snowflake BRONZE ...
2026-05-19 ... | SUCCESS | [CATEGORIES] Loaded 15 rows.
...
2026-05-19 ... | INFO  | === Bronze ingestion done: bronze_... | success=14 failed=0 ===
```

Total runtime: 30–90 seconds depending on your network to Snowflake Mumbai.

## 3.6 Verify

```bash
python -m src.ingestion.verify_bronze
```

Expected:

```
Table                  Postgres   Snowflake     Diff   Status
----------------------------------------------------------------
CATEGORIES                   15          15       +0       OK
SUPPLIERS                    20          20       +0       OK
PRODUCTS                    300         300       +0       OK
INVENTORY                   300         300       +0       OK
CUSTOMERS                 2,000       2,000       +0       OK
MARKETING_CAMPAIGNS          30          30       +0       OK
COUPONS                      50          50       +0       OK
ORDERS                    5,000       5,000       +0       OK
ORDER_ITEMS              15,000      15,000       +0       OK
PAYMENTS                  5,000       5,000       +0       OK
SHIPMENTS                 3,277       3,277       +0       OK
RETURNS                   1,000       1,000       +0       OK
REVIEWS                   1,979       1,979       +0       OK
WEB_SESSIONS              8,000       8,000       +0       OK
----------------------------------------------------------------
All tables match.
```

## 3.7 Sanity-check the audit log in Snowsight

Open a worksheet in Snowsight and run:

```sql
USE DATABASE ECOMMERCE_DW;
USE SCHEMA BRONZE;

-- Latest run summary
SELECT INGESTION_ID, COUNT(*) AS TABLES_PROCESSED,
       SUM(ROWS_LOADED) AS TOTAL_ROWS,
       MIN(STARTED_AT) AS RUN_START,
       MAX(ENDED_AT)   AS RUN_END,
       SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSES,
       SUM(CASE WHEN STATUS = 'FAILED'  THEN 1 ELSE 0 END) AS FAILURES
FROM _INGESTION_LOG
GROUP BY INGESTION_ID
ORDER BY RUN_START DESC
LIMIT 5;

-- Inspect a sample bronze table with audit columns
SELECT * FROM ORDERS LIMIT 5;

-- See what tables now live in BRONZE
SHOW TABLES IN SCHEMA BRONZE;
```

The `ORDERS` sample should show the source columns plus `_LOADED_AT`, `_INGESTION_ID`, `_SOURCE_SYSTEM` at the right end.

## 3.8 Common errors and fixes

- **`snowflake.connector.errors.ProgrammingError: 002003 (42S02): Object ... does not exist`** — usually a permissions issue. If you're using `ECOM_ENGINEER`, verify the future-grants from Step 2.2 ran (re-run them — they're idempotent). If using `ACCOUNTADMIN`, this shouldn't occur.
- **`pyarrow` not found** — already in `requirements.txt` but if you skipped it: `pip install pyarrow==16.0.0`. Snowflake's pandas integration needs it.
- **`No module named 'src.ingestion'`** — you forgot the empty `__init__.py` in `src/ingestion/`. Create it: `touch src/ingestion/__init__.py`.
- **Decimal columns arriving as FLOAT in Snowflake** — expected. pandas converts NUMERIC to `float64`. We'll cast to proper `NUMBER(p,s)` in Silver. Bronze tolerates lossy types by design.
- **`returns` table name conflict** — Snowflake treats `RETURNS` as a non-reserved word so we're fine, but if you ever hit a parser issue, quote it: `"RETURNS"`.

## Confirmation checklist before Step 4

1. `python -m src.ingestion.run_bronze` completes with `success=14 failed=0`.
2. `python -m src.ingestion.verify_bronze` shows `All tables match.`
3. In Snowsight, `SHOW TABLES IN SCHEMA BRONZE;` lists 15 tables (14 source + `_INGESTION_LOG`).
4. A `SELECT * FROM BRONZE.ORDERS LIMIT 5;` returns rows with `_LOADED_AT`, `_INGESTION_ID`, `_SOURCE_SYSTEM` columns populated.
5. Querying `BRONZE._INGESTION_LOG` shows one row per source table for your run, all with `STATUS = 'SUCCESS'`.

Paste the output of `verify_bronze.py` and confirm the checklist. Step 4 will be a brief, optional one: incremental ingestion (CDC-lite using high-water-mark on `order_date` / `signup_date` etc.) and a re-run safety test. If you'd rather skip the incremental step and jump straight to Silver-layer transformations (Step 5 in my original plan), say so and we'll merge those.

---



## Step 4: Silver Layer — Cleansing, Type-Conformance, Deduplication

## What Silver is for

Bronze holds the raw source as-is — useful for lineage but messy: mixed-case strings, inconsistent capitalization, possible duplicates, source-system types (FLOAT where it should be NUMBER, VARCHAR where it should be DATE), no business validation. Silver is where we fix all of that:

- **Type conformance** — cast numerics to proper `NUMBER(p,s)`, strings to `VARCHAR`, dates to `DATE`/`TIMESTAMP_NTZ`.
- **Standardization** — trim whitespace, `LOWER()` emails, `INITCAP()` proper nouns, `UPPER()` codes.
- **Deduplication** — `QUALIFY ROW_NUMBER() OVER (...) = 1` keeps the latest version per primary key (defensive, even though our source has PKs).
- **Business validation** — drop or null out impossible values (negative quantities, ages outside 0–120, ratings outside 1–5).
- **Add Silver audit columns** — `_PROCESSED_AT`, `_SILVER_RUN_ID` while preserving Bronze's `_LOADED_AT`, `_INGESTION_ID`, `_SOURCE_SYSTEM` for lineage.

Silver tables mirror Bronze 1:1 in structure (same columns, plus audit) and are queryable on their own — but the real consumer is Gold (Step 6+) where the star schema lives.

## Architecture

- **One SQL file per table** under `sql/ddl/silver/`. Each is a `CREATE OR REPLACE TABLE ... AS SELECT ...` — idempotent.
- **Python orchestrator** discovers `*.sql` files, executes them, logs each run to `SILVER._SILVER_RUN_LOG`.
- **`{{ SILVER_RUN_ID }}` placeholder** in each SQL — replaced at runtime so every row knows which Silver run produced it.

## 4.1 Create all 14 SQL files

Run this one block — it writes every `*.sql` file via heredocs. Paste the whole thing into your terminal:

```bash
cd ~/ecommerce-data-platform
mkdir -p sql/ddl/silver

# ------------- categories -------------
cat > sql/ddl/silver/categories.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.CATEGORIES AS
SELECT
    CATEGORY_ID::NUMBER                          AS CATEGORY_ID,
    INITCAP(TRIM(CATEGORY_NAME))                 AS CATEGORY_NAME,
    PARENT_CATEGORY_ID::NUMBER                   AS PARENT_CATEGORY_ID,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.CATEGORIES
QUALIFY ROW_NUMBER() OVER (PARTITION BY CATEGORY_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- suppliers -------------
cat > sql/ddl/silver/suppliers.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.SUPPLIERS AS
SELECT
    SUPPLIER_ID::NUMBER                          AS SUPPLIER_ID,
    INITCAP(TRIM(SUPPLIER_NAME))                 AS SUPPLIER_NAME,
    INITCAP(TRIM(COUNTRY))                       AS COUNTRY,
    GREATEST(LEAD_TIME_DAYS, 0)::NUMBER          AS LEAD_TIME_DAYS,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.SUPPLIERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY SUPPLIER_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- products -------------
cat > sql/ddl/silver/products.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.PRODUCTS AS
SELECT
    PRODUCT_ID::NUMBER                           AS PRODUCT_ID,
    TRIM(PRODUCT_NAME)                           AS PRODUCT_NAME,
    CATEGORY_ID::NUMBER                          AS CATEGORY_ID,
    INITCAP(TRIM(BRAND))                         AS BRAND,
    UNIT_PRICE::NUMBER(10,2)                     AS UNIT_PRICE,
    COST::NUMBER(10,2)                           AS COST,
    (UNIT_PRICE - COST)::NUMBER(10,2)            AS UNIT_MARGIN,
    SUPPLIER_ID::NUMBER                          AS SUPPLIER_ID,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.PRODUCTS
WHERE UNIT_PRICE > 0 AND COST >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- inventory -------------
cat > sql/ddl/silver/inventory.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.INVENTORY AS
SELECT
    INVENTORY_ID::NUMBER                         AS INVENTORY_ID,
    PRODUCT_ID::NUMBER                           AS PRODUCT_ID,
    UPPER(TRIM(WAREHOUSE))                       AS WAREHOUSE,
    GREATEST(STOCK_QTY, 0)::NUMBER               AS STOCK_QTY,
    RESTOCK_DATE::DATE                           AS RESTOCK_DATE,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.INVENTORY
QUALIFY ROW_NUMBER() OVER (PARTITION BY INVENTORY_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- customers -------------
cat > sql/ddl/silver/customers.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.CUSTOMERS AS
SELECT
    CUSTOMER_ID::NUMBER                                          AS CUSTOMER_ID,
    INITCAP(TRIM(FIRST_NAME))                                    AS FIRST_NAME,
    INITCAP(TRIM(LAST_NAME))                                     AS LAST_NAME,
    LOWER(TRIM(EMAIL))                                           AS EMAIL,
    SIGNUP_DATE::DATE                                            AS SIGNUP_DATE,
    INITCAP(TRIM(COUNTRY))                                       AS COUNTRY,
    INITCAP(TRIM(STATE))                                         AS STATE,
    INITCAP(TRIM(CITY))                                          AS CITY,
    CASE WHEN AGE BETWEEN 13 AND 120 THEN AGE END                AS AGE,
    UPPER(TRIM(GENDER))                                          AS GENDER,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                          AS _SILVER_RUN_ID
FROM BRONZE.CUSTOMERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- marketing_campaigns -------------
cat > sql/ddl/silver/marketing_campaigns.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.MARKETING_CAMPAIGNS AS
SELECT
    CAMPAIGN_ID::NUMBER                          AS CAMPAIGN_ID,
    TRIM(CAMPAIGN_NAME)                          AS CAMPAIGN_NAME,
    INITCAP(TRIM(CHANNEL))                       AS CHANNEL,
    START_DATE::DATE                             AS START_DATE,
    END_DATE::DATE                               AS END_DATE,
    BUDGET::NUMBER(12,2)                         AS BUDGET,
    DATEDIFF(DAY, START_DATE, END_DATE)          AS DURATION_DAYS,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.MARKETING_CAMPAIGNS
WHERE END_DATE >= START_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAMPAIGN_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- coupons -------------
cat > sql/ddl/silver/coupons.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.COUPONS AS
SELECT
    COUPON_ID::NUMBER                            AS COUPON_ID,
    UPPER(TRIM(CODE))                            AS CODE,
    UPPER(TRIM(DISCOUNT_TYPE))                   AS DISCOUNT_TYPE,
    DISCOUNT_VALUE::NUMBER(10,2)                 AS DISCOUNT_VALUE,
    START_DATE::DATE                             AS START_DATE,
    END_DATE::DATE                               AS END_DATE,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.COUPONS
WHERE DISCOUNT_VALUE > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY COUPON_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- orders -------------
cat > sql/ddl/silver/orders.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.ORDERS AS
SELECT
    ORDER_ID::NUMBER                             AS ORDER_ID,
    CUSTOMER_ID::NUMBER                          AS CUSTOMER_ID,
    ORDER_DATE::TIMESTAMP_NTZ                    AS ORDER_DATE,
    UPPER(TRIM(STATUS))                          AS STATUS,
    TOTAL_AMOUNT::NUMBER(12,2)                   AS TOTAL_AMOUNT,
    UPPER(TRIM(PAYMENT_METHOD))                  AS PAYMENT_METHOD,
    COUPON_ID::NUMBER                            AS COUPON_ID,
    CAMPAIGN_ID::NUMBER                          AS CAMPAIGN_ID,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.ORDERS
WHERE TOTAL_AMOUNT >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- order_items -------------
cat > sql/ddl/silver/order_items.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.ORDER_ITEMS AS
SELECT
    ORDER_ITEM_ID::NUMBER                                       AS ORDER_ITEM_ID,
    ORDER_ID::NUMBER                                            AS ORDER_ID,
    PRODUCT_ID::NUMBER                                          AS PRODUCT_ID,
    GREATEST(QUANTITY, 0)::NUMBER                               AS QUANTITY,
    UNIT_PRICE::NUMBER(10,2)                                    AS UNIT_PRICE,
    COALESCE(DISCOUNT, 0)::NUMBER(10,2)                         AS DISCOUNT,
    COALESCE(TAX, 0)::NUMBER(10,2)                              AS TAX,
    ((QUANTITY * UNIT_PRICE) - COALESCE(DISCOUNT, 0)
       + COALESCE(TAX, 0))::NUMBER(12,2)                        AS LINE_TOTAL,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                         AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                         AS _SILVER_RUN_ID
FROM BRONZE.ORDER_ITEMS
WHERE QUANTITY > 0 AND UNIT_PRICE >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- payments -------------
cat > sql/ddl/silver/payments.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.PAYMENTS AS
SELECT
    PAYMENT_ID::NUMBER                           AS PAYMENT_ID,
    ORDER_ID::NUMBER                             AS ORDER_ID,
    PAYMENT_DATE::TIMESTAMP_NTZ                  AS PAYMENT_DATE,
    AMOUNT::NUMBER(12,2)                         AS AMOUNT,
    UPPER(TRIM(PAYMENT_STATUS))                  AS PAYMENT_STATUS,
    UPPER(TRIM(METHOD))                          AS METHOD,
    TRIM(TRANSACTION_ID)                         AS TRANSACTION_ID,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.PAYMENTS
WHERE AMOUNT >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY PAYMENT_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- shipments -------------
cat > sql/ddl/silver/shipments.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.SHIPMENTS AS
SELECT
    SHIPMENT_ID::NUMBER                                          AS SHIPMENT_ID,
    ORDER_ID::NUMBER                                             AS ORDER_ID,
    SHIPPED_DATE::TIMESTAMP_NTZ                                  AS SHIPPED_DATE,
    DELIVERY_DATE::TIMESTAMP_NTZ                                 AS DELIVERY_DATE,
    UPPER(TRIM(SHIPPING_STATUS))                                 AS SHIPPING_STATUS,
    GREATEST(SHIPPING_COST, 0)::NUMBER(10,2)                     AS SHIPPING_COST,
    INITCAP(TRIM(CARRIER))                                       AS CARRIER,
    CASE WHEN DELIVERY_DATE IS NOT NULL AND SHIPPED_DATE IS NOT NULL
         THEN DATEDIFF(DAY, SHIPPED_DATE, DELIVERY_DATE) END     AS DAYS_TO_DELIVER,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                          AS _SILVER_RUN_ID
FROM BRONZE.SHIPMENTS
WHERE DELIVERY_DATE IS NULL OR DELIVERY_DATE >= SHIPPED_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- returns -------------
cat > sql/ddl/silver/returns.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.RETURNS AS
SELECT
    RETURN_ID::NUMBER                            AS RETURN_ID,
    ORDER_ITEM_ID::NUMBER                        AS ORDER_ITEM_ID,
    RETURN_DATE::TIMESTAMP_NTZ                   AS RETURN_DATE,
    INITCAP(TRIM(REASON))                        AS REASON,
    GREATEST(REFUND_AMOUNT, 0)::NUMBER(10,2)     AS REFUND_AMOUNT,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.RETURNS
QUALIFY ROW_NUMBER() OVER (PARTITION BY RETURN_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- reviews -------------
cat > sql/ddl/silver/reviews.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.REVIEWS AS
SELECT
    REVIEW_ID::NUMBER                            AS REVIEW_ID,
    ORDER_ID::NUMBER                             AS ORDER_ID,
    PRODUCT_ID::NUMBER                           AS PRODUCT_ID,
    CUSTOMER_ID::NUMBER                          AS CUSTOMER_ID,
    CASE WHEN RATING BETWEEN 1 AND 5 THEN RATING END AS RATING,
    TRIM(REVIEW_TEXT)                            AS REVIEW_TEXT,
    REVIEW_DATE::TIMESTAMP_NTZ                   AS REVIEW_DATE,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                          AS _SILVER_RUN_ID
FROM BRONZE.REVIEWS
WHERE RATING IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY REVIEW_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

# ------------- web_sessions -------------
cat > sql/ddl/silver/web_sessions.sql << 'EOF'
CREATE OR REPLACE TABLE SILVER.WEB_SESSIONS AS
SELECT
    SESSION_ID::NUMBER                                           AS SESSION_ID,
    CUSTOMER_ID::NUMBER                                          AS CUSTOMER_ID,
    SESSION_START::TIMESTAMP_NTZ                                 AS SESSION_START,
    SESSION_END::TIMESTAMP_NTZ                                   AS SESSION_END,
    DATEDIFF(SECOND, SESSION_START, SESSION_END)                 AS SESSION_DURATION_SEC,
    LOWER(TRIM(SOURCE))                                          AS SOURCE,
    LOWER(TRIM(MEDIUM))                                          AS MEDIUM,
    CAMPAIGN_ID::NUMBER                                          AS CAMPAIGN_ID,
    INITCAP(TRIM(DEVICE))                                        AS DEVICE,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                          AS _SILVER_RUN_ID
FROM BRONZE.WEB_SESSIONS
WHERE SESSION_END IS NULL OR SESSION_END >= SESSION_START
QUALIFY ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY _LOADED_AT DESC) = 1;
EOF

ls -la sql/ddl/silver/
```

The final `ls` should show 14 `.sql` files.

## 4.2 Python orchestrator

```bash
mkdir -p src/transformations
touch src/transformations/__init__.py

cat > src/transformations/silver.py << 'EOF'
"""
Silver layer orchestrator: discovers sql/ddl/silver/*.sql, executes
each against Snowflake, and logs every run to SILVER._SILVER_RUN_LOG.
"""
import uuid
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.utils.db import get_snowflake_connection

SILVER_SCHEMA = "SILVER"
LOG_TABLE = "_SILVER_RUN_LOG"
SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "silver"


def ensure_log_table(sf_conn) -> None:
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{LOG_TABLE} (
                SILVER_RUN_ID    VARCHAR        NOT NULL,
                TABLE_NAME       VARCHAR        NOT NULL,
                STARTED_AT       TIMESTAMP_NTZ  NOT NULL,
                ENDED_AT         TIMESTAMP_NTZ,
                ROW_COUNT        NUMBER,
                STATUS           VARCHAR        NOT NULL,
                ERROR_MESSAGE    VARCHAR
            );
        """)
    finally:
        cur.close()


def log_run(sf_conn, run_id, table_name, started, ended, row_count, status, error=None):
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {SILVER_SCHEMA}.{LOG_TABLE}
            (SILVER_RUN_ID, TABLE_NAME, STARTED_AT, ENDED_AT, ROW_COUNT, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (run_id, table_name, started, ended, row_count, status, error))
    finally:
        cur.close()


def execute_silver_file(sf_conn, sql_file: Path, run_id: str) -> tuple[bool, int]:
    table_name = sql_file.stem.upper()
    started = datetime.utcnow()
    row_count = 0
    status, error = "RUNNING", None
    success = False
    try:
        sql_template = sql_file.read_text()
        # Substitute the run_id placeholder; quote as SQL string literal
        sql = sql_template.replace("{{ SILVER_RUN_ID }}", f"'{run_id}'")

        cur = sf_conn.cursor()
        try:
            logger.info(f"[{table_name}] Executing {sql_file.name} ...")
            for statement in [s.strip() for s in sql.split(";") if s.strip()]:
                cur.execute(statement)

            cur.execute(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.{table_name};")
            row_count = cur.fetchone()[0]
            logger.success(f"[{table_name}] {row_count:,} rows in SILVER.{table_name}")
            status, success = "SUCCESS", True
        finally:
            cur.close()
    except Exception as e:
        status, error = "FAILED", str(e)[:1000]
        logger.exception(f"[{table_name}] Failed: {e}")
    finally:
        log_run(sf_conn, run_id, table_name, started, datetime.utcnow(),
                row_count, status, error)
    return success, row_count


def run_silver_transformations() -> str:
    run_id = (
        f"silver_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    logger.info(f"=== Silver run start: {run_id} ===")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(f"No SQL files in {SQL_DIR}. Aborting.")
        return run_id

    sf_conn = get_snowflake_connection(schema=SILVER_SCHEMA)
    successes = failures = 0
    try:
        ensure_log_table(sf_conn)
        for sql_file in sql_files:
            ok, _ = execute_silver_file(sf_conn, sql_file, run_id)
            successes += int(ok)
            failures += int(not ok)
    finally:
        sf_conn.close()

    logger.info(
        f"=== Silver run done: {run_id} | "
        f"success={successes} failed={failures} ==="
    )
    return run_id
EOF

cat > src/transformations/run_silver.py << 'EOF'
"""CLI entry: python -m src.transformations.run_silver"""
from src.transformations.silver import run_silver_transformations

if __name__ == "__main__":
    run_silver_transformations()
EOF
```

## 4.3 Verification script

```bash
cat > src/transformations/verify_silver.py << 'EOF'
"""
Compare row counts between Snowflake BRONZE and SILVER, and flag any
tables where Silver dropped rows (which is expected for rows that
failed business-validation filters — but worth surfacing).
"""
from src.utils.db import get_snowflake_connection
from src.ingestion.config import INGESTION_TABLES

BRONZE = "BRONZE"
SILVER = "SILVER"


def main():
    sf = get_snowflake_connection()
    cur = sf.cursor()

    print(f"{'Table':<22} {'Bronze':>10} {'Silver':>10} {'Δ Rows':>10} {'% Kept':>8}")
    print("-" * 64)
    try:
        for t in INGESTION_TABLES:
            cur.execute(f'SELECT COUNT(*) FROM {BRONZE}."{t.target_table}";')
            b_count = cur.fetchone()[0]
            cur.execute(f'SELECT COUNT(*) FROM {SILVER}."{t.target_table}";')
            s_count = cur.fetchone()[0]

            diff = s_count - b_count
            pct = (s_count / b_count * 100) if b_count else 0.0
            print(f"{t.target_table:<22} {b_count:>10,} {s_count:>10,} "
                  f"{diff:>+10,} {pct:>7.1f}%")
    finally:
        cur.close()
        sf.close()


if __name__ == "__main__":
    main()
EOF
```

## 4.4 Run it

```bash
python -m src.transformations.run_silver
```

You'll see streaming logs — one per SQL file:

```
INFO  | === Silver run start: silver_20260520_... ===
INFO  | [CATEGORIES] Executing categories.sql ...
SUCCESS | [CATEGORIES] 15 rows in SILVER.CATEGORIES
INFO  | [SUPPLIERS] Executing suppliers.sql ...
SUCCESS | [SUPPLIERS] 20 rows in SILVER.SUPPLIERS
...
INFO  | === Silver run done: silver_... | success=14 failed=0 ===
```

Total runtime: 30–60 seconds.

## 4.5 Verify

```bash
python -m src.transformations.verify_silver
```

Expected shape (your exact diffs may vary by 1–2 rows):

```
Table                      Bronze     Silver     Δ Rows   % Kept
----------------------------------------------------------------
CATEGORIES                     15         15         +0   100.0%
SUPPLIERS                      20         20         +0   100.0%
PRODUCTS                      300        300         +0   100.0%
INVENTORY                     300        300         +0   100.0%
CUSTOMERS                   2,000      2,000         +0   100.0%
MARKETING_CAMPAIGNS            30         30         +0   100.0%
COUPONS                        50         50         +0   100.0%
ORDERS                      5,000      5,000         +0   100.0%
ORDER_ITEMS                15,000     15,000         +0   100.0%
PAYMENTS                    5,000      5,000         +0   100.0%
SHIPMENTS                   3,277      3,277         +0   100.0%
RETURNS                     1,000      1,000         +0   100.0%
REVIEWS                     1,979      1,979         +0   100.0%
WEB_SESSIONS                8,000      8,000         +0   100.0%
```

If any row count drops, that's the validation filter catching invalid data — e.g., a review with rating 0 or 7 would be filtered out by `WHERE RATING IS NOT NULL` (after the `CASE` nulls it). For a portfolio talking point, that's actually a *good* thing — it demonstrates your pipeline detects and rejects bad data instead of silently passing it through.

## 4.6 Snowsight spot-checks

Open a worksheet and run:

```sql
USE DATABASE ECOMMERCE_DW;

-- 1. All Silver tables present
SHOW TABLES IN SCHEMA SILVER;

-- 2. Silver run log — should have 14 SUCCESS rows for your latest run
SELECT SILVER_RUN_ID, COUNT(*) AS TABLES, SUM(ROW_COUNT) AS TOTAL_ROWS,
       MIN(STARTED_AT) AS RUN_START, MAX(ENDED_AT) AS RUN_END,
       SUM(CASE WHEN STATUS='SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSES,
       SUM(CASE WHEN STATUS='FAILED'  THEN 1 ELSE 0 END) AS FAILURES
FROM SILVER._SILVER_RUN_LOG
GROUP BY SILVER_RUN_ID
ORDER BY RUN_START DESC
LIMIT 5;

-- 3. Inspect a few rows: standardization visible
SELECT FIRST_NAME, LAST_NAME, EMAIL, COUNTRY, AGE, GENDER
FROM SILVER.CUSTOMERS
LIMIT 5;
-- (FIRST_NAME / LAST_NAME / COUNTRY in Title Case, EMAIL lowercase, GENDER uppercase)

-- 4. New derived columns appear in Silver
SELECT ORDER_ITEM_ID, QUANTITY, UNIT_PRICE, DISCOUNT, TAX, LINE_TOTAL
FROM SILVER.ORDER_ITEMS LIMIT 5;

SELECT PRODUCT_ID, PRODUCT_NAME, UNIT_PRICE, COST, UNIT_MARGIN
FROM SILVER.PRODUCTS LIMIT 5;

-- 5. Audit trail: every silver row carries bronze + silver lineage
SELECT _INGESTION_ID, _LOADED_AT, _SILVER_RUN_ID, _PROCESSED_AT, COUNT(*)
FROM SILVER.ORDERS
GROUP BY ALL
LIMIT 5;
```

`UNIT_MARGIN` and `LINE_TOTAL` and `DURATION_DAYS` and `DAYS_TO_DELIVER` are pre-computed in Silver — saves Gold and Power BI from recalculating them on every query.

## Common failure modes

- **`Object 'BRONZE.X' does not exist or not authorized`** — Silver runs as `ECOM_ENGINEER`. If Bronze tables were created by a different role earlier, re-run the grants block from Step 3 troubleshooting (the `GRANT OWNERSHIP` and `GRANT ALL ON ALL TABLES` for BRONZE).
- **`QUALIFY clause not supported`** — only happens on very old Snowflake editions; you're on a current one so this won't fire.
- **`Numeric value 'X' is not recognized`** — a non-numeric string sneaked into a numeric source column. Inspect with `SELECT * FROM BRONZE.<table> WHERE TRY_CAST(<col> AS NUMBER) IS NULL;` to find the offender; for a one-off fix, wrap with `TRY_CAST(... AS NUMBER(p,s))` in the SQL file.

## Confirmation checklist before Step 5

1. `ls sql/ddl/silver/` shows 14 `.sql` files.
2. `python -m src.transformations.run_silver` finishes with `success=14 failed=0`.
3. `python -m src.transformations.verify_silver` shows all 14 tables, no unexpectedly-low `% Kept`.
4. In Snowsight, `SHOW TABLES IN SCHEMA SILVER;` returns 15 entries (14 tables + `_SILVER_RUN_LOG`).
5. Spot-check queries in 4.6 show standardized strings (case-normalized) and derived columns (`UNIT_MARGIN`, `LINE_TOTAL`, etc.) populated.

Paste the `verify_silver` output and confirm the checklist. Step 5 will be the most impactful one for the portfolio: **Gold layer dimensional design**. We'll lay out the star schema on paper (with a diagram so you can show it to recruiters), make explicit decisions about each dimension's SCD type, define grain for each fact table, and identify conformed dimensions. No code in Step 5 — just design. Step 6 then implements it.

---



## Step 5: Gold Layer — Dimensional Model Design

We'll follow Kimball's four-step design process, decide SCD types per dimension, and lay out the bus matrix. Step 6 implements it.

## Step 5.1 — The four design decisions (Kimball method)

**1. Business processes → fact tables.** Each thing the business *does* and measures becomes a fact. Your source has six measurable processes: placing orders, taking payments, shipping, handling returns, collecting reviews, and tracking web sessions. Six facts.

**2. Declare the grain.** The grain is "what does one row mean?" — decided *before* picking dimensions or measures. Getting this wrong is the most common modeling mistake.

| Fact | Grain (one row =) | Fact type |
|---|---|---|
| `FACT_SALES` | one product line within one order | transaction |
| `FACT_PAYMENTS` | one payment against one order | transaction |
| `FACT_SHIPMENTS` | one shipment of one order | accumulating-ish |
| `FACT_RETURNS` | one returned line item | transaction |
| `FACT_REVIEWS` | one product review | transaction |
| `FACT_WEB_SESSIONS` | one website session | transaction |

`FACT_SALES` is the star of the show — it sits at the `order_items` grain (15,000 rows), the finest level, which lets every revenue metric roll up cleanly.

**3. Identify dimensions.** The "by what?" questions — analyze sales *by date*, *by customer*, *by product*, *by campaign*, *by coupon*. Five dimensions.

**4. Identify facts (measures).** The numeric, additive things you sum: quantity, revenue, discount, tax, margin, refund amount, shipping cost, rating, session duration.

## Step 5.2 — Dimension design and SCD type decisions

This is the part interviewers dig into, so each choice has a stated reason.

| Dimension | Source | SCD Type | Why |
|---|---|---|---|
| `DIM_DATE` | generated | Type 0 (static) | Calendar never changes; pre-built once, conformed across all facts. |
| `DIM_CUSTOMER` | `customers` | **Type 2** | Customers relocate (city/state/country change). Type 2 preserves "where did they live *when they ordered*," essential for accurate regional analysis over time. |
| `DIM_PRODUCT` | `products` + `categories` + `suppliers` | **Type 2** | `unit_price` and `cost` change over time. Type 2 keeps the price *at time of sale* so historical margin is correct even after a repricing. Category and supplier are denormalized in (star, not snowflake). |
| `DIM_CAMPAIGN` | `marketing_campaigns` | Type 1 (overwrite) | Campaign attributes are fixed at launch; corrections just overwrite. History not analytically valuable. |
| `DIM_COUPON` | `coupons` | Type 1 (overwrite) | Same reasoning — coupon definitions are static once issued. |

The two Type-2 dimensions (`DIM_CUSTOMER`, `DIM_PRODUCT`) are where you demonstrate the headline skill from your project plan. Here's the structure they'll get:

Each Type-2 dimension carries a **surrogate key** (`CUSTOMER_KEY`, an auto-generated integer — the dimension's true primary key) separate from the **business/natural key** (`CUSTOMER_ID`, from source). The surrogate key is what facts join on. It also carries effective-dating columns: `VALID_FROM`, `VALID_TO`, and an `IS_CURRENT` boolean, plus a `ROW_HASH` over the tracked attributes for fast change detection.

When the pipeline sees a customer whose tracked attributes changed, it **expires** the old row (sets `VALID_TO = now`, `IS_CURRENT = FALSE`) and **inserts** a new row (new surrogate key, `VALID_FROM = now`, `IS_CURRENT = TRUE`). Facts then join to the surrogate key whose `[VALID_FROM, VALID_TO]` window contains the transaction date — a point-in-time join that locks in historical truth.

Let me show you the resulting star.

Here's the primary star centered on `FACT_SALES`:
A star schema is fundamentally an ERD, so here it is rendered properly with crow's-foot notation. The five dimensions all point into the central `FACT_SALES` — note the surrogate keys (`_KEY`, the PK of each dimension) versus business keys (`_ID`), and the SCD-2 effective-dating columns on `DIM_CUSTOMER` and `DIM_PRODUCT`:
That's the primary star. The other five facts reuse these same dimensions — that reuse is what Kimball calls **conformed dimensions**, and it's the single most important property of the model: because `DIM_CUSTOMER` and `DIM_DATE` mean the same thing everywhere, you can compare returns against sales against reviews on the same customer and the same calendar without reconciliation. The full picture is a galaxy schema (multiple fact tables sharing conformed dimensions), summarized in the bus matrix below.

## Step 5.3 — The bus matrix

This is the Kimball artifact that proves your dimensions are conformed. Rows are facts, columns are dimensions, a mark means "this fact joins to this dimension." Put this in your README — it tells a data architect everything about the model at a glance.

| Fact (grain) | DIM_DATE | DIM_CUSTOMER | DIM_PRODUCT | DIM_CAMPAIGN | DIM_COUPON |
|---|:---:|:---:|:---:|:---:|:---:|
| FACT_SALES (order line) | ✓ | ✓ | ✓ | ✓ | ✓ |
| FACT_PAYMENTS (payment) | ✓ | ✓ | — | — | — |
| FACT_SHIPMENTS (shipment) | ✓ | ✓ | — | — | — |
| FACT_RETURNS (returned line) | ✓ | ✓ | ✓ | — | — |
| FACT_REVIEWS (review) | ✓ | ✓ | ✓ | — | — |
| FACT_WEB_SESSIONS (session) | ✓ | ✓* | — | ✓ | — |

\* `DIM_CUSTOMER` is nullable for web sessions — anonymous/guest traffic has no customer. Those rows point at the dimension's "unknown member."

## Step 5.4 — Two patterns to remember for Step 6

**Degenerate dimensions.** `ORDER_ID` lives *in* `FACT_SALES` as a plain column, not in its own dimension table — it's a transaction identifier with no descriptive attributes worth a separate table, but it's still useful for grouping line items back into orders. Same for `TRANSACTION_ID` in `FACT_PAYMENTS`. This is correct Kimball practice, not laziness — a "DIM_ORDER" with one meaningful column would be wasteful.

**Unknown member.** Every dimension gets a special row with surrogate key `-1` representing "unknown / not applicable" (e.g., orders with no coupon, sessions with no customer). Facts with a missing or orphaned business key point at `-1` instead of `NULL`. This keeps every fact-to-dimension join an inner join — no rows silently dropped, no `NULL` handling scattered through your DAX later. It's a small thing that separates a polished model from a fragile one.

## Confirmation before Step 6

Nothing to run this step — it's design. Just confirm you're comfortable with these decisions, because Step 6 builds them as code:

1. Six facts at the grains in the table above; `FACT_SALES` is the centerpiece at order-line grain.
2. Five dimensions; `DIM_CUSTOMER` and `DIM_PRODUCT` are SCD Type 2 (surrogate keys + `VALID_FROM`/`VALID_TO`/`IS_CURRENT`), the rest Type 1 or static.
3. `DIM_DATE` is generated, not sourced.
4. Degenerate dimensions (`ORDER_ID`, `TRANSACTION_ID`) stay in the facts.
5. Every dimension carries an unknown-member row at key `-1`.

If any decision doesn't sit right — say you'd rather snowflake out `DIM_SUPPLIER` and `DIM_CATEGORY` as separate tables to show that technique, or make `DIM_CAMPAIGN` Type 2 as well — tell me now and I'll adjust before we write code.

Step 6 will implement the dimensions: build `DIM_DATE` (a generated calendar spanning your order date range), then the SCD-2 load logic for `DIM_CUSTOMER` and `DIM_PRODUCT` (with a re-run test that proves history is captured when an attribute changes), then the Type-1 dimensions. Step 7 builds the six fact tables with their surrogate-key lookups against those dimensions. Reply "next" when you're ready.

---



## Step 6: Gold Layer — Dimensions

## What we build and how each is loaded

| Dimension | Load strategy | Surrogate keys |
|---|---|---|
| `DIM_DATE` | `CREATE OR REPLACE` (deterministic `YYYYMMDD` keys) | none needed |
| `DIM_CAMPAIGN`, `DIM_COUPON` (Type 1) | `MERGE` upsert — overwrite attributes, keep keys stable | sequence |
| `DIM_CUSTOMER`, `DIM_PRODUCT` (Type 2) | expire-then-insert — preserve history | sequence |

The key idea for Type 2: a staging view computes an `MD5` hash over the tracked attributes. On each run we (1) expire current rows whose hash changed, then (2) insert a fresh current row for anything that has no current row — which now covers both brand-new customers *and* the ones we just expired. Two simple statements, no clever single-MERGE gymnastics.

## 6.1 Create the dimension SQL files

Paste this whole block — it writes `00`–`05` under `sql/ddl/gold/`:

```bash
cd ~/ecommerce-data-platform
mkdir -p sql/ddl/gold

# ===== 00 sequences =====
cat > sql/ddl/gold/00_sequences.sql << 'EOF'
CREATE SEQUENCE IF NOT EXISTS GOLD.SEQ_DIM_CUSTOMER START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS GOLD.SEQ_DIM_PRODUCT  START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS GOLD.SEQ_DIM_CAMPAIGN START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS GOLD.SEQ_DIM_COUPON   START = 1 INCREMENT = 1;
EOF

# ===== 01 dim_date =====
cat > sql/ddl/gold/01_dim_date.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.DIM_DATE AS
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2022-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 2557))
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))   AS DATE_KEY,
    d                                   AS FULL_DATE,
    YEAR(d)                             AS YEAR,
    QUARTER(d)                          AS QUARTER,
    MONTH(d)                            AS MONTH_NUM,
    MONTHNAME(d)                        AS MONTH_NAME,
    DAY(d)                              AS DAY_OF_MONTH,
    DAYOFWEEKISO(d)                     AS DAY_OF_WEEK,
    DAYNAME(d)                          AS DAY_NAME,
    WEEKOFYEAR(d)                       AS WEEK_OF_YEAR,
    (DAYOFWEEKISO(d) >= 6)              AS IS_WEEKEND,
    (d = LAST_DAY(d))                   AS IS_MONTH_END,
    TO_CHAR(d, 'YYYY-MM')               AS YEAR_MONTH
FROM date_spine
WHERE d <= '2028-12-31'::DATE;

INSERT INTO GOLD.DIM_DATE
(DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH_NUM, MONTH_NAME, DAY_OF_MONTH,
 DAY_OF_WEEK, DAY_NAME, WEEK_OF_YEAR, IS_WEEKEND, IS_MONTH_END, YEAR_MONTH)
VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL, FALSE, FALSE, 'Unknown');
EOF

# ===== 02 dim_campaign (Type 1) =====
cat > sql/ddl/gold/02_dim_campaign.sql << 'EOF'
CREATE TABLE IF NOT EXISTS GOLD.DIM_CAMPAIGN (
    CAMPAIGN_KEY   NUMBER        NOT NULL,
    CAMPAIGN_ID    NUMBER,
    CAMPAIGN_NAME  VARCHAR,
    CHANNEL        VARCHAR,
    START_DATE     DATE,
    END_DATE       DATE,
    BUDGET         NUMBER(12,2),
    DURATION_DAYS  NUMBER,
    _DIM_RUN_ID    VARCHAR,
    _UPDATED_AT    TIMESTAMP_NTZ
);

MERGE INTO GOLD.DIM_CAMPAIGN t
USING (SELECT -1 AS CAMPAIGN_KEY) s ON t.CAMPAIGN_KEY = s.CAMPAIGN_KEY
WHEN NOT MATCHED THEN INSERT
(CAMPAIGN_KEY, CAMPAIGN_ID, CAMPAIGN_NAME, CHANNEL, START_DATE, END_DATE, BUDGET, DURATION_DAYS, _DIM_RUN_ID, _UPDATED_AT)
VALUES (-1, NULL, 'Unknown', 'Unknown', NULL, NULL, NULL, NULL, {{ DIM_RUN_ID }}, CURRENT_TIMESTAMP());

MERGE INTO GOLD.DIM_CAMPAIGN t
USING SILVER.MARKETING_CAMPAIGNS s ON t.CAMPAIGN_ID = s.CAMPAIGN_ID
WHEN MATCHED THEN UPDATE SET
    CAMPAIGN_NAME = s.CAMPAIGN_NAME, CHANNEL = s.CHANNEL,
    START_DATE = s.START_DATE, END_DATE = s.END_DATE,
    BUDGET = s.BUDGET, DURATION_DAYS = s.DURATION_DAYS,
    _DIM_RUN_ID = {{ DIM_RUN_ID }}, _UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(CAMPAIGN_KEY, CAMPAIGN_ID, CAMPAIGN_NAME, CHANNEL, START_DATE, END_DATE, BUDGET, DURATION_DAYS, _DIM_RUN_ID, _UPDATED_AT)
VALUES (GOLD.SEQ_DIM_CAMPAIGN.NEXTVAL, s.CAMPAIGN_ID, s.CAMPAIGN_NAME, s.CHANNEL,
        s.START_DATE, s.END_DATE, s.BUDGET, s.DURATION_DAYS, {{ DIM_RUN_ID }}, CURRENT_TIMESTAMP());
EOF

# ===== 03 dim_coupon (Type 1) =====
cat > sql/ddl/gold/03_dim_coupon.sql << 'EOF'
CREATE TABLE IF NOT EXISTS GOLD.DIM_COUPON (
    COUPON_KEY     NUMBER        NOT NULL,
    COUPON_ID      NUMBER,
    CODE           VARCHAR,
    DISCOUNT_TYPE  VARCHAR,
    DISCOUNT_VALUE NUMBER(10,2),
    START_DATE     DATE,
    END_DATE       DATE,
    _DIM_RUN_ID    VARCHAR,
    _UPDATED_AT    TIMESTAMP_NTZ
);

MERGE INTO GOLD.DIM_COUPON t
USING (SELECT -1 AS COUPON_KEY) s ON t.COUPON_KEY = s.COUPON_KEY
WHEN NOT MATCHED THEN INSERT
(COUPON_KEY, COUPON_ID, CODE, DISCOUNT_TYPE, DISCOUNT_VALUE, START_DATE, END_DATE, _DIM_RUN_ID, _UPDATED_AT)
VALUES (-1, NULL, 'Unknown', 'Unknown', NULL, NULL, NULL, {{ DIM_RUN_ID }}, CURRENT_TIMESTAMP());

MERGE INTO GOLD.DIM_COUPON t
USING SILVER.COUPONS s ON t.COUPON_ID = s.COUPON_ID
WHEN MATCHED THEN UPDATE SET
    CODE = s.CODE, DISCOUNT_TYPE = s.DISCOUNT_TYPE, DISCOUNT_VALUE = s.DISCOUNT_VALUE,
    START_DATE = s.START_DATE, END_DATE = s.END_DATE,
    _DIM_RUN_ID = {{ DIM_RUN_ID }}, _UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(COUPON_KEY, COUPON_ID, CODE, DISCOUNT_TYPE, DISCOUNT_VALUE, START_DATE, END_DATE, _DIM_RUN_ID, _UPDATED_AT)
VALUES (GOLD.SEQ_DIM_COUPON.NEXTVAL, s.COUPON_ID, s.CODE, s.DISCOUNT_TYPE,
        s.DISCOUNT_VALUE, s.START_DATE, s.END_DATE, {{ DIM_RUN_ID }}, CURRENT_TIMESTAMP());
EOF

# ===== 04 dim_customer (Type 2) =====
cat > sql/ddl/gold/04_dim_customer.sql << 'EOF'
CREATE TABLE IF NOT EXISTS GOLD.DIM_CUSTOMER (
    CUSTOMER_KEY   NUMBER        NOT NULL,
    CUSTOMER_ID    NUMBER,
    FIRST_NAME     VARCHAR,
    LAST_NAME      VARCHAR,
    FULL_NAME      VARCHAR,
    EMAIL          VARCHAR,
    CITY           VARCHAR,
    STATE          VARCHAR,
    COUNTRY        VARCHAR,
    AGE            NUMBER,
    AGE_BAND       VARCHAR,
    GENDER         VARCHAR,
    SIGNUP_DATE    DATE,
    ROW_HASH       VARCHAR,
    VALID_FROM     TIMESTAMP_NTZ,
    VALID_TO       TIMESTAMP_NTZ,
    IS_CURRENT     BOOLEAN,
    _DIM_RUN_ID    VARCHAR
);

MERGE INTO GOLD.DIM_CUSTOMER t
USING (SELECT -1 AS CUSTOMER_KEY) s ON t.CUSTOMER_KEY = s.CUSTOMER_KEY
WHEN NOT MATCHED THEN INSERT
(CUSTOMER_KEY, CUSTOMER_ID, FIRST_NAME, LAST_NAME, FULL_NAME, EMAIL, CITY, STATE, COUNTRY,
 AGE, AGE_BAND, GENDER, SIGNUP_DATE, ROW_HASH, VALID_FROM, VALID_TO, IS_CURRENT, _DIM_RUN_ID)
VALUES (-1, NULL, 'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown', 'Unknown', 'Unknown',
        NULL, 'Unknown', 'Unknown', NULL, 'UNKNOWN', '1900-01-01'::TIMESTAMP_NTZ, NULL, TRUE, {{ DIM_RUN_ID }});

CREATE OR REPLACE VIEW GOLD.STG_DIM_CUSTOMER AS
SELECT
    CUSTOMER_ID, FIRST_NAME, LAST_NAME,
    FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
    EMAIL, CITY, STATE, COUNTRY, AGE,
    CASE
        WHEN AGE IS NULL    THEN 'Unknown'
        WHEN AGE < 25       THEN '18-24'
        WHEN AGE < 35       THEN '25-34'
        WHEN AGE < 45       THEN '35-44'
        WHEN AGE < 55       THEN '45-54'
        WHEN AGE < 65       THEN '55-64'
        ELSE '65+'
    END AS AGE_BAND,
    GENDER, SIGNUP_DATE,
    MD5(
        COALESCE(FIRST_NAME,'') || '|' || COALESCE(LAST_NAME,'') || '|' ||
        COALESCE(EMAIL,'')      || '|' || COALESCE(CITY,'')      || '|' ||
        COALESCE(STATE,'')      || '|' || COALESCE(COUNTRY,'')   || '|' ||
        COALESCE(TO_VARCHAR(AGE),'') || '|' || COALESCE(GENDER,'')
    ) AS ROW_HASH
FROM SILVER.CUSTOMERS;

UPDATE GOLD.DIM_CUSTOMER d
SET VALID_TO = CURRENT_TIMESTAMP(), IS_CURRENT = FALSE
FROM GOLD.STG_DIM_CUSTOMER s
WHERE d.CUSTOMER_ID = s.CUSTOMER_ID
  AND d.IS_CURRENT = TRUE
  AND d.ROW_HASH <> s.ROW_HASH;

INSERT INTO GOLD.DIM_CUSTOMER
(CUSTOMER_KEY, CUSTOMER_ID, FIRST_NAME, LAST_NAME, FULL_NAME, EMAIL, CITY, STATE, COUNTRY,
 AGE, AGE_BAND, GENDER, SIGNUP_DATE, ROW_HASH, VALID_FROM, VALID_TO, IS_CURRENT, _DIM_RUN_ID)
SELECT
    GOLD.SEQ_DIM_CUSTOMER.NEXTVAL, s.CUSTOMER_ID, s.FIRST_NAME, s.LAST_NAME, s.FULL_NAME,
    s.EMAIL, s.CITY, s.STATE, s.COUNTRY, s.AGE, s.AGE_BAND, s.GENDER, s.SIGNUP_DATE,
    s.ROW_HASH, CURRENT_TIMESTAMP(), NULL, TRUE, {{ DIM_RUN_ID }}
FROM GOLD.STG_DIM_CUSTOMER s
LEFT JOIN GOLD.DIM_CUSTOMER d
    ON s.CUSTOMER_ID = d.CUSTOMER_ID AND d.IS_CURRENT = TRUE
WHERE d.CUSTOMER_KEY IS NULL;
EOF

# ===== 05 dim_product (Type 2) =====
cat > sql/ddl/gold/05_dim_product.sql << 'EOF'
CREATE TABLE IF NOT EXISTS GOLD.DIM_PRODUCT (
    PRODUCT_KEY     NUMBER        NOT NULL,
    PRODUCT_ID      NUMBER,
    PRODUCT_NAME    VARCHAR,
    CATEGORY_ID     NUMBER,
    CATEGORY_NAME   VARCHAR,
    PARENT_CATEGORY VARCHAR,
    BRAND           VARCHAR,
    SUPPLIER_ID     NUMBER,
    SUPPLIER_NAME   VARCHAR,
    SUPPLIER_COUNTRY VARCHAR,
    UNIT_PRICE      NUMBER(10,2),
    COST            NUMBER(10,2),
    UNIT_MARGIN     NUMBER(10,2),
    ROW_HASH        VARCHAR,
    VALID_FROM      TIMESTAMP_NTZ,
    VALID_TO        TIMESTAMP_NTZ,
    IS_CURRENT      BOOLEAN,
    _DIM_RUN_ID     VARCHAR
);

MERGE INTO GOLD.DIM_PRODUCT t
USING (SELECT -1 AS PRODUCT_KEY) s ON t.PRODUCT_KEY = s.PRODUCT_KEY
WHEN NOT MATCHED THEN INSERT
(PRODUCT_KEY, PRODUCT_ID, PRODUCT_NAME, CATEGORY_ID, CATEGORY_NAME, PARENT_CATEGORY, BRAND,
 SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, UNIT_PRICE, COST, UNIT_MARGIN,
 ROW_HASH, VALID_FROM, VALID_TO, IS_CURRENT, _DIM_RUN_ID)
VALUES (-1, NULL, 'Unknown', NULL, 'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown', 'Unknown',
        NULL, NULL, NULL, 'UNKNOWN', '1900-01-01'::TIMESTAMP_NTZ, NULL, TRUE, {{ DIM_RUN_ID }});

CREATE OR REPLACE VIEW GOLD.STG_DIM_PRODUCT AS
SELECT
    p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY_ID,
    c.CATEGORY_NAME,
    pc.CATEGORY_NAME AS PARENT_CATEGORY,
    p.BRAND, p.SUPPLIER_ID,
    s.SUPPLIER_NAME, s.COUNTRY AS SUPPLIER_COUNTRY,
    p.UNIT_PRICE, p.COST, p.UNIT_MARGIN,
    MD5(
        COALESCE(p.PRODUCT_NAME,'')        || '|' || COALESCE(c.CATEGORY_NAME,'') || '|' ||
        COALESCE(p.BRAND,'')               || '|' || COALESCE(s.SUPPLIER_NAME,'') || '|' ||
        COALESCE(TO_VARCHAR(p.UNIT_PRICE),'') || '|' || COALESCE(TO_VARCHAR(p.COST),'')
    ) AS ROW_HASH
FROM SILVER.PRODUCTS p
LEFT JOIN SILVER.CATEGORIES c  ON p.CATEGORY_ID = c.CATEGORY_ID
LEFT JOIN SILVER.CATEGORIES pc ON c.PARENT_CATEGORY_ID = pc.CATEGORY_ID
LEFT JOIN SILVER.SUPPLIERS s   ON p.SUPPLIER_ID = s.SUPPLIER_ID;

UPDATE GOLD.DIM_PRODUCT d
SET VALID_TO = CURRENT_TIMESTAMP(), IS_CURRENT = FALSE
FROM GOLD.STG_DIM_PRODUCT s
WHERE d.PRODUCT_ID = s.PRODUCT_ID
  AND d.IS_CURRENT = TRUE
  AND d.ROW_HASH <> s.ROW_HASH;

INSERT INTO GOLD.DIM_PRODUCT
(PRODUCT_KEY, PRODUCT_ID, PRODUCT_NAME, CATEGORY_ID, CATEGORY_NAME, PARENT_CATEGORY, BRAND,
 SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, UNIT_PRICE, COST, UNIT_MARGIN,
 ROW_HASH, VALID_FROM, VALID_TO, IS_CURRENT, _DIM_RUN_ID)
SELECT
    GOLD.SEQ_DIM_PRODUCT.NEXTVAL, s.PRODUCT_ID, s.PRODUCT_NAME, s.CATEGORY_ID, s.CATEGORY_NAME,
    s.PARENT_CATEGORY, s.BRAND, s.SUPPLIER_ID, s.SUPPLIER_NAME, s.SUPPLIER_COUNTRY,
    s.UNIT_PRICE, s.COST, s.UNIT_MARGIN, s.ROW_HASH, CURRENT_TIMESTAMP(), NULL, TRUE, {{ DIM_RUN_ID }}
FROM GOLD.STG_DIM_PRODUCT s
LEFT JOIN GOLD.DIM_PRODUCT d
    ON s.PRODUCT_ID = d.PRODUCT_ID AND d.IS_CURRENT = TRUE
WHERE d.PRODUCT_KEY IS NULL;
EOF

ls -la sql/ddl/gold/
```

The `ls` should list six `.sql` files (`00`–`05`).

## 6.2 Orchestrator

This uses the connector's `execute_string()`, which handles multi-statement files robustly:

```bash
cat > src/transformations/gold_dims.py << 'EOF'
"""
Gold dimension loader. Runs sql/ddl/gold/*.sql in filename order,
substituting the {{ DIM_RUN_ID }} placeholder, logging each file to
GOLD._GOLD_RUN_LOG.
"""
import re
import uuid
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.utils.db import get_snowflake_connection

GOLD_SCHEMA = "GOLD"
LOG_TABLE = "_GOLD_RUN_LOG"
SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "gold"


def ensure_log_table(sf_conn) -> None:
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{LOG_TABLE} (
                GOLD_RUN_ID    VARCHAR        NOT NULL,
                OBJECT_NAME    VARCHAR        NOT NULL,
                STARTED_AT     TIMESTAMP_NTZ  NOT NULL,
                ENDED_AT       TIMESTAMP_NTZ,
                ROW_COUNT      NUMBER,
                STATUS         VARCHAR        NOT NULL,
                ERROR_MESSAGE  VARCHAR
            );
        """)
    finally:
        cur.close()


def log_run(sf_conn, run_id, obj, started, ended, row_count, status, error=None):
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {GOLD_SCHEMA}.{LOG_TABLE}
            (GOLD_RUN_ID, OBJECT_NAME, STARTED_AT, ENDED_AT, ROW_COUNT, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (run_id, obj, started, ended, row_count, status, error))
    finally:
        cur.close()


def table_name_from_file(sql_file: Path) -> str | None:
    """01_dim_date.sql -> DIM_DATE; 00_sequences.sql -> None (no table)."""
    stem = re.sub(r"^\d+_", "", sql_file.stem)
    if stem == "sequences":
        return None
    return stem.upper()


def run_gold_dimensions() -> str:
    run_id = f"golddim_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logger.info(f"=== Gold dimension run start: {run_id} ===")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(f"No SQL files in {SQL_DIR}. Aborting.")
        return run_id

    sf = get_snowflake_connection(schema=GOLD_SCHEMA)
    successes = failures = 0
    try:
        ensure_log_table(sf)
        for sql_file in sql_files:
            obj = table_name_from_file(sql_file) or sql_file.stem
            started = datetime.utcnow()
            row_count, status, error = 0, "RUNNING", None
            try:
                sql = sql_file.read_text().replace("{{ DIM_RUN_ID }}", f"'{run_id}'")
                logger.info(f"[{obj}] Executing {sql_file.name} ...")
                sf.execute_string(sql, remove_comments=False)

                table = table_name_from_file(sql_file)
                if table:
                    cur = sf.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.{table};")
                    row_count = cur.fetchone()[0]
                    cur.close()
                    logger.success(f"[{obj}] {row_count:,} rows in GOLD.{table}")
                else:
                    logger.success(f"[{obj}] done")
                status = "SUCCESS"
                successes += 1
            except Exception as e:
                status, error = "FAILED", str(e)[:1000]
                logger.exception(f"[{obj}] Failed: {e}")
                failures += 1
            finally:
                log_run(sf, run_id, obj, started, datetime.utcnow(),
                        row_count, status, error)
    finally:
        sf.close()

    logger.info(f"=== Gold dimension run done: {run_id} | success={successes} failed={failures} ===")
    return run_id
EOF

cat > src/transformations/run_gold_dims.py << 'EOF'
"""CLI entry: python -m src.transformations.run_gold_dims"""
from src.transformations.gold_dims import run_gold_dimensions

if __name__ == "__main__":
    run_gold_dimensions()
EOF
```

## 6.3 Verification script

```bash
cat > src/transformations/verify_gold_dims.py << 'EOF'
"""Summary of the Gold dimensions, including SCD2 current vs historical counts."""
from src.utils.db import get_snowflake_connection

GOLD = "GOLD"


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def main():
    sf = get_snowflake_connection(schema=GOLD)
    cur = sf.cursor()
    try:
        print("Type 1 / static dimensions")
        print("-" * 48)
        for d in ["DIM_DATE", "DIM_CAMPAIGN", "DIM_COUPON"]:
            total = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{d};")
            unknown = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{d} WHERE "
                                  f"{'DATE_KEY' if d=='DIM_DATE' else d.split('_')[1]+'_KEY'} = -1;")
            print(f"  {d:<16} rows={total:>8,}  unknown_member={'yes' if unknown else 'NO'}")

        print("\nSCD Type 2 dimensions")
        print("-" * 48)
        for d, key in [("DIM_CUSTOMER", "CUSTOMER"), ("DIM_PRODUCT", "PRODUCT")]:
            total   = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{d};")
            current = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{d} WHERE IS_CURRENT = TRUE;")
            hist    = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{d} WHERE IS_CURRENT = FALSE;")
            distinct_bk = scalar(cur, f"SELECT COUNT(DISTINCT {key}_ID) FROM {GOLD}.{d} WHERE {key}_ID IS NOT NULL;")
            print(f"  {d:<16} total={total:>8,}  current={current:>8,}  "
                  f"historical={hist:>6,}  distinct_business_keys={distinct_bk:>6,}")
    finally:
        cur.close()
        sf.close()


if __name__ == "__main__":
    main()
EOF
```

## 6.4 Run it

```bash
python -m src.transformations.run_gold_dims
```

Expect `success=6 failed=0` and row counts roughly: `DIM_DATE` ≈ 2,558 (2,557 days + unknown), `DIM_CAMPAIGN` 31, `DIM_COUPON` 51, `DIM_CUSTOMER` 2,001, `DIM_PRODUCT` 301 (each +1 for the unknown member).

## 6.5 Verify

```bash
python -m src.transformations.verify_gold_dims
```

Expected:

```
Type 1 / static dimensions
------------------------------------------------
  DIM_DATE         rows=   2,558  unknown_member=yes
  DIM_CAMPAIGN     rows=      31  unknown_member=yes
  DIM_COUPON       rows=      51  unknown_member=yes

SCD Type 2 dimensions
------------------------------------------------
  DIM_CUSTOMER     total=   2,001  current=   2,001  historical=     0  distinct_business_keys=  2,000
  DIM_PRODUCT      total=     301  current=     301  historical=     0  distinct_business_keys=   300
```

`historical=0` is correct on the first load — nothing has changed yet. Now we make something change.

## 6.6 Prove SCD Type 2 works (the money shot)

Change one customer's city in the source, re-run the pipeline, and watch a historical version appear.

Step 1 — update the source (Python one-liner, no psql needed):

```bash
python -c "
from sqlalchemy import text
from src.utils.db import get_postgres_engine
e = get_postgres_engine()
with e.begin() as c:
    c.execute(text(\"UPDATE customers SET city = 'Springfield' WHERE customer_id = 1\"))
print('customer_id=1 city changed to Springfield')
"
```

(If your source schema isn't `public`, change `customers` to `your_schema.customers`.)

Step 2 — re-run the three layers:

```bash
python -m src.ingestion.run_bronze
python -m src.transformations.run_silver
python -m src.transformations.run_gold_dims
```

Step 3 — observe the history. In Snowsight:

```sql
SELECT CUSTOMER_KEY, CUSTOMER_ID, CITY, VALID_FROM, VALID_TO, IS_CURRENT
FROM GOLD.DIM_CUSTOMER
WHERE CUSTOMER_ID = 1
ORDER BY VALID_FROM;
```

You should now see **two rows** for `CUSTOMER_ID = 1`:

| CUSTOMER_KEY | CITY | VALID_FROM | VALID_TO | IS_CURRENT |
|---|---|---|---|---|
| (original) | (old city) | first load | now | FALSE |
| (new) | Springfield | now | NULL | TRUE |

Re-running `verify_gold_dims` will now show `DIM_CUSTOMER historical=1`. That single expired row *is* the SCD Type 2 working — the customer's order history before the move still joins to their old city, and orders after the move join to Springfield. That's the whole point, and it's exactly what you walk an interviewer through.

Idempotency check: run `python -m src.transformations.run_gold_dims` once more without changing anything — `historical` stays at 1, no new rows. The hash matched, so nothing was expired or inserted. That proves the load is safe to re-run.

## 6.7 Common failure modes

- **`SQL compilation error: Object 'SILVER.CATEGORIES' does not exist`** in `dim_product` — the parent-category self-join needs `SILVER.CATEGORIES` twice; it exists, so this would only fire on a grants issue. Re-run the SILVER grants from Step 4 troubleshooting.
- **`Sequence 'SEQ_DIM_CUSTOMER' does not exist`** — `00_sequences.sql` didn't run first. Confirm filenames start with the numeric prefix so `sorted()` orders them correctly.
- **Duplicate unknown-member rows on re-run** — won't happen; the seed uses `MERGE ... WHEN NOT MATCHED`, so the `-1` row inserts once and is skipped thereafter.
- **`execute_string` silently runs only the first statement** — only if `remove_comments` strips something oddly; we pass `remove_comments=False` and the files have no inline `--` comments, so all statements run.

## Confirmation checklist before Step 7

1. `python -m src.transformations.run_gold_dims` → `success=6 failed=0`.
2. `verify_gold_dims` shows all five dimensions with their unknown members, `historical=0` initially.
3. The SCD2 test produces a second row for `CUSTOMER_ID = 1` (one `IS_CURRENT=FALSE`, one `TRUE`), and `verify_gold_dims` then reports `historical=1`.
4. Re-running the gold load without source changes leaves `historical` unchanged (idempotent).

Paste the `verify_gold_dims` output from *after* the SCD2 test, plus the two-row result for customer 1. Step 7 builds the six fact tables — each does surrogate-key lookups against these dimensions (joining on business key + the point-in-time `VALID_FROM`/`VALID_TO` window for the Type 2 dims), routes orphans to the `-1` unknown members, and computes the additive measures. `FACT_SALES` first, then the rest.

> **Critical SCD2 fix applied here** — the initial VALID_FROM for Type 2 dims must be anchored to `'1900-01-01'` (NOT `CURRENT_TIMESTAMP()`), otherwise historical facts dated in 2024-2026 fall outside the initial dimension version's validity window and get orphaned to -1 by point-in-time joins. Confirm the `dim_customer.sql` and `dim_product.sql` inserts use `'1900-01-01'::TIMESTAMP_NTZ` for the initial VALID_FROM.


---



## Step 7: Gold Layer — Fact Tables

## What we build

Six facts. Each looks up surrogate keys against the dimensions, routes any orphan to the `-1` unknown member, and carries additive measures. The two Type 2 dims (`DIM_CUSTOMER`, `DIM_PRODUCT`) use the point-in-time window join — match on business key *and* transaction date inside `[VALID_FROM, VALID_TO)`. This is why SCD2 matters: `FACT_SALES` pulls `COST` from the product version that was current *when the sale happened*, so historical margin stays correct even after a repricing.

## 7.1 Fact SQL files

```bash
cd ~/ecommerce-data-platform
mkdir -p sql/ddl/gold/facts

# ===== fact_sales =====
cat > sql/ddl/gold/facts/01_fact_sales.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_SALES AS
SELECT
    oi.ORDER_ITEM_ID,
    o.ORDER_ID,
    COALESCE(dd.DATE_KEY, -1)       AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1)   AS CUSTOMER_KEY,
    COALESCE(dp.PRODUCT_KEY, -1)    AS PRODUCT_KEY,
    COALESCE(dcam.CAMPAIGN_KEY, -1) AS CAMPAIGN_KEY,
    COALESCE(dcoup.COUPON_KEY, -1)  AS COUPON_KEY,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    (oi.QUANTITY * oi.UNIT_PRICE)::NUMBER(12,2)                            AS GROSS_REVENUE,
    oi.DISCOUNT                                                            AS DISCOUNT_AMT,
    oi.TAX                                                                 AS TAX_AMT,
    (oi.QUANTITY * oi.UNIT_PRICE - oi.DISCOUNT)::NUMBER(12,2)              AS NET_REVENUE,
    (oi.QUANTITY * COALESCE(dp.COST, 0))::NUMBER(12,2)                     AS COST_AMT,
    (oi.QUANTITY * oi.UNIT_PRICE - oi.DISCOUNT
       - oi.QUANTITY * COALESCE(dp.COST, 0))::NUMBER(12,2)                 AS GROSS_MARGIN,
    o.ORDER_DATE,
    CURRENT_TIMESTAMP()  AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}    AS _FACT_RUN_ID
FROM SILVER.ORDER_ITEMS oi
JOIN SILVER.ORDERS o          ON oi.ORDER_ID = o.ORDER_ID
LEFT JOIN GOLD.DIM_DATE dd    ON TO_NUMBER(TO_CHAR(o.ORDER_DATE, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON o.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND o.ORDER_DATE >= dc.VALID_FROM
                              AND o.ORDER_DATE <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ)
LEFT JOIN GOLD.DIM_PRODUCT dp  ON oi.PRODUCT_ID = dp.PRODUCT_ID
                              AND o.ORDER_DATE >= dp.VALID_FROM
                              AND o.ORDER_DATE <  COALESCE(dp.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ)
LEFT JOIN GOLD.DIM_CAMPAIGN dcam ON o.CAMPAIGN_ID = dcam.CAMPAIGN_ID
LEFT JOIN GOLD.DIM_COUPON dcoup  ON o.COUPON_ID = dcoup.COUPON_ID;
EOF

# ===== fact_payments =====
cat > sql/ddl/gold/facts/02_fact_payments.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_PAYMENTS AS
SELECT
    p.PAYMENT_ID,
    p.ORDER_ID,
    p.TRANSACTION_ID,
    COALESCE(dd.DATE_KEY, -1)     AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    p.AMOUNT,
    p.PAYMENT_STATUS,
    p.METHOD AS PAYMENT_METHOD,
    p.PAYMENT_DATE,
    CURRENT_TIMESTAMP() AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}   AS _FACT_RUN_ID
FROM SILVER.PAYMENTS p
JOIN SILVER.ORDERS o          ON p.ORDER_ID = o.ORDER_ID
LEFT JOIN GOLD.DIM_DATE dd    ON TO_NUMBER(TO_CHAR(p.PAYMENT_DATE, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON o.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND p.PAYMENT_DATE >= dc.VALID_FROM
                              AND p.PAYMENT_DATE <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ);
EOF

# ===== fact_shipments =====
cat > sql/ddl/gold/facts/03_fact_shipments.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_SHIPMENTS AS
SELECT
    sh.SHIPMENT_ID,
    sh.ORDER_ID,
    COALESCE(dd.DATE_KEY, -1)     AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    sh.SHIPPING_STATUS,
    sh.CARRIER,
    sh.SHIPPING_COST,
    sh.DAYS_TO_DELIVER,
    sh.SHIPPED_DATE,
    sh.DELIVERY_DATE,
    CURRENT_TIMESTAMP() AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}   AS _FACT_RUN_ID
FROM SILVER.SHIPMENTS sh
JOIN SILVER.ORDERS o          ON sh.ORDER_ID = o.ORDER_ID
LEFT JOIN GOLD.DIM_DATE dd    ON TO_NUMBER(TO_CHAR(sh.SHIPPED_DATE, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON o.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND sh.SHIPPED_DATE >= dc.VALID_FROM
                              AND sh.SHIPPED_DATE <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ);
EOF

# ===== fact_returns =====
cat > sql/ddl/gold/facts/04_fact_returns.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_RETURNS AS
SELECT
    r.RETURN_ID,
    r.ORDER_ITEM_ID,
    oi.ORDER_ID,
    COALESCE(dd.DATE_KEY, -1)     AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    COALESCE(dp.PRODUCT_KEY, -1)  AS PRODUCT_KEY,
    r.REASON,
    r.REFUND_AMOUNT,
    r.RETURN_DATE,
    CURRENT_TIMESTAMP() AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}   AS _FACT_RUN_ID
FROM SILVER.RETURNS r
JOIN SILVER.ORDER_ITEMS oi    ON r.ORDER_ITEM_ID = oi.ORDER_ITEM_ID
JOIN SILVER.ORDERS o          ON oi.ORDER_ID = o.ORDER_ID
LEFT JOIN GOLD.DIM_DATE dd    ON TO_NUMBER(TO_CHAR(r.RETURN_DATE, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON o.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND r.RETURN_DATE >= dc.VALID_FROM
                              AND r.RETURN_DATE <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ)
LEFT JOIN GOLD.DIM_PRODUCT dp  ON oi.PRODUCT_ID = dp.PRODUCT_ID
                              AND r.RETURN_DATE >= dp.VALID_FROM
                              AND r.RETURN_DATE <  COALESCE(dp.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ);
EOF

# ===== fact_reviews =====
cat > sql/ddl/gold/facts/05_fact_reviews.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_REVIEWS AS
SELECT
    rv.REVIEW_ID,
    rv.ORDER_ID,
    COALESCE(dd.DATE_KEY, -1)     AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    COALESCE(dp.PRODUCT_KEY, -1)  AS PRODUCT_KEY,
    rv.RATING,
    rv.REVIEW_DATE,
    CURRENT_TIMESTAMP() AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}   AS _FACT_RUN_ID
FROM SILVER.REVIEWS rv
LEFT JOIN GOLD.DIM_DATE dd     ON TO_NUMBER(TO_CHAR(rv.REVIEW_DATE, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON rv.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND rv.REVIEW_DATE >= dc.VALID_FROM
                              AND rv.REVIEW_DATE <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ)
LEFT JOIN GOLD.DIM_PRODUCT dp  ON rv.PRODUCT_ID = dp.PRODUCT_ID
                              AND rv.REVIEW_DATE >= dp.VALID_FROM
                              AND rv.REVIEW_DATE <  COALESCE(dp.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ);
EOF

# ===== fact_web_sessions =====
cat > sql/ddl/gold/facts/06_fact_web_sessions.sql << 'EOF'
CREATE OR REPLACE TABLE GOLD.FACT_WEB_SESSIONS AS
SELECT
    ws.SESSION_ID,
    COALESCE(dd.DATE_KEY, -1)       AS DATE_KEY,
    COALESCE(dc.CUSTOMER_KEY, -1)   AS CUSTOMER_KEY,
    COALESCE(dcam.CAMPAIGN_KEY, -1) AS CAMPAIGN_KEY,
    ws.SOURCE,
    ws.MEDIUM,
    ws.DEVICE,
    ws.SESSION_DURATION_SEC,
    ws.SESSION_START,
    CURRENT_TIMESTAMP() AS _FACT_LOADED_AT,
    {{ FACT_RUN_ID }}   AS _FACT_RUN_ID
FROM SILVER.WEB_SESSIONS ws
LEFT JOIN GOLD.DIM_DATE dd     ON TO_NUMBER(TO_CHAR(ws.SESSION_START, 'YYYYMMDD')) = dd.DATE_KEY
LEFT JOIN GOLD.DIM_CUSTOMER dc ON ws.CUSTOMER_ID = dc.CUSTOMER_ID
                              AND ws.SESSION_START >= dc.VALID_FROM
                              AND ws.SESSION_START <  COALESCE(dc.VALID_TO, '9999-12-31'::TIMESTAMP_NTZ)
LEFT JOIN GOLD.DIM_CAMPAIGN dcam ON ws.CAMPAIGN_ID = dcam.CAMPAIGN_ID;
EOF

ls -la sql/ddl/gold/facts/
```

`ls` should show six fact files.

## 7.2 Fact orchestrator

```bash
cat > src/transformations/gold_facts.py << 'EOF'
"""
Gold fact loader. Runs sql/ddl/gold/facts/*.sql in order, substituting
{{ FACT_RUN_ID }}, logging each to GOLD._GOLD_RUN_LOG.
"""
import re
import uuid
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.utils.db import get_snowflake_connection

GOLD_SCHEMA = "GOLD"
LOG_TABLE = "_GOLD_RUN_LOG"
SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "gold" / "facts"


def ensure_log_table(sf_conn) -> None:
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{LOG_TABLE} (
                GOLD_RUN_ID    VARCHAR        NOT NULL,
                OBJECT_NAME    VARCHAR        NOT NULL,
                STARTED_AT     TIMESTAMP_NTZ  NOT NULL,
                ENDED_AT       TIMESTAMP_NTZ,
                ROW_COUNT      NUMBER,
                STATUS         VARCHAR        NOT NULL,
                ERROR_MESSAGE  VARCHAR
            );
        """)
    finally:
        cur.close()


def log_run(sf_conn, run_id, obj, started, ended, row_count, status, error=None):
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {GOLD_SCHEMA}.{LOG_TABLE}
            (GOLD_RUN_ID, OBJECT_NAME, STARTED_AT, ENDED_AT, ROW_COUNT, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (run_id, obj, started, ended, row_count, status, error))
    finally:
        cur.close()


def table_name_from_file(sql_file: Path) -> str:
    return re.sub(r"^\d+_", "", sql_file.stem).upper()


def run_gold_facts() -> str:
    run_id = f"goldfact_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logger.info(f"=== Gold fact run start: {run_id} ===")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(f"No SQL files in {SQL_DIR}. Aborting.")
        return run_id

    sf = get_snowflake_connection(schema=GOLD_SCHEMA)
    successes = failures = 0
    try:
        ensure_log_table(sf)
        for sql_file in sql_files:
            obj = table_name_from_file(sql_file)
            started = datetime.utcnow()
            row_count, status, error = 0, "RUNNING", None
            try:
                sql = sql_file.read_text().replace("{{ FACT_RUN_ID }}", f"'{run_id}'")
                logger.info(f"[{obj}] Executing {sql_file.name} ...")
                sf.execute_string(sql, remove_comments=False)
                cur = sf.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.{obj};")
                row_count = cur.fetchone()[0]
                cur.close()
                logger.success(f"[{obj}] {row_count:,} rows in GOLD.{obj}")
                status = "SUCCESS"
                successes += 1
            except Exception as e:
                status, error = "FAILED", str(e)[:1000]
                logger.exception(f"[{obj}] Failed: {e}")
                failures += 1
            finally:
                log_run(sf, run_id, obj, started, datetime.utcnow(),
                        row_count, status, error)
    finally:
        sf.close()

    logger.info(f"=== Gold fact run done: {run_id} | success={successes} failed={failures} ===")
    return run_id
EOF

cat > src/transformations/run_gold_facts.py << 'EOF'
"""CLI entry: python -m src.transformations.run_gold_facts"""
from src.transformations.gold_facts import run_gold_facts

if __name__ == "__main__":
    run_gold_facts()
EOF
```

## 7.3 Verification — row counts and orphan rates

Orphan rate (% of rows pointing at `-1`) is the key data-quality metric: near-zero for required keys means joins are healthy; high rates on *nullable* keys (campaign, coupon) are expected and correct.

```bash
cat > src/transformations/verify_gold_facts.py << 'EOF'
"""Fact row counts + orphan (-1 surrogate key) rates per dimension key."""
from src.utils.db import get_snowflake_connection

GOLD = "GOLD"

FACTS = {
    "FACT_SALES":        ["DATE_KEY", "CUSTOMER_KEY", "PRODUCT_KEY", "CAMPAIGN_KEY", "COUPON_KEY"],
    "FACT_PAYMENTS":     ["DATE_KEY", "CUSTOMER_KEY"],
    "FACT_SHIPMENTS":    ["DATE_KEY", "CUSTOMER_KEY"],
    "FACT_RETURNS":      ["DATE_KEY", "CUSTOMER_KEY", "PRODUCT_KEY"],
    "FACT_REVIEWS":      ["DATE_KEY", "CUSTOMER_KEY", "PRODUCT_KEY"],
    "FACT_WEB_SESSIONS": ["DATE_KEY", "CUSTOMER_KEY", "CAMPAIGN_KEY"],
}


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def main():
    sf = get_snowflake_connection(schema=GOLD)
    cur = sf.cursor()
    try:
        for fact, keys in FACTS.items():
            total = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{fact};")
            print(f"\n{fact}  ({total:,} rows)")
            for k in keys:
                orphans = scalar(cur, f"SELECT COUNT(*) FROM {GOLD}.{fact} WHERE {k} = -1;")
                pct = (orphans / total * 100) if total else 0.0
                flag = "  <- expected (nullable)" if k in ("CAMPAIGN_KEY", "COUPON_KEY") and orphans else ""
                print(f"    {k:<14} orphans={orphans:>7,} ({pct:5.1f}%){flag}")
    finally:
        cur.close()
        sf.close()


if __name__ == "__main__":
    main()
EOF
```

## 7.4 Run

```bash
python -m src.transformations.run_gold_facts
```

Expect `success=6 failed=0` with: `FACT_SALES` 15,000, `FACT_PAYMENTS` 5,000, `FACT_SHIPMENTS` 3,277, `FACT_RETURNS` 1,000, `FACT_REVIEWS` 1,979, `FACT_WEB_SESSIONS` 8,000.

## 7.5 Verify

```bash
python -m src.transformations.verify_gold_facts
```

What healthy output looks like: `DATE_KEY`, `CUSTOMER_KEY`, `PRODUCT_KEY` orphans at or near `0.0%` (every fact resolved to a real dimension version). `CAMPAIGN_KEY` and `COUPON_KEY` on `FACT_SALES` will show meaningful orphan percentages — that's correct, because most orders carry no campaign or coupon, so they route to the `-1` unknown member by design. `FACT_WEB_SESSIONS.CUSTOMER_KEY` may also show orphans (guest sessions with no customer). If `DATE_KEY` or `CUSTOMER_KEY`/`PRODUCT_KEY` show high orphan rates, that signals a join problem — tell me and we'll debug.

## 7.6 Snowsight payoff queries

Now the model actually answers business questions. Run a few to confirm it all hangs together:

```sql
USE DATABASE ECOMMERCE_DW;

-- Monthly net revenue and margin
SELECT d.YEAR, d.MONTH_NUM, d.MONTH_NAME,
       SUM(f.NET_REVENUE) AS NET_REVENUE,
       SUM(f.GROSS_MARGIN) AS GROSS_MARGIN
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR, d.MONTH_NUM, d.MONTH_NAME
ORDER BY d.YEAR, d.MONTH_NUM;

-- Top 10 products by revenue
SELECT p.PRODUCT_NAME, p.CATEGORY_NAME,
       SUM(f.NET_REVENUE) AS REVENUE, SUM(f.QUANTITY) AS UNITS
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
GROUP BY p.PRODUCT_NAME, p.CATEGORY_NAME
ORDER BY REVENUE DESC
LIMIT 10;

-- Revenue by customer country
SELECT c.COUNTRY, COUNT(DISTINCT f.ORDER_ID) AS ORDERS, SUM(f.NET_REVENUE) AS REVENUE
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
GROUP BY c.COUNTRY
ORDER BY REVENUE DESC;

-- Return rate by category (joining two facts via conformed dims)
SELECT p.CATEGORY_NAME,
       SUM(s.NET_REVENUE) AS SALES_REVENUE,
       COALESCE(SUM(r.REFUND_AMOUNT), 0) AS REFUNDS
FROM GOLD.FACT_SALES s
JOIN GOLD.DIM_PRODUCT p ON s.PRODUCT_KEY = p.PRODUCT_KEY
LEFT JOIN GOLD.FACT_RETURNS r ON s.ORDER_ID = r.ORDER_ID AND s.PRODUCT_KEY = r.PRODUCT_KEY
GROUP BY p.CATEGORY_NAME
ORDER BY SALES_REVENUE DESC;
```

## 7.7 Common failure modes

- **High `DATE_KEY` orphan rate** — order dates fall outside the `DIM_DATE` range (2022–2028). Check `SELECT MIN(ORDER_DATE), MAX(ORDER_DATE) FROM SILVER.ORDERS;` and widen the `GENERATOR` rowcount in `01_dim_date.sql` if needed.
- **High `CUSTOMER_KEY`/`PRODUCT_KEY` orphan rate** — almost always the `VALID_FROM` anchoring issue. Confirm you applied the `04`/`05` patch and rebuilt; check `SELECT MIN(VALID_FROM) FROM GOLD.DIM_CUSTOMER;` returns `1900-01-01`.
- **`Object 'GOLD.DIM_CUSTOMER' does not exist`** — the dims must exist before facts. Run `run_gold_dims` first.

## Confirmation checklist before Step 8

1. `04`/`05` patched, Type 2 dims dropped and rebuilt; `MIN(VALID_FROM)` on both is `1900-01-01`.
2. `run_gold_facts` → `success=6 failed=0`, six fact tables at the expected row counts.
3. `verify_gold_facts` shows `DATE_KEY`/`CUSTOMER_KEY`/`PRODUCT_KEY` orphans ≈ 0%; campaign/coupon orphans are fine.
4. The Snowsight payoff queries return sensible numbers.

Paste the `verify_gold_facts` output. Step 8 will be a short consolidation: a `run_pipeline.py` that chains bronze → silver → gold-dims → gold-facts into one command with end-to-end logging, plus a couple of Gold-layer reporting views (`VW_SALES_DETAIL`, `VW_CUSTOMER_360`) that flatten the star for easy Power BI consumption. Then Step 9 is Airflow orchestration, and Step 10+ is Power BI and DAX.

---



## Step 8: Pipeline Consolidation + Semantic Views

Two things here: a single command that runs the whole pipeline end-to-end (this is also what Airflow will call in Step 9), and a few Gold semantic views that flatten the star for convenient consumption.

## 8.1 Semantic views

Three views: a unified pipeline-observability view across all three run logs, a flattened sales view, and a customer-360 that aggregates *across SCD2 versions* (note it groups by the business key `CUSTOMER_ID`, not the surrogate key, so a customer's full history rolls up correctly).

```bash
cd ~/ecommerce-data-platform
mkdir -p sql/ddl/gold/views

# ===== unified run log =====
cat > sql/ddl/gold/views/01_vw_pipeline_runs.sql << 'EOF'
CREATE OR REPLACE VIEW GOLD.VW_PIPELINE_RUNS AS
SELECT 'BRONZE' AS LAYER, INGESTION_ID AS RUN_ID, TABLE_NAME AS OBJECT_NAME,
       STARTED_AT, ENDED_AT, ROWS_LOADED AS ROW_COUNT, STATUS, ERROR_MESSAGE
FROM BRONZE._INGESTION_LOG
UNION ALL
SELECT 'SILVER', SILVER_RUN_ID, TABLE_NAME, STARTED_AT, ENDED_AT, ROW_COUNT, STATUS, ERROR_MESSAGE
FROM SILVER._SILVER_RUN_LOG
UNION ALL
SELECT 'GOLD', GOLD_RUN_ID, OBJECT_NAME, STARTED_AT, ENDED_AT, ROW_COUNT, STATUS, ERROR_MESSAGE
FROM GOLD._GOLD_RUN_LOG;
EOF

# ===== flattened sales =====
cat > sql/ddl/gold/views/02_vw_sales_detail.sql << 'EOF'
CREATE OR REPLACE VIEW GOLD.VW_SALES_DETAIL AS
SELECT
    f.ORDER_ITEM_ID, f.ORDER_ID,
    d.FULL_DATE, d.YEAR, d.QUARTER, d.MONTH_NUM, d.MONTH_NAME, d.YEAR_MONTH, d.IS_WEEKEND,
    c.CUSTOMER_ID, c.FULL_NAME AS CUSTOMER_NAME, c.CITY, c.STATE, c.COUNTRY, c.AGE_BAND, c.GENDER,
    p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY_NAME, p.BRAND, p.SUPPLIER_NAME,
    cam.CAMPAIGN_NAME, cam.CHANNEL,
    cp.CODE AS COUPON_CODE, cp.DISCOUNT_TYPE,
    f.QUANTITY, f.GROSS_REVENUE, f.DISCOUNT_AMT, f.TAX_AMT,
    f.NET_REVENUE, f.COST_AMT, f.GROSS_MARGIN
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_DATE d       ON f.DATE_KEY = d.DATE_KEY
JOIN GOLD.DIM_CUSTOMER c   ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
JOIN GOLD.DIM_PRODUCT p    ON f.PRODUCT_KEY = p.PRODUCT_KEY
JOIN GOLD.DIM_CAMPAIGN cam ON f.CAMPAIGN_KEY = cam.CAMPAIGN_KEY
JOIN GOLD.DIM_COUPON cp    ON f.COUPON_KEY = cp.COUPON_KEY;
EOF

# ===== customer 360 (aggregates across SCD2 versions) =====
cat > sql/ddl/gold/views/03_vw_customer_360.sql << 'EOF'
CREATE OR REPLACE VIEW GOLD.VW_CUSTOMER_360 AS
WITH sales AS (
    SELECT dc.CUSTOMER_ID,
           COUNT(DISTINCT f.ORDER_ID) AS TOTAL_ORDERS,
           SUM(f.NET_REVENUE)         AS TOTAL_REVENUE,
           SUM(f.GROSS_MARGIN)        AS TOTAL_MARGIN,
           MIN(f.ORDER_DATE)          AS FIRST_ORDER_DATE,
           MAX(f.ORDER_DATE)          AS LAST_ORDER_DATE
    FROM GOLD.FACT_SALES f
    JOIN GOLD.DIM_CUSTOMER dc ON f.CUSTOMER_KEY = dc.CUSTOMER_KEY
    GROUP BY dc.CUSTOMER_ID
),
ret AS (
    SELECT dc.CUSTOMER_ID, COUNT(*) AS TOTAL_RETURNS, SUM(f.REFUND_AMOUNT) AS TOTAL_REFUNDS
    FROM GOLD.FACT_RETURNS f
    JOIN GOLD.DIM_CUSTOMER dc ON f.CUSTOMER_KEY = dc.CUSTOMER_KEY
    GROUP BY dc.CUSTOMER_ID
),
rev AS (
    SELECT dc.CUSTOMER_ID, COUNT(*) AS TOTAL_REVIEWS, AVG(f.RATING) AS AVG_RATING
    FROM GOLD.FACT_REVIEWS f
    JOIN GOLD.DIM_CUSTOMER dc ON f.CUSTOMER_KEY = dc.CUSTOMER_KEY
    GROUP BY dc.CUSTOMER_ID
)
SELECT
    c.CUSTOMER_ID, c.FULL_NAME, c.EMAIL, c.CITY, c.STATE, c.COUNTRY,
    c.AGE_BAND, c.GENDER, c.SIGNUP_DATE,
    COALESCE(s.TOTAL_ORDERS, 0)   AS TOTAL_ORDERS,
    COALESCE(s.TOTAL_REVENUE, 0)  AS TOTAL_REVENUE,
    COALESCE(s.TOTAL_MARGIN, 0)   AS TOTAL_MARGIN,
    s.FIRST_ORDER_DATE, s.LAST_ORDER_DATE,
    COALESCE(r.TOTAL_RETURNS, 0)  AS TOTAL_RETURNS,
    COALESCE(r.TOTAL_REFUNDS, 0)  AS TOTAL_REFUNDS,
    COALESCE(rv.TOTAL_REVIEWS, 0) AS TOTAL_REVIEWS,
    rv.AVG_RATING
FROM GOLD.DIM_CUSTOMER c
LEFT JOIN sales s ON c.CUSTOMER_ID = s.CUSTOMER_ID
LEFT JOIN ret r   ON c.CUSTOMER_ID = r.CUSTOMER_ID
LEFT JOIN rev rv  ON c.CUSTOMER_ID = rv.CUSTOMER_ID
WHERE c.IS_CURRENT = TRUE AND c.CUSTOMER_KEY <> -1;
EOF

ls -la sql/ddl/gold/views/
```

## 8.2 Views builder

```bash
cat > src/transformations/gold_views.py << 'EOF'
"""Builds the Gold semantic views from sql/ddl/gold/views/*.sql."""
from pathlib import Path
from loguru import logger
from src.utils.db import get_snowflake_connection

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "gold" / "views"


def build_gold_views() -> str:
    sql_files = sorted(SQL_DIR.glob("*.sql"))
    sf = get_snowflake_connection(schema="GOLD")
    try:
        for f in sql_files:
            logger.info(f"[views] Building {f.name} ...")
            sf.execute_string(f.read_text(), remove_comments=False)
            logger.success(f"[views] {f.stem} built")
    finally:
        sf.close()
    return f"views_{len(sql_files)}_built"
EOF
```

## 8.3 End-to-end pipeline runner

This chains all five stages, times each, and prints a summary. It's exactly what Airflow will invoke per-stage in Step 9.

```bash
cat > src/run_pipeline.py << 'EOF'
"""
End-to-end pipeline: bronze -> silver -> gold dims -> gold facts -> gold views.
Run with: python -m src.run_pipeline
"""
import sys
import time

from loguru import logger

from src.ingestion.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold_dims import run_gold_dimensions
from src.transformations.gold_facts import run_gold_facts
from src.transformations.gold_views import build_gold_views

STAGES = [
    ("Bronze ingestion",  run_bronze_ingestion),
    ("Silver transforms", run_silver_transformations),
    ("Gold dimensions",   run_gold_dimensions),
    ("Gold facts",        run_gold_facts),
    ("Gold views",        build_gold_views),
]


def run_pipeline(fail_fast: bool = True) -> int:
    overall = time.time()
    results = []
    for name, fn in STAGES:
        logger.info(f"{'#' * 12} STAGE: {name} {'#' * 12}")
        start = time.time()
        try:
            run_id = fn()
            results.append((name, "SUCCESS", run_id, time.time() - start))
            logger.success(f"Stage '{name}' OK in {time.time() - start:.1f}s")
        except Exception as e:
            results.append((name, "FAILED", str(e)[:200], time.time() - start))
            logger.exception(f"Stage '{name}' FAILED")
            if fail_fast:
                logger.error("fail_fast=True -> aborting remaining stages.")
                break

    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    for name, status, info, elapsed in results:
        logger.info(f"  {status:<8} {name:<20} {elapsed:6.1f}s  {info}")
    logger.info(f"  TOTAL ELAPSED: {time.time() - overall:.1f}s")
    logger.info("=" * 60)

    return 1 if any(r[1] == "FAILED" for r in results) else 0


if __name__ == "__main__":
    sys.exit(run_pipeline())
EOF
```

## 8.4 Run the full pipeline

```bash
python -m src.run_pipeline
```

This re-runs everything from source to views. Because every stage is idempotent (bronze full-refresh, silver `CREATE OR REPLACE`, dims hash-compare/MERGE, facts `CREATE OR REPLACE`, views `CREATE OR REPLACE`), running it repeatedly always converges to the same state. End with a summary like:

```
============================================================
PIPELINE SUMMARY
  SUCCESS  Bronze ingestion       12.4s  bronze_20260522_...
  SUCCESS  Silver transforms      18.1s  silver_20260522_...
  SUCCESS  Gold dimensions         9.7s  golddim_20260522_...
  SUCCESS  Gold facts             14.2s  goldfact_20260522_...
  SUCCESS  Gold views              3.1s  views_3_built
  TOTAL ELAPSED: 57.5s
============================================================
```

## 8.5 Verify the views in Snowsight

```sql
USE DATABASE ECOMMERCE_DW;

-- Pipeline observability: latest run per layer
SELECT LAYER, COUNT(*) AS OBJECTS, SUM(ROW_COUNT) AS ROWS,
       SUM(IFF(STATUS='SUCCESS',1,0)) AS OK, SUM(IFF(STATUS='FAILED',1,0)) AS FAILED,
       MAX(ENDED_AT) AS LAST_RUN
FROM GOLD.VW_PIPELINE_RUNS
GROUP BY LAYER
ORDER BY LAST_RUN DESC;

-- Flattened sales: should return 15,000 rows, fully denormalized
SELECT COUNT(*) FROM GOLD.VW_SALES_DETAIL;
SELECT * FROM GOLD.VW_SALES_DETAIL LIMIT 5;

-- Customer 360: one row per current customer (2,000)
SELECT COUNT(*) FROM GOLD.VW_CUSTOMER_360;

-- Top 10 customers by lifetime revenue
SELECT CUSTOMER_ID, FULL_NAME, COUNTRY, TOTAL_ORDERS, TOTAL_REVENUE, TOTAL_RETURNS, AVG_RATING
FROM GOLD.VW_CUSTOMER_360
ORDER BY TOTAL_REVENUE DESC
LIMIT 10;
```

`VW_SALES_DETAIL` should be exactly 15,000 rows (matches `FACT_SALES`), and `VW_CUSTOMER_360` exactly 2,000 (one per current customer, unknown member excluded).

## A note on how Power BI will consume this

You now have two consumption options, and we'll use both in Step 10:

The proper approach for the dashboard is to import the *star* — the five `DIM_*` and six `FACT_*` tables — into Power BI and define the relationships there, then write DAX measures against the model. That's what showcases dimensional-modeling skill and is what we'll build. The flattened views (`VW_SALES_DETAIL`, `VW_CUSTOMER_360`) are useful for quick single-visual pages or validating DAX against a known-good SQL result, but importing fully-flattened views as your *primary* model is an anti-pattern (it bloats the model and loses the star's performance benefits). So: star tables for the model, views as helpers.

## Confirmation checklist before Step 9

1. `python -m src.run_pipeline` completes with all five stages `SUCCESS`.
2. `GOLD.VW_PIPELINE_RUNS` shows BRONZE/SILVER/GOLD layers with their object counts and `FAILED=0`.
3. `VW_SALES_DETAIL` = 15,000 rows; `VW_CUSTOMER_360` = 2,000 rows.
4. The top-10-customers query returns sensible revenue figures.

Paste the pipeline summary block and the `VW_PIPELINE_RUNS` query result. Step 9 is Airflow: we'll containerize it with Docker, mount this project, and build a DAG where each stage (bronze → silver → gold-dims → gold-facts → gold-views) is a task with proper dependencies, so the whole thing runs on a schedule with retries and visible task-level status. Then Step 10 moves to Power BI.

---



## Step 9: Airflow Orchestration (Dockerized)

## What we build

Airflow running in Docker (LocalExecutor — webserver + scheduler + its own metadata Postgres), with your project mounted in so the DAG calls the same `run_*` functions you already wrote. The DAG chains five tasks with real dependencies, retries, and per-task status:

```
bronze_ingestion → silver_transforms → gold_dimensions → gold_facts → gold_views
```

The one genuinely tricky part is networking: Airflow runs inside Docker containers, but your source Postgres lives on the Windows host. We handle that with `host.docker.internal`.

## 9.1 Confirm Docker is available

```bash
docker --version
docker compose version
```

Both should print versions. If `docker` isn't found, you need Docker Desktop (with WSL integration enabled: Settings → Resources → WSL Integration → toggle on for Ubuntu) or Docker Engine installed natively in WSL. Tell me which you have if neither works — the networking setup differs slightly.

## 9.2 Create the Airflow Docker setup

```bash
cd ~/ecommerce-data-platform
mkdir -p docker/airflow/dags docker/airflow/logs

# ---- pip deps the DAG needs inside the Airflow image ----
cat > docker/airflow/requirements-airflow.txt << 'EOF'
snowflake-connector-python==3.10.1
snowflake-sqlalchemy==1.6.1
psycopg2-binary==2.9.9
pandas==2.2.2
pyarrow==16.0.0
python-dotenv==1.0.1
loguru==0.7.2
SQLAlchemy>=1.4.28,<2.0
EOF

# ---- custom Airflow image ----
cat > docker/airflow/Dockerfile << 'EOF'
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
EOF

# ---- compose: LocalExecutor (meta-db + init + webserver + scheduler) ----
cat > docker/airflow/docker-compose.yaml << 'EOF'
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-meta/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__WEBSERVER__SECRET_KEY: 'dev-secret-change-me'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    PYTHONPATH: /opt/airflow/project
    PG_HOST: host.docker.internal
  env_file:
    - ../../.env
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ../../:/opt/airflow/project
  user: "${AIRFLOW_UID:-50000}:0"
  extra_hosts:
    - "host.docker.internal:host-gateway"
  depends_on:
    airflow-meta:
      condition: service_healthy

services:
  airflow-meta:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-meta-vol:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
    restart: always

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create --username admin --password admin \
          --firstname Admin --lastname User --role Admin --email admin@example.com || true
    restart: on-failure

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

volumes:
  airflow-meta-vol:
EOF

# ---- AIRFLOW_UID for correct file permissions on mounted logs ----
echo "AIRFLOW_UID=$(id -u)" > docker/airflow/.env
```

Two `.env` files now exist and they do different jobs: `docker/airflow/.env` holds `AIRFLOW_UID` for Compose variable substitution; your project `../../.env` (referenced by `env_file`) injects the Snowflake + Postgres credentials into the containers. The compose `environment` block overrides `PG_HOST` to `host.docker.internal` so the containers reach Windows Postgres regardless of what your local `.env` says.

## 9.3 The DAG

```bash
cat > docker/airflow/dags/ecommerce_pipeline_dag.py << 'EOF'
"""
Ecommerce medallion pipeline DAG.
Postgres source -> Snowflake BRONZE -> SILVER -> GOLD (dims, facts, views).
Each task calls the same run_* function used by src/run_pipeline.py.
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Project is mounted at /opt/airflow/project (also on PYTHONPATH)
sys.path.insert(0, "/opt/airflow/project")

from src.ingestion.bronze import run_bronze_ingestion
from src.transformations.silver import run_silver_transformations
from src.transformations.gold_dims import run_gold_dimensions
from src.transformations.gold_facts import run_gold_facts
from src.transformations.gold_views import build_gold_views

default_args = {
    "owner": "vinay",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="ecommerce_medallion_pipeline",
    description="Postgres -> Snowflake medallion (bronze/silver/gold)",
    default_args=default_args,
    schedule_interval="0 2 * * *",          # daily at 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "medallion", "snowflake"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )
    silver = PythonOperator(
        task_id="silver_transforms",
        python_callable=run_silver_transformations,
    )
    gold_dims = PythonOperator(
        task_id="gold_dimensions",
        python_callable=run_gold_dimensions,
    )
    gold_facts = PythonOperator(
        task_id="gold_facts",
        python_callable=run_gold_facts,
    )
    gold_views = PythonOperator(
        task_id="gold_views",
        python_callable=build_gold_views,
    )

    bronze >> silver >> gold_dims >> gold_facts >> gold_views
EOF
```

## 9.4 Build and start

```bash
cd ~/ecommerce-data-platform/docker/airflow

# Build the custom image (first time pulls Airflow + installs deps — a few minutes)
docker compose build

# Initialize the metadata DB and create the admin user
docker compose up airflow-init

# Start webserver + scheduler in the background
docker compose up -d

# Watch them come healthy
docker compose ps
```

Wait until `airflow-webserver` shows `healthy` (can take ~60s). Then open http://localhost:8080 in your browser and log in with `admin` / `admin`.

## 9.5 Connectivity check from inside the container

Before triggering the DAG, confirm the containers can reach both databases — this is where most setups stumble:

```bash
# Snowflake (internet — should just work)
docker compose exec airflow-scheduler python -c "
from src.utils.db import get_snowflake_connection
c = get_snowflake_connection(); cur = c.cursor()
cur.execute('SELECT CURRENT_ROLE(), CURRENT_DATABASE()')
print('Snowflake OK:', cur.fetchone()); c.close()
"

# Postgres (via host.docker.internal -> Windows host)
docker compose exec airflow-scheduler python -c "
from sqlalchemy import text
from src.utils.db import get_postgres_engine
e = get_postgres_engine()
with e.connect() as c:
    print('Postgres OK:', c.execute(text('SELECT COUNT(*) FROM customers')).scalar(), 'customers')
"
```

Both should print `OK`. If Snowflake works but Postgres fails with "connection refused," see troubleshooting below — it's a host-networking issue, not a code issue.

## 9.6 Trigger the DAG

In the Airflow UI: find `ecommerce_medallion_pipeline`, toggle it **on** (the switch on the left), then click the **▶ play button** → "Trigger DAG." Watch the Grid view — five tasks should go green in sequence.

Or trigger from the CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_medallion_pipeline
```

Each task's logs are viewable in the UI (click a task square → Logs) — you'll see the same loguru output you saw locally. After it completes, confirm in Snowsight that `GOLD.VW_PIPELINE_RUNS` has fresh timestamps from the Airflow run.

## 9.7 Troubleshooting

The networking ones are most common:

- **Postgres "connection refused" from the container, but works from WSL directly.** `host.docker.internal` isn't resolving to the Windows host. On Docker Desktop it should work out of the box (the `extra_hosts: host-gateway` line is the safety net for native Docker). Test resolution: `docker compose exec airflow-scheduler getent hosts host.docker.internal`. If it returns nothing or the wrong IP, override `PG_HOST` in the compose `environment` block with your actual Windows host IP from `ip route show | grep default | awk '{print $3}'` (run on the WSL side), then `docker compose up -d` to recreate.

- **`ModuleNotFoundError: No module named 'src'` in a task.** The project mount or `PYTHONPATH` isn't taking. Confirm: `docker compose exec airflow-scheduler ls /opt/airflow/project/src` — you should see `ingestion`, `transformations`, `utils`. The DAG's `sys.path.insert` is the backup.

- **DAG not appearing in the UI.** Parsing error. Check: `docker compose exec airflow-scheduler airflow dags list-import-errors`. Usually a missing dependency in the image — rebuild after editing `requirements-airflow.txt`: `docker compose build && docker compose up -d`.

- **Permission errors writing logs.** The `AIRFLOW_UID` didn't get set. Re-run `echo "AIRFLOW_UID=$(id -u)" > docker/airflow/.env`, then `docker compose down && docker compose up -d`.

- **Snowflake auth fails inside container but works locally.** The `env_file: ../../.env` path is relative to the compose file (`docker/airflow/`), so `../../.env` points at the project root `.env`. Confirm it's being read: `docker compose exec airflow-scheduler printenv | grep SNOWFLAKE_USER`.

## 9.8 Useful day-to-day commands

```bash
docker compose logs -f airflow-scheduler      # tail scheduler logs
docker compose restart airflow-scheduler      # after changing a DAG's imports
docker compose down                           # stop (keeps metadata volume)
docker compose down -v                        # stop + wipe Airflow metadata
docker compose up -d                          # start again
```

Editing the DAG file itself doesn't need a restart — the scheduler re-parses `dags/` automatically every ~30s. Only rebuild (`docker compose build`) when you change `requirements-airflow.txt` or the Dockerfile.

## Confirmation checklist before Step 10

1. `docker compose ps` shows `airflow-webserver` healthy and `airflow-scheduler` running.
2. http://localhost:8080 loads and you can log in (`admin`/`admin`).
3. Both connectivity checks in 9.5 print `OK`.
4. `ecommerce_medallion_pipeline` runs end-to-end with all five tasks green.
5. `GOLD.VW_PIPELINE_RUNS` in Snowsight shows fresh run timestamps from the Airflow-triggered run.

Paste a note on whether the DAG ran green (and the `docker compose ps` output if anything's stuck). Step 10 moves to Power BI: installing Power BI Desktop, connecting to Snowflake, importing the star (the five `DIM_*` + six `FACT_*` tables), and setting up the relationships and the date table — the foundation before we write DAX in Step 11.

---



## Step 10: Power BI — Connect, Import the Star, Build the Model

This is where the warehouse becomes a dashboard. Step 10 is connection + modeling (no DAX yet — that's Step 11). The goal is a clean star-schema model in Power BI with correct relationships, which is what makes the DAX in Step 11 simple and fast.

## 10.1 Install Power BI Desktop

Power BI Desktop is **Windows-only** (it won't run in WSL). Install it on Windows, not in your Ubuntu environment.

Easiest route — Microsoft Store: open the Store on Windows, search "Power BI Desktop," install. It auto-updates this way. Alternatively download from https://www.microsoft.com/power-platform/products/power-bi/desktop. It's free; no license needed for building and viewing locally.

Once installed, launch it. Close the splash/sign-in nag if it appears (you can use it without signing in for local work).

## 10.2 Connect to Snowflake

1. In Power BI Desktop: **Home → Get Data → More…**
2. Search "Snowflake," select **Snowflake**, click **Connect**.
3. Fill in:
   - **Server**: your full Snowflake account URL — `<account_locator>.snowflakecomputing.com`. From your earlier connection that's `zyaxunb-yi82392.snowflakecomputing.com` (the ORG-account form works). If that gives trouble, use the locator form `UW40253.ap-southeast-7.aws.snowflakecomputing.com`.
   - **Warehouse**: `WH_ECOM_XS`
4. Leave the rest default. Click **OK**.
5. **Data Connectivity mode**: choose **Import** (not DirectQuery). Import pulls the data into Power BI's in-memory engine — for 15K-row facts this is instant and gives you the fastest DAX. (DirectQuery is for huge tables that can't fit in memory; not your case.)
6. **Authentication**: select **Snowflake** (username/password). Enter:
   - Username: `ECOM_PIPELINE`
   - Password: your `ECOM_PIPELINE` password
   - Click **Connect**.

A note on the role: by default Power BI connects with the user's *default* role, which for `ECOM_PIPELINE` is `ECOM_ENGINEER` — correct, it can read everything in `GOLD`. If you ever need to force a role, you can add it under the advanced connection options, but you won't need to here.

## 10.3 Select the tables (import the star, not the views)

The Navigator opens showing your databases. Expand **ECOMMERCE_DW → GOLD**. Check exactly these eleven tables — the five dimensions and six facts:

Dimensions: `DIM_DATE`, `DIM_CUSTOMER`, `DIM_PRODUCT`, `DIM_CAMPAIGN`, `DIM_COUPON`.
Facts: `FACT_SALES`, `FACT_PAYMENTS`, `FACT_SHIPMENTS`, `FACT_RETURNS`, `FACT_REVIEWS`, `FACT_WEB_SESSIONS`.

Do **not** import `VW_SALES_DETAIL` or `VW_CUSTOMER_360` into this model — importing flattened views alongside the star creates redundant data and confuses the relationships. (Those views are for ad-hoc analysis or validation, as we discussed in Step 8.) Skip the `_*_RUN_LOG` and `STG_*` objects too.

Click **Load**. Power BI imports all eleven tables. For your volumes this takes seconds.

## 10.4 Build the relationships

Switch to **Model view** (the diagram icon on the far left toolbar). Power BI may auto-detect some relationships from matching column names — but it often guesses wrong or misses the SCD2 surrogate keys, so we set them explicitly. Delete any auto-created relationships you didn't intend (right-click the line → Delete), then create these by **dragging the dimension key onto the fact key**:

Each relationship is dimension `_KEY` (the "one" side) → fact `_KEY` (the "many" side), single-direction filter (dimension filters fact):

| Dimension.key | → | Fact.key |
|---|---|---|
| `DIM_DATE[DATE_KEY]` | → | `FACT_SALES[DATE_KEY]` |
| `DIM_CUSTOMER[CUSTOMER_KEY]` | → | `FACT_SALES[CUSTOMER_KEY]` |
| `DIM_PRODUCT[PRODUCT_KEY]` | → | `FACT_SALES[PRODUCT_KEY]` |
| `DIM_CAMPAIGN[CAMPAIGN_KEY]` | → | `FACT_SALES[CAMPAIGN_KEY]` |
| `DIM_COUPON[COUPON_KEY]` | → | `FACT_SALES[COUPON_KEY]` |

Then repeat the date + customer (+ product where applicable) joins for the other facts:

- `DIM_DATE[DATE_KEY]` → `FACT_PAYMENTS[DATE_KEY]`, and `DIM_CUSTOMER[CUSTOMER_KEY]` → `FACT_PAYMENTS[CUSTOMER_KEY]`
- `DIM_DATE[DATE_KEY]` → `FACT_SHIPMENTS[DATE_KEY]`, `DIM_CUSTOMER` → `FACT_SHIPMENTS`
- `DIM_DATE`, `DIM_CUSTOMER`, `DIM_PRODUCT` → `FACT_RETURNS`
- `DIM_DATE`, `DIM_CUSTOMER`, `DIM_PRODUCT` → `FACT_REVIEWS`
- `DIM_DATE`, `DIM_CUSTOMER`, `DIM_CAMPAIGN` → `FACT_WEB_SESSIONS`

For each relationship, when the dialog opens, confirm: **Cardinality = Many-to-one (∗:1)** with the fact on the many side, **Cross-filter direction = Single**, **Make this relationship active = checked**.

One subtlety on multiple date relationships: a fact can have only **one active** relationship to `DIM_DATE` at a time. `FACT_SALES[DATE_KEY]` is your active one (order date). That's fine for now — we use one date per fact. If later you want to analyze, say, shipments by both shipped-date and delivery-date, you'd add `DIM_DATE` relationships to those extra date keys as *inactive* and activate them in specific measures with `USERELATIONSHIP`. Not needed yet; flagging it so the concept's on your radar for Step 11.

## 10.5 Mark the date table

This is a step people skip and then wonder why time-intelligence DAX (YTD, prior-year, etc.) misbehaves. Tell Power BI that `DIM_DATE` is *the* date table:

1. In **Model view** or **Data view**, select the `DIM_DATE` table.
2. **Table tools** ribbon → **Mark as date table** → **Mark as date table**.
3. In the dialog, set **Date column = `FULL_DATE`**. Click OK.

One catch: `DIM_DATE` has the `-1` unknown-member row where `FULL_DATE` is `NULL`. Power BI's "mark as date table" requires a contiguous, gap-free, non-null set of dates. The `NULL` row will trip it. Two clean options:

- Simplest: in Power Query (Home → Transform data), filter `DIM_DATE` to remove the row where `FULL_DATE` is null — but that drops the unknown member, which facts with `DATE_KEY = -1` point to. Since every fact resolved to a real date (0% DATE_KEY orphans, from your Step 7 verify), no fact actually uses the `-1` date row, so removing it from the model is safe here.
- Do this in Power Query: select `DIM_DATE` → filter `FULL_DATE` → uncheck `(null)` → Close & Apply. Then mark as date table.

## 10.6 Tidy the model (quick wins)

A few things that make the model professional and the DAX cleaner:

- **Hide surrogate keys from report view.** In Model view, for each `_KEY` column on both dims and facts, right-click → **Hide in report view**. They're needed for relationships but shouldn't clutter the field list when building visuals.
- **Hide the audit columns** (`_LOADED_AT`, `_INGESTION_ID`, `_SOURCE_SYSTEM`, `_PROCESSED_AT`, `_*_RUN_ID`, `ROW_HASH`, `VALID_FROM`, `VALID_TO`, `IS_CURRENT`) on the dimensions — useful in the warehouse, noise in the report.
- **Set data categories** for geography: select `DIM_CUSTOMER[COUNTRY]` → Column tools → Data category → **Country/Region**; `[CITY]` → **City**; `[STATE]` → **State or Province**. This enables map visuals later.
- **Format the measures' source columns**: select revenue/cost columns → Column tools → Format → Currency, and set decimal places to 2.

## Confirmation checklist before Step 11

1. Power BI Desktop installed on Windows, connected to Snowflake in **Import** mode.
2. All eleven star tables loaded (5 dims + 6 facts); views and log/staging tables excluded.
3. Relationships created — every fact joined to `DIM_DATE` and `DIM_CUSTOMER` (plus `DIM_PRODUCT`/`DIM_CAMPAIGN`/`DIM_COUPON` where the bus matrix says so), all Many-to-one, Single-direction.
4. `DIM_DATE` marked as date table on `FULL_DATE` (after removing the null `-1` row).
5. Surrogate/audit columns hidden; geography columns categorized.

This step is all in the Power BI GUI, so there's nothing to paste — instead, tell me:

- Did the Snowflake connection succeed and all eleven tables load?
- Does the Model view show the star shape — `FACT_SALES` in the middle with five dimensions radiating out, and the other facts each linked to their dims?

Once you confirm the model's in place, Step 11 is the fun part: DAX measures — a proper measure table with revenue, AOV, margin %, return rate, customer LTV, month-over-month and year-over-year growth, running totals, and a few cohort/retention measures. Then Step 12 assembles the dashboard pages.

---



## Step 11: DAX Measures

## 11.0 Required prerequisite — mark the date table

Time-intelligence DAX (YoY, YTD, MoM) will silently misbehave if `DIM_DATE` isn't marked. Confirm it's done:

1. Select `DIM_DATE` → **Table tools** ribbon → **Mark as date table** → set **Date column = `FULL_DATE`**.
2. If it rejects because of the null `-1` row: Home → **Transform data** → select `DIM_DATE` → filter `FULL_DATE` → uncheck `(null)` → **Close & Apply**, then mark as date table. (Safe — your facts had 0% `DATE_KEY` orphans, so nothing uses the `-1` date.)

## 11.1 Create a measures home table

Best practice is to keep all measures in one dedicated table so they're easy to find. Home → **Enter data** → leave it empty → name it `_Measures` → **Load**. (The leading underscore sorts it to the top of the field list.) After you add your first measure to it, you can delete the auto-created `Column1`.

To add each measure below: select the `_Measures` table → **Table tools → New measure** → paste the DAX → Enter. Repeat for each.

## 11.2 Core sales measures

```dax
Total Revenue = SUM(FACT_SALES[NET_REVENUE])

Gross Revenue = SUM(FACT_SALES[GROSS_REVENUE])

Total Discount = SUM(FACT_SALES[DISCOUNT_AMT])

Total Tax = SUM(FACT_SALES[TAX_AMT])

Total Cost = SUM(FACT_SALES[COST_AMT])

Gross Margin = SUM(FACT_SALES[GROSS_MARGIN])

Gross Margin % = DIVIDE([Gross Margin], [Total Revenue])

Total Quantity = SUM(FACT_SALES[QUANTITY])

Total Orders = DISTINCTCOUNT(FACT_SALES[ORDER_ID])

Order Lines = COUNTROWS(FACT_SALES)

Avg Order Value = DIVIDE([Total Revenue], [Total Orders])

Avg Items per Order = DIVIDE([Total Quantity], [Total Orders])
```

`DIVIDE` (rather than `/`) is deliberate everywhere — it returns blank instead of erroring on divide-by-zero, which keeps visuals clean when a filter context has no denominator.

## 11.3 Customer measures

```dax
Active Customers = DISTINCTCOUNT(FACT_SALES[CUSTOMER_KEY])

Revenue per Customer = DIVIDE([Total Revenue], [Active Customers])
```

A note on `Active Customers`: it counts distinct surrogate keys. Today that equals distinct customers because your `DIM_CUSTOMER` has no historical versions (you rebuilt it, `historical=0`). But if SCD2 history accumulates, one customer with two address versions would have two surrogate keys and get double-counted. The SCD2-robust version, which you can swap in later, counts the business key with a temporary bidirectional filter:

```dax
Active Customers (SCD2-safe) =
CALCULATE(
    DISTINCTCOUNT(DIM_CUSTOMER[CUSTOMER_ID]),
    CROSSFILTER(FACT_SALES[CUSTOMER_KEY], DIM_CUSTOMER[CUSTOMER_KEY], BOTH)
)
```

For the dashboard, the simple version is fine and faster; the second one is a good thing to be able to explain in an interview.

## 11.4 Operational measures (returns, reviews, shipping, web, payments)

```dax
Total Returns = COUNTROWS(FACT_RETURNS)

Total Refunds = SUM(FACT_RETURNS[REFUND_AMOUNT])

Return Rate % = DIVIDE([Total Returns], [Order Lines])

Refund Ratio % = DIVIDE([Total Refunds], [Total Revenue])

Avg Rating = AVERAGE(FACT_REVIEWS[RATING])

Total Reviews = COUNTROWS(FACT_REVIEWS)

Total Sessions = COUNTROWS(FACT_WEB_SESSIONS)

Avg Session Minutes = DIVIDE(SUM(FACT_WEB_SESSIONS[SESSION_DURATION_SEC]), [Total Sessions]) / 60

Conversion Rate % = DIVIDE([Total Orders], [Total Sessions])

Total Payments = SUM(FACT_PAYMENTS[AMOUNT])

Avg Shipping Cost = AVERAGE(FACT_SHIPMENTS[SHIPPING_COST])

Avg Days to Deliver = AVERAGE(FACT_SHIPMENTS[DAYS_TO_DELIVER])
```

`Conversion Rate %` is a coarse total-orders-over-total-sessions ratio — fine as a headline KPI, but note it's not session-level attributed conversion (that'd need a session→order link your source doesn't model).

## 11.5 Time-intelligence measures

These require the date table to be marked (11.0).

```dax
Revenue PY = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DIM_DATE[FULL_DATE]))

Revenue YoY % = DIVIDE([Total Revenue] - [Revenue PY], [Revenue PY])

Revenue YTD = TOTALYTD([Total Revenue], DIM_DATE[FULL_DATE])

Revenue Prior Month = CALCULATE([Total Revenue], DATEADD(DIM_DATE[FULL_DATE], -1, MONTH))

Revenue MoM % = DIVIDE([Total Revenue] - [Revenue Prior Month], [Revenue Prior Month])

Revenue Running Total =
CALCULATE(
    [Total Revenue],
    FILTER(
        ALLSELECTED(DIM_DATE[FULL_DATE]),
        DIM_DATE[FULL_DATE] <= MAX(DIM_DATE[FULL_DATE])
    )
)
```

## 11.6 Format the measures

Select each measure → **Measure tools** ribbon → set Format:

- Currency ones (`Total Revenue`, `Gross Margin`, `Avg Order Value`, `Total Refunds`, etc.) → Currency, 2 decimals (or 0 for big headline cards).
- Percentage ones (`Gross Margin %`, `Return Rate %`, `Revenue YoY %`, etc.) → Percentage, 1–2 decimals.
- Counts (`Total Orders`, `Active Customers`) → Whole number, with thousands separator.

This is what makes a card show `$1,234,567` and `12.4%` instead of raw decimals.

## 11.7 Validate against known SQL

Drop a few **Card** visuals on a blank page (Report view → Card visual → drag a measure in) and cross-check against Snowflake so you trust the model. Run in Snowsight:

```sql
SELECT
    SUM(NET_REVENUE)              AS TOTAL_REVENUE,
    SUM(GROSS_MARGIN)             AS GROSS_MARGIN,
    COUNT(DISTINCT ORDER_ID)      AS TOTAL_ORDERS,
    SUM(QUANTITY)                 AS TOTAL_QTY
FROM GOLD.FACT_SALES;
```

`Total Revenue`, `Gross Margin`, `Total Orders`, `Total Quantity` cards must match these numbers exactly. If they do, your model and DAX are sound and every downstream visual will be trustworthy. If a number's off, tell me which and we'll trace it (usually a relationship direction or a wrong source column).

## Confirmation checklist before Step 12

1. `DIM_DATE` marked as date table on `FULL_DATE`.
2. `_Measures` table created; all measures from 11.2–11.5 added without DAX errors.
3. Measures formatted (currency / percentage / whole number).
4. The four validation cards match the SQL numbers exactly.

Tell me whether the validation cards matched the SQL (and paste any DAX error if a measure won't commit). Step 12 is the payoff — assembling the dashboard pages: an executive overview (KPI cards, revenue trend with YoY, top products/categories), a customer page (geography map, age/gender breakdowns, LTV), an operations page (returns, shipping, reviews), and a marketing page (campaigns, channels, conversion) — with slicers, drill-through, and consistent theming so it's recruiter-ready.

---



## Step 12: Dashboard Pages

Four pages, built for recruiter impact: an executive overview, customer analytics, operations, and marketing. I'll give you the layout, exact visuals, and field mappings for each. This is all GUI work in Report view, so I'll be precise about what goes where.

First, two setup pieces that make everything look professional.

## 12.0 Theme and shared slicers

**Apply a theme:** View ribbon → Themes → pick a clean one (Executive or a built-in muted palette), or **Browse for themes** if you download a custom JSON later. A consistent palette across pages is what separates "polished" from "default Power BI blue."

**Page setup:** for each page, Format pane (with nothing selected) → Canvas settings → keep 16:9. Give each page a title text box at top-left (Insert → Text box).

## 12.1 Page 1 — Executive Overview

The page a hiring manager looks at first. Layout top-to-bottom:

**Row 1 — KPI cards** (Insert these as Card visuals across the top):
`Total Revenue`, `Gross Margin %`, `Total Orders`, `Avg Order Value`, `Active Customers`, `Revenue YoY %`. Six cards in a row.

**Row 2 — Revenue trend with prior year:**
A **Line chart**. X-axis = `DIM_DATE[YEAR_MONTH]` (or drag `FULL_DATE` and drill to month). Y-axis (Values) = `Total Revenue` and `Revenue PY`. This shows current vs prior-year revenue over time — instantly readable growth story.

**Row 3 — split into three:**
- **Top products** — Bar chart (clustered). Y-axis = `DIM_PRODUCT[PRODUCT_NAME]`, X = `Total Revenue`, then in the Filter pane apply a Top-N filter (Top 10 by `Total Revenue`).
- **Revenue by category** — Donut or Treemap. Legend/Category = `DIM_PRODUCT[CATEGORY_NAME]`, Values = `Total Revenue`.
- **Revenue by country** — Map. Location = `DIM_CUSTOMER[COUNTRY]` (you set its data category to Country/Region in 10.6, so it'll plot), Bubble size = `Total Revenue`.

**Slicers** (top-right or a left rail): `DIM_DATE[YEAR]` and `DIM_PRODUCT[CATEGORY_NAME]`. Use the slicer visual; for Year, set it to a dropdown or tile style.

## 12.2 Page 2 — Customer Analytics

**Row 1 — cards:** `Active Customers`, `Revenue per Customer`, `Avg Order Value`, `Avg Items per Order`.

**Row 2:**
- **Revenue by age band** — Column chart. Axis = `DIM_CUSTOMER[AGE_BAND]`, Values = `Total Revenue`. (Sort the axis so bands read 18-24 → 65+; if it sorts alphabetically, that order already works.)
- **Revenue by gender** — Donut. Legend = `DIM_CUSTOMER[GENDER]`, Values = `Total Revenue`.

**Row 3 — Top customers table:**
A **Table** visual. Columns: `DIM_CUSTOMER[FULL_NAME]`, `DIM_CUSTOMER[COUNTRY]`, `Total Orders`, `Total Revenue`, `Revenue per Customer`, `Avg Rating`. Sort descending by `Total Revenue`. This is your "who are our best customers" view.

**Map:** a filled or bubble map by `DIM_CUSTOMER[STATE]` or `[CITY]` with `Active Customers` as size — geographic customer distribution.

**Slicers:** `DIM_CUSTOMER[COUNTRY]`, `DIM_CUSTOMER[AGE_BAND]`.

## 12.3 Page 3 — Operations (Returns, Shipping, Reviews)

**Row 1 — cards:** `Return Rate %`, `Total Refunds`, `Avg Days to Deliver`, `Avg Rating`, `Avg Shipping Cost`.

**Row 2:**
- **Return rate by category** — Bar chart. Axis = `DIM_PRODUCT[CATEGORY_NAME]`, Values = `Return Rate %`. Highlights which categories get returned most.
- **Returns reason breakdown** — Donut or bar. Category = `FACT_RETURNS[REASON]`, Values = `Total Returns`.

**Row 3:**
- **Delivery performance over time** — Line chart. X = `DIM_DATE[YEAR_MONTH]`, Y = `Avg Days to Deliver`. Trend of shipping speed.
- **Rating distribution** — Column chart. Axis = `FACT_REVIEWS[RATING]` (1–5), Values = `Total Reviews`. Or `Avg Rating` by `DIM_PRODUCT[CATEGORY_NAME]`.

**Shipping by carrier** — Table or bar: `FACT_SHIPMENTS[CARRIER]`, `Avg Days to Deliver`, `Avg Shipping Cost`, count of shipments.

## 12.4 Page 4 — Marketing & Web

**Row 1 — cards:** `Total Sessions`, `Conversion Rate %`, `Avg Session Minutes`, `Total Orders`.

**Row 2:**
- **Sessions by source/medium** — Bar chart. Axis = `FACT_WEB_SESSIONS[SOURCE]` (or `MEDIUM`), Values = `Total Sessions`.
- **Sessions by device** — Donut. Legend = `FACT_WEB_SESSIONS[DEVICE]`, Values = `Total Sessions`.

**Row 3 — Campaign performance:**
A **Table**: `DIM_CAMPAIGN[CAMPAIGN_NAME]`, `DIM_CAMPAIGN[CHANNEL]`, `DIM_CAMPAIGN[BUDGET]`, `Total Sessions`, `Total Revenue` (revenue attributed to campaign via `FACT_SALES[CAMPAIGN_KEY]`), and a quick ROI-style ratio if you want (`Total Revenue` / budget). This ties marketing spend to revenue.

**Channel revenue** — Column: `DIM_CAMPAIGN[CHANNEL]`, `Total Revenue`.

## 12.5 Interactivity that impresses

A few touches that signal you know Power BI beyond dragging fields:

- **Drill-through to customer detail.** Create a hidden page "Customer Detail" with a `DIM_CUSTOMER[CUSTOMER_ID]` drill-through filter (Format pane → Add to drill-through). Then right-clicking any customer on Page 2 → Drill through → shows their full profile (orders, returns, reviews timeline). This is a standout feature.
- **Cross-filtering** is on by default — clicking a category bar filters the whole page. Verify it feels right; if a visual shouldn't filter others, Format → Edit interactions.
- **Sync slicers across pages.** Select a slicer → View → Sync slicers → tick the pages you want it to apply to. So a Year filter persists as the viewer moves between pages.
- **Tooltips.** Hover tooltips work automatically; for richer ones, build a tooltip page (small canvas, set as Tooltip type) showing mini-trends.
- **Buttons for navigation.** Insert → Buttons → page navigation, so the report feels like an app with a nav bar.

## 12.6 Save and (optionally) publish

Save the `.pbix` to your project: `~/ecommerce-data-platform/powerbi/` — though note from WSL you'd save to the Windows path. Easiest: save it on Windows, then for the repo, copy it into `\\wsl.localhost\Ubuntu\home\vinaygautam\ecommerce-data-platform\powerbi\` via Explorer, or just keep a copy there.

If you want it shareable online (great for a portfolio link): **Home → Publish** to the Power BI Service (free account). You can then share a link or embed screenshots. Note the Service needs a scheduled refresh + a gateway to reach Snowflake for live data — for a portfolio, publishing a snapshot is usually enough, and screenshots in your README work just as well.

## Confirmation checklist before Step 13

1. Four pages built with the cards and visuals above (adapt freely — these are a strong starting set, not a straitjacket).
2. Slicers work and cross-filtering feels right.
3. At least one advanced touch (drill-through, synced slicers, or nav buttons).
4. `.pbix` saved.

Build the pages at your own pace — there's a lot of visual assembly here, so don't rush it. Tell me how it's going, or ask about any specific visual that's not behaving (Top-N filters, the map not plotting, drill-through setup, sorting an axis — these are the usual snags).

Step 13 — the final one — is portfolio packaging: reintroducing git, a strong README (architecture diagram, the bus matrix, screenshots, the SCD2 explanation, setup instructions), the `.gitignore` so your `.env` never leaks, and framing the whole thing for recruiters. That's what turns a working project into something that gets you interviews. Reply when you're ready for it, or with any Step 12 questions.

---


## Step 13: Portfolio Packaging

Turn the working project into a recruiter-ready GitHub repository — clean documentation, architecture diagrams, dashboard screenshots, and no secrets.

The single most important ordering rule: **`.gitignore` with `.env` in it must exist before the first `git add`**, or your Snowflake and Postgres credentials get baked into git history permanently.

### 13.1 Create `.gitignore` first (CRITICAL — before `git init`)

```bash
cd ~/ecommerce-data-platform

cat > .gitignore << 'EOF'
# Secrets — never commit
.env
docker/airflow/.env

# Python
.venv/
__pycache__/
*.pyc
*.pyo

# Airflow runtime
docker/airflow/logs/
airflow/logs/

# Power BI (can embed data snapshot + connection details)
*.pbix

# OS / IDE
.DS_Store
.idea/
.vscode/
*.swp
EOF
```

### 13.2 Initialize git and stage everything

```bash
git init
git branch -M main
git add .
git status
```

### 13.3 Verify no secrets are staged

Before committing, confirm `.env` is NOT in the staged list:

```bash
git status --short | grep -i "\.env$"
```

This should return **nothing** (real `.env` is ignored). You should see `.env.example` staged — that's the safe template. Double-check it has no real credentials:

```bash
cat .env.example
```

All values to the right of `=` should be blank or placeholders.

### 13.4 First commit

```bash
git commit -m "Initial commit: end-to-end ecommerce data platform"
```

### 13.5 Write the README (`README.md`)

Create the README — this is what recruiters actually read. GitHub renders Mermaid natively, so architecture and ER diagrams render as real diagrams with no image files needed.

````markdown
# Ecommerce Data Platform — Modern Data Stack (Postgres → Snowflake → Power BI)

An end-to-end analytics engineering project: a production-style pipeline that
ingests an ecommerce OLTP database, transforms it through a medallion architecture
into a Kimball dimensional model with SCD Type 2 history, orchestrates the flow
with Airflow, and surfaces insights in a Power BI dashboard.

## Architecture

```mermaid
flowchart LR
    PG[("PostgreSQL<br/>OLTP source<br/>14 tables")]
    B["BRONZE<br/>raw mirror<br/>+ audit cols"]
    S["SILVER<br/>cleansed,<br/>typed, deduped"]
    G["GOLD<br/>Kimball star<br/>5 dims · 6 facts"]
    PBI["Power BI<br/>dashboards<br/>+ DAX"]

    PG -->|"Python EL"| B
    B -->|"SQL transforms"| S
    S -->|"dims + facts"| G
    G -->|"Import + DAX"| PBI

    AF{{"Airflow<br/>(Dockerized)"}}
    AF -.->|orchestrates| B
    AF -.-> S
    AF -.-> G
```

The pipeline runs as a single Airflow DAG:
`bronze_ingestion → silver_transforms → gold_dimensions → gold_facts → gold_views`

## Tech stack

| Layer | Technology |
|---|---|
| Source | PostgreSQL (OLTP) |
| Ingestion | Python, pandas, SQLAlchemy, snowflake-connector |
| Warehouse | Snowflake (Bronze / Silver / Gold schemas) |
| Transformation | SQL (medallion), SCD Type 2 logic |
| Orchestration | Apache Airflow (Docker, LocalExecutor) |
| BI | Power BI Desktop (Import mode, DAX) |
| Environment | WSL2 / Ubuntu, Docker |

## Dimensional model

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_SALES : "order date"
    DIM_CUSTOMER ||--o{ FACT_SALES : "placed by"
    DIM_PRODUCT ||--o{ FACT_SALES : "line item"
    DIM_CAMPAIGN ||--o{ FACT_SALES : "attributed to"
    DIM_COUPON ||--o{ FACT_SALES : "discounted by"
    FACT_SALES {
        int SALES_KEY PK
        int DATE_KEY FK
        int CUSTOMER_KEY FK
        int PRODUCT_KEY FK
        number NET_REVENUE
        number GROSS_MARGIN
    }
    DIM_CUSTOMER {
        int CUSTOMER_KEY PK
        int CUSTOMER_ID "business key"
        string CITY
        timestamp VALID_FROM "SCD2"
        timestamp VALID_TO "SCD2"
        boolean IS_CURRENT "SCD2"
    }
    DIM_PRODUCT {
        int PRODUCT_KEY PK
        int PRODUCT_ID "business key"
        number UNIT_PRICE
        number COST
        timestamp VALID_FROM "SCD2"
        boolean IS_CURRENT "SCD2"
    }
```

### Bus matrix

| Fact (grain)              | DIM_DATE | DIM_CUSTOMER | DIM_PRODUCT | DIM_CAMPAIGN | DIM_COUPON |
|---------------------------|:---:|:---:|:---:|:---:|:---:|
| FACT_SALES (order line)   | ✓ | ✓ | ✓ | ✓ | ✓ |
| FACT_PAYMENTS (payment)   | ✓ | ✓ | — | — | — |
| FACT_SHIPMENTS (shipment) | ✓ | ✓ | — | — | — |
| FACT_RETURNS (line)       | ✓ | ✓ | ✓ | — | — |
| FACT_REVIEWS (review)     | ✓ | ✓ | ✓ | — | — |
| FACT_WEB_SESSIONS         | ✓ | ✓* | — | ✓ | — |

\* nullable — guest sessions route to the unknown-member row.

## Technical highlights

- **SCD Type 2 dimensions** (DIM_CUSTOMER, DIM_PRODUCT) with surrogate keys,
  effective-dating (VALID_FROM/VALID_TO/IS_CURRENT), and MD5 row-hash change
  detection. Facts join to the dimension version that was current at the
  transaction date (point-in-time join), so historical margin stays correct
  even after a product is repriced.
- **Medallion architecture** with full lineage: every row carries audit
  columns; every pipeline run is logged (see `GOLD.VW_PIPELINE_RUNS`).
- **Idempotent, re-runnable** at every stage — full-refresh Bronze,
  hash-compared Type 2 dims, MERGE-upsert Type 1 dims.
- **Role-based access control** in Snowflake: a least-privilege service role
  (ECOM_ENGINEER) owns all objects; ACCOUNTADMIN is reserved for account ops.
- **Data quality**: business-rule validation in Silver, orphan-key routing
  to unknown members (0% orphans on required keys), reconciliation scripts
  at each layer boundary.

## Running the pipeline

```bash
# 1. Environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in Postgres + Snowflake credentials

# 2. Run end-to-end locally
python -m src.run_pipeline

# 3. Or orchestrate with Airflow
cd docker/airflow
docker compose build
docker compose up airflow-init
docker compose up -d           # UI at http://localhost:8080
```

## Dashboard

See `powerbi/` for screenshots. The report covers executive overview
(revenue trend, YoY, top products), customer analytics (LTV, geography,
segments), operations (returns, shipping, reviews), and marketing
(campaigns, channels, conversion).

## Possible extensions

- Incremental ingestion (CDC / high-water-mark) instead of full refresh
- dbt for transformation management and tests
- CI/CD with GitHub Actions
- Great Expectations / dbt tests for automated data quality gates
````

### 13.6 Add MIT `LICENSE`

```bash
cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2026 Vinaykumar B V

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF
```

### 13.7 Dashboard screenshots

In Power BI Desktop, for each page: **File → Export → Export to PDF** (all pages), or screenshot each page (Windows `Win+Shift+S`). Save PNGs into `powerbi/`:

```bash
mkdir -p ~/ecommerce-data-platform/powerbi
```

Copy the PNGs in (from Windows, use `\\wsl.localhost\Ubuntu\home\<user>\ecommerce-data-platform\powerbi\`). Name them clearly: `01_executive_overview.png`, `02_customer_analytics.png`, `03_operations.png`, `04_marketing_web.png`.

Embed the headline one near the top of the README (edit `README.md`):

```markdown
![Executive Dashboard](powerbi/01_executive_overview.png)
```

### 13.8 Commit and push to GitHub

```bash
cd ~/ecommerce-data-platform
git add .
git commit -m "Add README, license, dashboard screenshots"
```

Create the repo on GitHub: **New repository** → name it `ecommerce-data-platform` → **Public** → do **NOT** initialize with a README (you already have one) → Create.

```bash
git remote add origin https://github.com/<your-username>/ecommerce-data-platform.git
git push -u origin main
```

If push prompts for a password: GitHub no longer accepts account passwords over HTTPS. Options:
- **GitHub CLI** (easiest): install `gh`, run `gh auth login`, then push works.
- **Personal Access Token**: GitHub → Settings → Developer settings → Tokens → Generate. Use the token as your password.
- **SSH keys**: standard SSH auth, no password prompts.

### 13.9 Pin the repository to your profile

GitHub profile → **Customize your pins** → select `ecommerce-data-platform`. Pinned repos are the first thing recruiters see.

### 13.10 Framing it for recruiters

**Resume bullet** (concise, outcome- and skill-dense):

> Built an end-to-end analytics engineering pipeline ingesting a 14-table ecommerce database into Snowflake via a medallion architecture (bronze/silver/gold), implementing a Kimball star schema with SCD Type 2 dimensions, orchestrated with Dockerized Airflow, and surfaced in a Power BI dashboard with 25+ DAX measures.

**LinkedIn / portfolio blurb** — link the repo and lead with the three differentiators: SCD Type 2 with point-in-time joins, Airflow orchestration, and the full medallion + dimensional model.

**Interview talking points to be ready with**:

- Why SCD Type 2 over Type 1 for customer/product, and the VALID_FROM anchoring gotcha you solved.
- The grain decision for FACT_SALES (order-line) and why it enables clean rollups.
- Conformed dimensions and the bus matrix — why returns, sales, and reviews can be compared on the same customer/date.
- The RBAC model and why you don't run pipelines as ACCOUNTADMIN.
- Idempotency — how each layer is safe to re-run.

### Final checklist

1. `.gitignore` created **before** first `git add`; `.env` confirmed not tracked.
2. `.env.example` committed (blank values), real `.env` excluded.
3. `README.md` with architecture + ER Mermaid diagrams and bus matrix renders on GitHub.
4. `LICENSE` added; dashboard screenshots in `powerbi/`; headline screenshot embedded in the README.
5. Repo pushed to GitHub, set public, pinned to your profile.

---


## Troubleshooting & Gotchas

The full set of issues encountered during the build and how each was resolved. Referenced by step number.

### Step 1 — Environment

**PostgreSQL admin password unknown (needed for the source connection).**
Reset via `pg_hba.conf`:
1. Find it: `psql -U postgres -c "SHOW hba_file;"`
2. Temporarily change auth method for local connections to `trust`.
3. Restart Postgres.
4. Connect without password and set a new one: `ALTER USER postgres WITH PASSWORD 'new_password';`
5. Revert `pg_hba.conf` to `scram-sha-256` (or `md5`) and restart again.
6. Update `.env`.

**WSL + Windows Postgres — connection refused.**
By default Postgres binds to `127.0.0.1` only. To make it reachable from WSL:
1. In `postgresql.conf`: set `listen_addresses = '*'`
2. In `pg_hba.conf`: add `host all all 172.16.0.0/12 scram-sha-256`
3. Windows firewall: `New-NetFirewallRule -DisplayName "PostgreSQL WSL" -Direction Inbound -Protocol TCP -LocalPort 5432 -Action Allow`
4. Restart Postgres service.
5. In `.env`, set `PG_HOST` to the Windows host IP as seen from WSL: `ip route show | grep default | awk '{print $3}'`
6. Test from WSL: `nc -zv <host_ip> 5432` — should show `Connection to <host_ip> 5432 port [tcp/*] succeeded!`

**`No module named 'src.utils'` when running `python -m src.utils.inventory_source`.**
The `__init__.py` files were missing:
```bash
touch src/__init__.py src/utils/__init__.py
```

### Step 2 — Snowflake

**`snowflake.connector.errors.DatabaseError: 250001: Could not connect to Snowflake backend after ...`.**
Wrong account locator format. It must include the region and cloud, e.g. `abc12345.ap-southeast-1.aws`, not just `abc12345`.

**Connection succeeded but `CURRENT_ROLE()` shows `ACCOUNTADMIN` instead of `ECOM_ENGINEER`.**
`.env` has the wrong `SNOWFLAKE_USER`. Change to `SNOWFLAKE_USER=ECOM_PIPELINE` and `SNOWFLAKE_ROLE=ECOM_ENGINEER`. Never run the pipeline as ACCOUNTADMIN in a portfolio project — recruiters check this.

### Step 3 — Bronze

**`snowflake.connector.errors.ProgrammingError: 002003 (42S02): Object 'BRONZE._INGESTION_LOG' does not exist.`**
Future grants from Step 2 didn't run cleanly, or you're connected with the wrong role. Re-run the `GRANT ON FUTURE TABLES` statements from Step 2.2. They're idempotent.

**Decimal columns arriving as FLOAT.**
Expected — pandas converts NUMERIC to `float64`. Bronze tolerates lossy types by design; Silver casts back to proper `NUMBER(p,s)`.

### Step 4 — Silver

**`SQL access control error: Insufficient privileges to operate on table '_SILVER_RUN_LOG'`.**
The `_SILVER_RUN_LOG` was created by ACCOUNTADMIN in a prior run, so ECOM_ENGINEER can't write to it. Fix by transferring ownership:
```sql
USE ROLE ACCOUNTADMIN;
GRANT OWNERSHIP ON TABLE ECOMMERCE_DW.SILVER._SILVER_RUN_LOG TO ROLE ECOM_ENGINEER COPY CURRENT GRANTS;
```

### Step 6 — Gold Dimensions

**`STRING TRUNCATION` error inserting the unknown-member row into `DIM_DATE`.**
Snowflake inferred `VARCHAR(3)` for `MONTH_NAME` from `MONTHNAME()`, so 'Unknown' didn't fit. Fix — explicit VARCHAR(20) casts in the DIM_DATE generator:
```sql
CAST(MONTHNAME(FULL_DATE) AS VARCHAR(20)) AS MONTH_NAME,
CAST(DAYNAME(FULL_DATE) AS VARCHAR(20))   AS DAY_NAME,
CAST(TO_CHAR(FULL_DATE, 'YYYY-MM') AS VARCHAR(20)) AS YEAR_MONTH,
```

**Facts joined to SCD2 dims but all rows orphaned to key -1.**
The initial VALID_FROM for Type 2 dims was set to `CURRENT_TIMESTAMP()` — meaning historical facts dated 2024–2026 fall *before* the dimension's validity window and get routed to the unknown member. Fix — anchor the initial insert to a far-past timestamp:
```sql
INSERT INTO DIM_CUSTOMER (...)
SELECT ..., '1900-01-01'::TIMESTAMP_NTZ AS VALID_FROM, ...
```

Then drop and rebuild DIM_CUSTOMER and DIM_PRODUCT so history reflects the corrected anchor.

### Step 9 — Airflow

**`docker: command not found` in WSL.**
Install Docker Desktop on Windows and enable WSL integration in Settings → Resources → WSL Integration. The Docker daemon runs on Windows; the CLI proxies through WSL.

**Airflow init container fails with `KeyError: getpwuid()` / `getpwuid(): uid not found`.**
The mounted volume's uid doesn't match a user in the container's `/etc/passwd`. Fix — set `user: "0:0"` (root) on the `airflow-init` service in `docker-compose.yaml`:
```yaml
airflow-init:
  user: "0:0"
```

**Airflow scheduler runs but log server URLs are empty.**
Set explicit hostnames on the webserver and scheduler services:
```yaml
webserver:
  hostname: airflow-webserver
scheduler:
  hostname: airflow-scheduler
```

**Airflow tasks fail to reach Windows PostgreSQL.**
Add `extra_hosts` to the scheduler/webserver services so `host.docker.internal` resolves:
```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```
And in the compose `.env`, set `PG_HOST=host.docker.internal`.

**`_PIP_ADDITIONAL_DEPENDENCIES` env var doesn't install packages.**
It's a typo — the actual variable is `_PIP_ADDITIONAL_REQUIREMENTS`. But building a custom image with a proper `Dockerfile` and `requirements-airflow.txt` is the more robust approach anyway.

### Step 10 — Power BI

**Blank/null year appears alongside actual years in a Year slicer.**
The SCD unknown-member row (`DATE_KEY = -1, FULL_DATE = NULL, YEAR = NULL`) is leaking into the visual. Filter it out in Power Query:
1. Home → Transform data → DIM_DATE query.
2. Filter `DATE_KEY` → Number Filters → Does Not Equal → -1.
3. Close & Apply.
4. Re-apply Mark as Date Table (may have been lost).

**DIM_DATE goes past today (into 2027/2028).**
This is by design — date dimensions are precomputed spines sized to cover the analytic horizon. Facts legitimately contain future dates (campaign end dates, coupon expiry). Time intelligence functions (SAMEPERIODLASTYEAR, TOTALYTD) also need future coverage near year boundaries. Leave it as-is; explain this in interviews.

### Step 11 — DAX

**Revenue YoY % shows 0.00% on a standalone card.**
Filter context problem, not a broken measure. On a card with no date filter, `SAMEPERIODLASTYEAR` shifts the entire date range back by one year, but the fact data sits entirely inside both ranges, so both totals compute the same sum → YoY = 0. Fix: put the measure in a matrix with `DIM_DATE[YEAR]` on rows (shows real YoY per year), OR use a hard-anchored measure:
```dax
Revenue YoY % (Latest Year) =
VAR LatestYear = CALCULATE(MAX(DIM_DATE[YEAR]), ALL(DIM_DATE), FACT_SALES)
VAR CurrYear = CALCULATE([Total Revenue], DIM_DATE[YEAR] = LatestYear)
VAR PrevYear = CALCULATE([Total Revenue], DIM_DATE[YEAR] = LatestYear - 1)
RETURN DIVIDE(CurrYear - PrevYear, PrevYear)
```

### Step 13 — Git

**`git restore --staged` fails with `fatal: could not resolve HEAD`.**
You're pre-first-commit, so HEAD doesn't exist yet. Use `git rm --cached` instead:
```bash
git rm --cached -r docker/airflow/logs/
```
The `-r` is recursive; `--cached` keeps files on disk while removing from the index.

**Accidentally committed `.env` before adding `.gitignore`.**
The credentials are compromised — rotate them first (Snowflake: `ALTER USER ECOM_PIPELINE SET PASSWORD = '<new>';`), then scrub git history:
```bash
pip install git-filter-repo
git filter-repo --path .env --invert-paths
git push --force origin main
```

---

## Key architectural decisions (interview-ready summary)

- **Medallion architecture**: Bronze (raw mirror), Silver (cleansed), Gold (Kimball star). Enables clear ownership boundaries, incremental refactoring, and per-layer observability.
- **Kimball over Data Vault or 3NF**: BI-consumer-optimized, straight to Power BI; SCD2 handles history; simpler mental model for interviews.
- **Truncate-and-load Bronze**: 42K rows total across 14 tables — full refresh runs in seconds and is dramatically simpler than incremental. Add CDC/high-water-mark logic when volumes grow past a few hundred MB per run.
- **SCD Type 2 for CUSTOMER and PRODUCT, Type 1 for CAMPAIGN and COUPON**: Type 2 where the attribute changes have analytical meaning (customer city, product price/cost); Type 1 where the current value is what matters (campaign name, coupon discount %).
- **Denormalize categories + suppliers into DIM_PRODUCT**: star schema, not snowflake. One join from FACT_SALES instead of three.
- **`orders` treated as a connector, not a fact**: coarser grain than `order_items`. `FACT_SALES` starts from `order_items` and joins through `orders` for header attributes.
- **Point-in-time joins for SCD2**: fact rows join to the dimension version that was current *at the transaction date*, not the current version. Enables historically accurate margin, geography, etc.
- **Unknown-member pattern (key = -1)**: every dim carries a `-1` row for orphan handling; facts use `COALESCE(dim_key, -1)`. Preserves inner-join semantics without hiding data.
- **Least-privilege service role (ECOM_ENGINEER)**: pipeline never runs as ACCOUNTADMIN. Owned objects, future grants, and hierarchy connection into SYSADMIN.
- **Airflow with LocalExecutor**: simpler than CeleryExecutor for a portfolio; the DAG structure (5 chained tasks with retries and daily schedule) demonstrates the pattern without operational overhead.
- **Power BI Import mode with a marked DIM_DATE**: enables full time-intelligence DAX without DirectQuery latency.

---

## Full file & folder tree (final state)

```
ecommerce-data-platform/
├── .env                       # (gitignored) real credentials
├── .env.example               # committed template
├── .gitignore
├── requirements.txt
├── README.md
├── LICENSE
├── .venv/                     # (gitignored) Python virtual env
│
├── src/
│   ├── __init__.py
│   ├── run_pipeline.py        # end-to-end orchestrator (all 5 stages)
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── db.py              # Postgres + Snowflake connection helpers
│   │   ├── inventory_source.py
│   │   └── test_snowflake.py
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── config.py          # INGESTION_TABLES list (single source of truth)
│   │   ├── bronze.py          # extract + load + logging
│   │   ├── run_bronze.py      # CLI entry
│   │   └── verify_bronze.py   # Postgres vs Bronze parity check
│   │
│   └── transformations/
│       ├── __init__.py
│       ├── silver.py                # runs all silver SQL files, logs runs
│       ├── run_silver.py
│       ├── verify_silver.py         # % rows retained (Bronze -> Silver)
│       ├── gold_dims.py             # runs all dim SQL files
│       ├── run_gold_dims.py
│       ├── verify_gold_dims.py
│       ├── gold_facts.py            # runs all fact SQL files
│       ├── run_gold_facts.py
│       ├── verify_gold_facts.py     # orphan rate metric
│       └── gold_views.py            # runs semantic view SQL
│
├── sql/
│   └── ddl/
│       ├── silver/                   # 14 SQL files (one per source table)
│       │   ├── categories.sql
│       │   ├── suppliers.sql
│       │   ├── products.sql
│       │   ├── inventory.sql
│       │   ├── customers.sql
│       │   ├── marketing_campaigns.sql
│       │   ├── coupons.sql
│       │   ├── orders.sql
│       │   ├── order_items.sql
│       │   ├── payments.sql
│       │   ├── shipments.sql
│       │   ├── returns.sql
│       │   ├── reviews.sql
│       │   └── web_sessions.sql
│       │
│       └── gold/
│           ├── 00_sequences.sql
│           ├── 01_dim_date.sql
│           ├── 02_dim_campaign.sql   # Type 1 MERGE
│           ├── 03_dim_coupon.sql     # Type 1 MERGE
│           ├── 04_dim_customer.sql   # Type 2 (expire + insert)
│           ├── 05_dim_product.sql    # Type 2 (expire + insert)
│           │
│           ├── facts/
│           │   ├── 01_fact_sales.sql
│           │   ├── 02_fact_payments.sql
│           │   ├── 03_fact_shipments.sql
│           │   ├── 04_fact_returns.sql
│           │   ├── 05_fact_reviews.sql
│           │   └── 06_fact_web_sessions.sql
│           │
│           └── views/
│               ├── 01_vw_pipeline_runs.sql   # unified run log
│               ├── 02_vw_sales_detail.sql    # flattened FACT_SALES
│               └── 03_vw_customer_360.sql    # customer-level rollup across SCD2 versions
│
├── docker/
│   └── airflow/
│       ├── Dockerfile
│       ├── docker-compose.yaml
│       ├── requirements-airflow.txt
│       ├── .env               # AIRFLOW_UID, PG_HOST override for containers
│       ├── logs/              # (gitignored)
│       └── dags/
│           └── ecommerce_pipeline_dag.py
│
└── powerbi/
    ├── ecommerce-data-platform.pbix   # (may be gitignored due to size)
    ├── 01_executive_overview.png
    ├── 02_customer_analytics.png
    ├── 03_operations.png
    └── 04_marketing_web.png
```

---

## End of guide

You have a working, documented, publicly-hosted end-to-end analytics engineering project. Total effort: ~47 hours across the 6 phases.

Portfolio next steps to consider:
1. Add CI (GitHub Actions running `verify_bronze.py` on a schedule).
2. Convert Silver + Gold to dbt and demonstrate `dbt test`.
3. Add a streaming variant (Kafka → Snowflake Snowpipe) as a follow-up repo.
4. Publish a technical blog post walking through the SCD2 point-in-time joins — that specific piece is what genuinely separates senior candidates from mid-level in interviews.
