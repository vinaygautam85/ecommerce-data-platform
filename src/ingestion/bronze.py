"""
Bronze layer ingestion: Postgres source > Snowflake BRONZE

Stragety: Truncate and load per run. Adds audit columns to every row
Every run is logged to BRONZE._INGESTION_LOG
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
                Create table if not exists {BRONZE_SCHEMA}.{LOG_TABLE} (
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


def log_run(sf_conn, ingestion_id,table_name, started_at, ended_at, rows_extracted, rows_loaded, status, error=None):
    cur = sf_conn.cursor()
    try:
        cur.execute(f"""
                insert into {BRONZE_SCHEMA}.{LOG_TABLE} (
                    INGESTION_ID, SOURCE_SYSTEM, TABLE_NAME, STARTED_AT, ENDED_AT, ROWS_EXTRACTED, ROWS_LOADED, STATUS, ERROR_MESSAGE)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (ingestion_id, SOURCE_SYSTEM, table_name, started_at, ended_at, rows_extracted, rows_loaded, status, error)
        )
    finally:
        cur.close()


# ---------- Extract & Load ----------

def extract_from_postgres(table: IngestionTable) -> pd.DataFrame:
    pg = get_postgres_engine()
    try:
        query=f'Select * from "{table.source_schema}"."{table.source_table}";'
        df = pd.read_sql(query,pg)
        return df
    finally:
        pg.dispose()


def load_to_snowflake(df: pd.DataFrame, target_table: str, ingestion_id: str, loaded_at: datetime) -> int:
    # uppercase column names - snowflake convention
    df.columns = [c.upper() for c in df.columns]

    # Audit Columns
    df["_LOADED_AT"] = loaded_at
    df["_INGESTION_ID"] = ingestion_id
    df["_SOURCE_SYSTEM"] = SOURCE_SYSTEM

    engine = get_snowflake_engine(schema=BRONZE_SCHEMA)
    try:
        df.to_sql(
            name=target_table.lower(),
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

