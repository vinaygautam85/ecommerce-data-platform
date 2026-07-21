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
    logger.info(f"SQL_DIR: {SQL_DIR}")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(
            f"No SQL files found in {SQL_DIR}."
            f"Check the project volume mount and the path."
        )
    logger.info(f"Found {len(sql_files)} SQL files to execute")

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


    logger.info(f"=== Silver run done: {run_id} | success={successes} failed={failures} ===")
    if failures > 0:
        raise RuntimeError(
            f"{failures} of {len(sql_files)} silver loads failed."
            f"Query SILVER._SILVER_RUN_LOG WHERE SILVER_RUN_ID='{run_id}' for details."
        )
    return run_id