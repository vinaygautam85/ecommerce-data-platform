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
    logger.info(f"SQL_DIR: {SQL_DIR}")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(
            f"No SQL files found in {SQL_DIR}."
            f"Check the project volume mount and the path."
        )
    logger.info(f"Found {len(sql_files)} SQL files to excecute")

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
    if failures > 0:
        raise RuntimeError(
            f"{failures} of {len(sql_files)} fact loads failed."
            f"Query GOLD._GOLD_RUN_LOG WHERE GOLD_RUN_ID='{run_id}' for details."
        )
    return run_id