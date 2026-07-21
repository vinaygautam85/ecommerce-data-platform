"""Builds the Gold semantic views from sql/ddl/gold/views/*.sql."""
from pathlib import Path
from loguru import logger
from src.utils.db import get_snowflake_connection

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "gold" / "views"


def run_gold_views_transformations() -> str:
    logger.info(f"SQL_DIR: {SQL_DIR}")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(
            f"No SQL files found in {SQL_DIR}. "
            f"Check the project volume mount and the path."
        )
    logger.info(f"Found {len(sql_files)} SQL files to execute")

    sf = get_snowflake_connection(schema="GOLD")
    try:
        for f in sql_files:
            logger.info(f"[views] Building {f.name} ...")
            sf.execute_string(f.read_text(), remove_comments=False)
            logger.success(f"[views] {f.stem} built")
    finally:
        sf.close()
    return f"views_{len(sql_files)}_built"

if __name__ == "__main__":
    run_gold_views_transformations()