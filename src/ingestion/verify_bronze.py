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