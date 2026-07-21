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
