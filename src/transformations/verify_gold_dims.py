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