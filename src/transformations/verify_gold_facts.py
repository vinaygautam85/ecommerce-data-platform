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