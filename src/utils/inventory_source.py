from sqlalchemy import text
from src.utils.db import get_postgres_engine

expected_tables = ["categories","suppliers","products","inventory","customers","marketing_campaigns","coupons","orders",
                    "order_items","payments","shipments","returns","reviews","web_sessions"
                ]

def main():
    engine = get_postgres_engine()
    schema = "public" # should be case sensitive

    print(f"{'Table':<25} {'Rows':>12}")
    print("-" * 40)

    with engine.connect() as conn:
        results = conn.execute(text(f"""
                    select table_name from information_schema.tables
                    where table_schema = :schema
                        and table_type = 'BASE TABLE'
                    order by table_name;
              """), {"schema": schema})
        present = {r[0] for r in results}

        missing = [t for t in expected_tables if t not in present]
        if missing:
            print(f"Warning - missing tables: {missing}")
        
        for tbl in expected_tables:
            if tbl not in present:
                continue
            cnt = conn.execute(text(f'Select count(*) from "{schema}"."{tbl}";')).scalar()
            print(f"{tbl:<25} {cnt:>12,}")

if __name__ == "__main__":
    main()