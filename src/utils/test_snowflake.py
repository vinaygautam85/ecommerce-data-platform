from src.utils.db import get_snowflake_connection

def main():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_REGION(), "
                    "CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
        row = cur.fetchone()
        labels = ["Version", "Account", "Region", "Role", "Warehouse", "Database", "Schema"]
        print("Snowflake connection OK\n")
        for label, value in zip(labels, row):
            print(f"  {label:<10} {value}")

        # Confirm all three schemas exist
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = CURRENT_DATABASE()
              AND schema_name IN ('BRONZE', 'SILVER', 'GOLD')
            ORDER BY schema_name;
        """)
        schemas = [r[0] for r in cur.fetchall()]
        print(f"\nSchemas present: {schemas}")
        assert set(schemas) == {"BRONZE", "SILVER", "GOLD"}, "Missing one or more medallion schemas"
        print("All three medallion schemas present.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()