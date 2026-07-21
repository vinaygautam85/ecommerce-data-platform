# Ingestion configuration. Single source of truth for which tables move from the Postgres source to the Snowflake's BRONZE schema

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class IngestionTable:
    source_schema: str
    source_table: str
    target_table: str
    primary_key: str | None = None

SOURCE_SCHEMA = os.getenv("PG_SCHEMA","public")

# Ordered parent → child. Order doesn't matter for full-refresh Bronze,
# but it'll matter later for Silver/Gold dependency-aware runs.
INGESTION_TABLES: list[IngestionTable] = [
    IngestionTable(SOURCE_SCHEMA, "categories",          "CATEGORIES",          "category_id"),
    IngestionTable(SOURCE_SCHEMA, "suppliers",           "SUPPLIERS",           "supplier_id"),
    IngestionTable(SOURCE_SCHEMA, "products",            "PRODUCTS",            "product_id"),
    IngestionTable(SOURCE_SCHEMA, "inventory",           "INVENTORY",           "inventory_id"),
    IngestionTable(SOURCE_SCHEMA, "customers",           "CUSTOMERS",           "customer_id"),
    IngestionTable(SOURCE_SCHEMA, "marketing_campaigns", "MARKETING_CAMPAIGNS", "campaign_id"),
    IngestionTable(SOURCE_SCHEMA, "coupons",             "COUPONS",             "coupon_id"),
    IngestionTable(SOURCE_SCHEMA, "orders",              "ORDERS",              "order_id"),
    IngestionTable(SOURCE_SCHEMA, "order_items",         "ORDER_ITEMS",         "order_item_id"),
    IngestionTable(SOURCE_SCHEMA, "payments",            "PAYMENTS",            "payment_id"),
    IngestionTable(SOURCE_SCHEMA, "shipments",           "SHIPMENTS",           "shipment_id"),
    IngestionTable(SOURCE_SCHEMA, "returns",             "RETURNS",             "return_id"),
    IngestionTable(SOURCE_SCHEMA, "reviews",             "REVIEWS",             "review_id"),
    IngestionTable(SOURCE_SCHEMA, "web_sessions",        "WEB_SESSIONS",        "session_id"),
    ]