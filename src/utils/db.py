import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine
import snowflake.connector

load_dotenv()

required_pg_vars = ["PG_HOST","PG_PORT","PG_USER","PG_PASSWORD","PG_DB"]

def _require(var: str) -> str:
    value = os.getenv(var)
    if not value:
        raise RuntimeError(
            f"Environment variable {var} is missing or empty."
            f"Check that .env exists in the project root and contains all of:"
            f"{', '.join(required_pg_vars)}"
        )
    return value

# ---------- PostgreSQL (source) ----------

def get_postgres_engine():
    host = _require("PG_HOST")
    port = int(_require("PG_PORT"))   # cast after the check
    user = _require("PG_USER")
    password = _require("PG_PASSWORD")
    db = _require("PG_DB")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


# ---------- Snowflake (warehouse) ----------

def _snowflake_credentials(schema: str | None = None) -> dict:
    """
    Builds the dict of Snowflake connection parameters from .env.
    Single source of truth — both the raw connector and the SQLAlchemy
    engine consume it.
    """
    return {
        "account": _require("SNOWFLAKE_ACCOUNT"),
        "user": _require("SNOWFLAKE_USER"),
        "password": _require("SNOWFLAKE_PASSWORD"),
        "role": _require("SNOWFLAKE_ROLE"),
        "warehouse": _require("SNOWFLAKE_WAREHOUSE"),
        "database": _require("SNOWFLAKE_DATABASE"),
        "schema": _require("SNOWFLAKE_SCHEMA"),
    }


def get_snowflake_connection(schema: str | None = None):
    """Raw snowflake.connector.Connection — for cursor-based operations."""
    return snowflake.connector.connect(
        **_snowflake_credentials(schema),
        client_session_keep_alive = False,
    )


def get_snowflake_engine(schema: str | None = None):
    """SQLAlchemy engine — for pandas to_sql / read_sql and bulk operations."""
    from snowflake.sqlalchemy import URL

    return create_engine(URL(**_snowflake_credentials(schema)))