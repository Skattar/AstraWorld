"""
Task 2a - Data Cleaning
========================
Reads from raw tables, applies cleaning rules, and writes to _clean tables.

Cleaning rules applied:
  customers_raw:
    - Standardize dob to YYYY-MM-DD (handles 3 formats + sentinel 1900-01-01)
    - Add customer_type: COMPANY (PT/CV prefix) or INDIVIDUAL
  sales_raw:
    - Convert price from "350.000.000" string to INTEGER 350000000
    - Flag potential duplicate sales (same customer_id + model + invoice_date)
  after_sales_raw:
    - Flag orphan VINs (not found in sales_raw)
  customer_addresses_raw:
    - Already cleaned at ingest (Title Case), just copy to clean layer

How to run:
    python scripts/task2a_clean.py
"""

import os
import sys
import logging

import pandas as pd
from dateutil import parser as dateparser
import mysql.connector

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 3306)),
    "user":     os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "Iamsuccessful"),
    "database": os.environ.get("DB_NAME", "astraworld"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPER: read a table WITHOUT pandas/sqlalchemy
# ─────────────────────────────────────────────

def read_table(conn, table: str) -> pd.DataFrame:
    """
    Fetch an entire table using a plain mysql-connector cursor.
    Returns a pandas DataFrame. No SQLAlchemy involved.
    """
    cursor = conn.cursor(dictionary=True)   # dictionary=True → each row is a dict
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def parse_dob(raw_dob):
    """
    Parse any of the 3 date formats found in customers_raw.dob:
      "1998-08-04"  → ISO format        → "1998-08-04"
      "1980/11/15"  → slash format      → "1980-11-15"
      "14/01/1995"  → day-first format  → "1995-01-14"
      "1900-01-01"  → sentinel value    → None
      None          → already null      → None
    """
    if raw_dob is None or str(raw_dob).strip() == "":
        return None
    raw_dob = str(raw_dob).strip()
    if raw_dob == "1900-01-01":
        log.warning(f"  Sentinel dob found: '{raw_dob}' → set to NULL")
        return None
    try:
        parsed = dateparser.parse(raw_dob, dayfirst=True)
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, OverflowError) as e:
        log.warning(f"  Could not parse dob '{raw_dob}': {e} → set to NULL")
        return None


def get_customer_type(name):
    if name is None:
        return "INDIVIDUAL"
    prefixes = ("PT ", "CV ", "UD ", "KOPERASI ", "YAYASAN ")
    if any(name.upper().startswith(p) for p in prefixes):
        return "COMPANY"
    return "INDIVIDUAL"


def clean_price(price_str):
    """Convert "350.000.000" → 350000000"""
    if price_str is None:
        return None
    try:
        return int(str(price_str).replace(".", "").replace(",", "").strip())
    except ValueError:
        log.warning(f"  Could not parse price '{price_str}' → set to NULL")
        return None


# ─────────────────────────────────────────────
# CLEANING FUNCTIONS
# ─────────────────────────────────────────────

def clean_customers(conn):
    log.info("Cleaning customers_raw...")
    df = read_table(conn, "customers_raw")
    log.info(f"  Rows read: {len(df)}")

    df["dob"]           = df["dob"].apply(parse_dob)
    df["customer_type"] = df["name"].apply(get_customer_type)

    df_clean = df[["id", "name", "dob", "customer_type", "created_at"]].copy()
    log.info(f"  Rows after cleaning: {len(df_clean)}")
    log.info(f"  customer_type breakdown:\n{df_clean['customer_type'].value_counts().to_string()}")
    return df_clean


def clean_sales(conn):
    log.info("Cleaning sales_raw...")
    df = read_table(conn, "sales_raw")
    log.info(f"  Rows read: {len(df)}")

    df["price"] = df["price"].apply(clean_price)

    dup_mask = df.duplicated(subset=["customer_id", "model", "invoice_date"], keep=False)
    df["is_duplicate_flag"] = dup_mask.astype(int)

    dup_count = df["is_duplicate_flag"].sum()
    if dup_count > 0:
        log.warning(f"  ⚠ Found {dup_count} row(s) flagged as potential duplicates:")
        log.warning(df[df["is_duplicate_flag"] == 1][["vin", "customer_id", "model", "invoice_date", "price"]].to_string())

    return df[["vin", "customer_id", "model", "invoice_date", "price", "is_duplicate_flag", "created_at"]].copy()


def clean_after_sales(conn):
    log.info("Cleaning after_sales_raw...")
    df_as    = read_table(conn, "after_sales_raw")
    df_sales = read_table(conn, "sales_raw")
    log.info(f"  Rows read: {len(df_as)}")

    known_vins = set(df_sales["vin"])
    df_as["is_orphan_vin"] = df_as["vin"].apply(lambda v: 1 if v not in known_vins else 0)

    orphans = df_as[df_as["is_orphan_vin"] == 1]
    if len(orphans) > 0:
        log.warning(f"  ⚠ Found {len(orphans)} orphan VIN(s) (serviced car not in sales system):")
        log.warning(orphans[["service_ticket", "vin", "customer_id", "model"]].to_string())

    return df_as


def clean_addresses(conn):
    log.info("Cleaning customer_addresses_raw...")
    df = read_table(conn, "customer_addresses_raw")
    log.info(f"  Rows read: {len(df)}")
    return df


# ─────────────────────────────────────────────
# WRITE CLEAN TABLES TO MYSQL
# ─────────────────────────────────────────────

def write_clean_table(df, table_name, conn):
    """
    Write a DataFrame to MySQL using pure mysql-connector.
    Drops and recreates the table each run (idempotent).
    """
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    type_map = {
        "int64": "BIGINT", "int32": "INT", "float64": "DOUBLE",
        "bool": "TINYINT(1)", "object": "TEXT", "datetime64[ns]": "DATETIME",
    }
    col_defs = [
        f"`{col}` {type_map.get(str(dtype), 'TEXT')}"
        for col, dtype in df.dtypes.items()
    ]
    cursor.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
    conn.commit()

    import datetime
    def safe_val(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        # Convert any datetime/timestamp (with or without timezone) to plain string
        if isinstance(v, (datetime.datetime, datetime.date)):
            return str(v)
        return v

    placeholders = ", ".join(["%s"] * len(df.columns))
    rows = [
        tuple(safe_val(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    conn.commit()
    cursor.close()
    log.info(f"  ✓ Written {len(df)} rows to {table_name}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("Task 2a: Data Cleaning Pipeline")
    log.info("=" * 55)

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        log.info("Connected to MySQL.\n")
    except mysql.connector.Error as e:
        log.error(f"MySQL connection failed: {e}")
        sys.exit(1)

    try:
        df_customers   = clean_customers(conn)
        df_sales       = clean_sales(conn)
        df_after_sales = clean_after_sales(conn)
        df_addresses   = clean_addresses(conn)

        log.info("\nWriting clean tables...")
        write_clean_table(df_customers,   "customers_clean",          conn)
        write_clean_table(df_sales,       "sales_clean",              conn)
        write_clean_table(df_after_sales, "after_sales_clean",        conn)
        write_clean_table(df_addresses,   "customer_addresses_clean", conn)

        log.info("\n✓ All clean tables ready.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
