"""
Task 1 - Data Landing Pipeline
================================
Reads customer_address_YYYYMMDD.csv from /data folder
and ingests it into customer_addresses_raw in MySQL.

How to run:
    python scripts/task1_ingest_addresses.py
    python scripts/task1_ingest_addresses.py --date 20260315   # specific date
    python scripts/task1_ingest_addresses.py --backfill        # load ALL csvs in /data
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import mysql.connector

# ─────────────────────────────────────────────
# CONFIGURATION — change these to match your setup
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",   # or "db" if using Docker
    "port":     3306,
    "user":     "root",
    "password": "Iamsuccessful",
    "database": "astraworld",
}

DATA_FOLDER = Path("data")     # folder where CSV files land
FILE_PREFIX = "customer_address_"
FILE_SUFFIX = ".csv"

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest.log"),
    ]
)
log = logging.getLogger(__name__)


def get_target_filepath(date_str: str) -> Path:
    """
    Build expected filename from a date string like '20260315'.
    Returns: Path('data/customer_address_20260315.csv')
    """
    filename = f"{FILE_PREFIX}{date_str}{FILE_SUFFIX}"
    return DATA_FOLDER / filename


def read_and_clean_csv(filepath: Path, source_file: str) -> pd.DataFrame:
    """
    Read a CSV file and apply basic cleaning:
    - Normalize city and province to Title Case
    - Add metadata columns (ingested_at, source_file)
    - Validate required columns exist
    """
    log.info(f"Reading file: {filepath}")

    df = pd.read_csv(filepath)

    # ── Validate columns ───────────────────────────────────────────────
    required_cols = {"id", "customer_id", "address", "city", "province", "created_at"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {filepath.name}: {missing}")

    log.info(f"  Rows read: {len(df)}")

    # ── Clean: normalize casing ────────────────────────────────────────
    # Fixes: "JAKARTA PUSAT" → "Jakarta Pusat", "jawa barat" → "Jawa Barat"
    df["city"]     = df["city"].str.strip().str.title()
    df["province"] = df["province"].str.strip().str.title()
    df["address"]  = df["address"].str.strip()

    # ── Add pipeline metadata columns ─────────────────────────────────
    df["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_file"] = source_file

    return df


def upsert_to_mysql(df: pd.DataFrame, conn) -> int:
    """
    Insert rows into customer_addresses_raw.
    Uses INSERT IGNORE to skip exact duplicates (same id + source_file).
    Returns the number of rows actually inserted.
    """
    cursor = conn.cursor()

    sql = """
        INSERT IGNORE INTO customer_addresses_raw
            (id, customer_id, address, city, province, created_at, ingested_at, source_file)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = [
        (
            row["id"],
            row["customer_id"],
            row["address"],
            row["city"],
            row["province"],
            row["created_at"],
            row["ingested_at"],
            row["source_file"],
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(sql, rows)
    conn.commit()

    inserted = cursor.rowcount
    cursor.close()
    return inserted


def process_file(filepath: Path) -> None:
    """
    Full pipeline for one CSV file:
    1. Read + clean CSV
    2. Connect to MySQL
    3. Upsert rows
    4. Log result
    """
    if not filepath.exists():
        log.error(f"File not found: {filepath}")
        log.error("Make sure the CSV is in the /data folder with name format:")
        log.error(f"  customer_address_YYYYMMDD.csv")
        sys.exit(1)

    source_file = filepath.name

    # Step 1: Read and clean
    df = read_and_clean_csv(filepath, source_file)

    # Step 2: Connect to MySQL
    log.info(f"Connecting to MySQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        log.info("  Connected successfully.")
    except mysql.connector.Error as e:
        log.error(f"MySQL connection failed: {e}")
        sys.exit(1)

    # Step 3: Upsert
    try:
        inserted = upsert_to_mysql(df, conn)
        log.info(f"  Rows read: {len(df)} | Rows inserted: {inserted} | Skipped (duplicates): {len(df) - inserted}")
    finally:
        conn.close()

    log.info(f"Done: {source_file}")


def main():
    parser = argparse.ArgumentParser(description="Ingest daily customer_address CSV into MySQL")
    parser.add_argument("--date",     type=str, help="Date to process (YYYYMMDD). Defaults to today.")
    parser.add_argument("--backfill", action="store_true", help="Process ALL csv files in /data folder")
    args = parser.parse_args()

    if args.backfill:
        # Find all matching CSV files in the data folder
        all_files = sorted(DATA_FOLDER.glob(f"{FILE_PREFIX}*{FILE_SUFFIX}"))
        if not all_files:
            log.warning(f"No CSV files found in {DATA_FOLDER}/")
            sys.exit(0)
        log.info(f"Backfill mode: found {len(all_files)} file(s)")
        for f in all_files:
            process_file(f)
    else:
        # Default: today's date
        date_str = args.date or datetime.today().strftime("%Y%m%d")
        filepath = get_target_filepath(date_str)
        process_file(filepath)


if __name__ == "__main__":
    main()
