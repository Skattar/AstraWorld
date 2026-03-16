"""
Task 2b - Datamart Runner
==========================
Executes the datamart SQL queries (02_datamart_queries.sql)
and prints a preview of both report tables.

How to run:
    python scripts/task2b_datamart.py
"""

import os
import sys
import logging
import mysql.connector
from pathlib import Path

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

SQL_FILE = Path(__file__).parent.parent / "sql" / "02_datamart_queries.sql"


def run_sql_file(conn, filepath: Path):
    """
    Execute a .sql file that contains multiple statements.
    Splits on semicolons and runs each statement individually.
    """
    sql_content = filepath.read_text(encoding="utf-8")

    # Remove comment lines and split by semicolon
    statements = []
    for stmt in sql_content.split(";"):
        # Strip whitespace and skip empty statements or pure comments
        cleaned = "\n".join(
            line for line in stmt.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        if cleaned:
            statements.append(cleaned)

    cursor = conn.cursor()
    for stmt in statements:
        try:
            cursor.execute(stmt)
            conn.commit()
        except mysql.connector.Error as e:
            log.error(f"SQL error on statement:\n{stmt[:120]}...\nError: {e}")
            raise
    cursor.close()
    log.info(f"Executed {len(statements)} SQL statement(s) from {filepath.name}")


def preview_table(conn, table, limit=10):
    """Print a table preview using plain cursor — no pandas/SQLAlchemy needed."""
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        print(f"\n(no rows in {table})")
        return
    cols = list(rows[0].keys())
    col_w = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    header = "  ".join(c.ljust(col_w[c]) for c in cols)
    divider = "  ".join("-" * col_w[c] for c in cols)
    print(f"\n{'='*60}")
    print(f"Preview: {table} ({len(rows)} rows shown)")
    print("=" * 60)
    print(header)
    print(divider)
    for r in rows:
        print("  ".join(str(r[c] if r[c] is not None else "NULL").ljust(col_w[c]) for c in cols))


def main():
    log.info("Task 2b: Datamart Query Runner")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        log.info("Connected to MySQL.")
    except mysql.connector.Error as e:
        log.error(f"MySQL connection failed: {e}")
        sys.exit(1)

    try:
        run_sql_file(conn, SQL_FILE)
        preview_table(conn, "dm_sales_summary")
        preview_table(conn, "dm_aftersales_activity")
        log.info("\n✓ Datamart tables updated successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
