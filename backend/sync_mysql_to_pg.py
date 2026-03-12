#!/usr/bin/env python3
"""
sync_mysql_to_pg.py
───────────────────
Full-replace sync from MySQL (buldingdb) → Aiven PostgreSQL.

Run manually whenever you want to refresh the PostgreSQL data:
    python3 sync_mysql_to_pg.py

What it does:
  1. Compares column schemas between MySQL and PostgreSQL; prints warnings
     if columns have been added or removed in MySQL since the last migration.
  2. Truncates all target tables in PostgreSQL (CASCADE to handle FK deps).
  3. Streams each table from MySQL and bulk-inserts into PostgreSQL in batches.
  4. Re-applies FK constraints are already in PG — data is loaded in dependency
     order so they stay satisfied.

Dependencies:
    pip install pymysql psycopg2-binary
"""

import os 
import sys
import datetime
import pymysql
import pymysql.converters
import pymysql.constants.FIELD_TYPE as MYSQL_TYPES
import psycopg2
import psycopg2.extras

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — edit these if credentials change
# ─────────────────────────────────────────────────────────────────────────────

MYSQL_CONFIG = dict(
    host     = os.environ["MYSQL_HOST"],
    user     = os.environ["MYSQL_USER"],
    password = os.environ["MYSQL_PASSWORD"],
    port     = int(os.getenv("MYSQL_PORT", "3306")),
    database = os.environ["MYSQL_DATABASE"],
    charset  = "utf8mb4",
    connect_timeout = 30,
)

PG_DSN = (
    "postgres://avnadmin:AVNS_tnbgy7eoqDmdFg-NsLA"
    "@buildingdb-buildingdb.a.aivencloud.com:13020"
    "/defaultdb?sslmode=require"
)

# Tables to sync, in dependency order (parents before children).
# The FK relationships are:
#   v2_countries → v2_regions
#   v2_cities    → v2_countries
#   ctbuh_complex → v2_regions, v2_countries, v2_cities
TABLES_IN_ORDER = [
    "v2_regions",
    "agglomerations",
    "agglomeration_countries",
    "agglomerations_countries",
    "ctbuh_company_categories",
    "ctbuh_company_subcategories",
    "v2_countries",
    "v2_cities",
    "ctbuh_building",
    "ctbuh_complex",
    "ctbuh_building_company_new",
]

# Rows fetched from MySQL and inserted into PG per batch
BATCH_SIZE = 500

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# ANSI colour codes (disabled automatically if not a TTY)
_COLOUR = sys.stdout.isatty()
def _c(code, text): return f"\033[{code}m{text}\033[0m" if _COLOUR else text
GREEN  = lambda t: _c("32", t)
YELLOW = lambda t: _c("33", t)
RED    = lambda t: _c("31", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


def make_mysql_connection():
    """Connect to MySQL with zero-date and error handling."""
    # Custom converters: treat zero-dates as None instead of raising
    conv = pymysql.converters.conversions.copy()

    def safe_datetime(val):
        if isinstance(val, (bytes, str)):
            s = val.decode() if isinstance(val, bytes) else val
            if s.startswith("0000-00-00"):
                return None
        return pymysql.converters.convert_datetime(val)

    def safe_date(val):
        if isinstance(val, (bytes, str)):
            s = val.decode() if isinstance(val, bytes) else val
            if s.startswith("0000-00-00"):
                return None
        return pymysql.converters.convert_date(val)

    conv[MYSQL_TYPES.DATETIME]  = safe_datetime
    conv[MYSQL_TYPES.TIMESTAMP] = safe_datetime
    conv[MYSQL_TYPES.DATE]      = safe_date

    return pymysql.connect(**MYSQL_CONFIG, conv=conv,
                           cursorclass=pymysql.cursors.DictCursor)


def make_pg_connection():
    return psycopg2.connect(PG_DSN)


# ─────────────────────────────────────────────────────────────────────────────
# Schema comparison
# ─────────────────────────────────────────────────────────────────────────────

def get_mysql_columns(mysql_cur, table):
    """Return ordered list of column names for a MySQL table."""
    mysql_cur.execute(f"SHOW COLUMNS FROM `{table}`")
    return [row["Field"] for row in mysql_cur.fetchall()]


def get_pg_columns(pg_cur, table):
    """Return ordered list of column names for a PostgreSQL table."""
    pg_cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return [row[0] for row in pg_cur.fetchall()]


def check_schema_drift(mysql_cur, pg_cur, table):
    """
    Compare MySQL and PG column lists.
    Returns (ok: bool, warnings: list[str]).
    Columns that exist in MySQL but not PG = the PG schema is out of date.
    Columns that exist in PG but not MySQL = dropped in MySQL (probably fine).
    """
    my_cols = get_mysql_columns(mysql_cur, table)
    pg_cols = get_pg_columns(pg_cur, table)

    my_set = set(my_cols)
    pg_set = set(pg_cols)

    warnings = []
    new_in_mysql = my_set - pg_set
    dropped_in_mysql = pg_set - my_set

    if new_in_mysql:
        warnings.append(
            f"  {YELLOW('NEW in MySQL (not in PG):'):} {', '.join(sorted(new_in_mysql))}"
        )
    if dropped_in_mysql:
        warnings.append(
            f"  {DIM('In PG but not MySQL (may be dropped):'):} "
            f"{', '.join(sorted(dropped_in_mysql))}"
        )

    return (len(warnings) == 0), warnings, my_cols, pg_cols


# ─────────────────────────────────────────────────────────────────────────────
# Data sync
# ─────────────────────────────────────────────────────────────────────────────

def clean_value(val):
    """
    Coerce a MySQL value to something PostgreSQL will accept:
    - datetime/date objects with year=0 (zero-dates that slipped through) → None
    - Everything else passes through unchanged (Python None → SQL NULL).
    """
    if isinstance(val, (datetime.datetime, datetime.date)):
        if val.year == 0 or (hasattr(val, 'year') and val.year < 1):
            return None
    return val


def sync_table(table, mysql_conn, pg_conn):
    """
    Full-replace sync for a single table.
    Returns (rows_inserted, had_error).
    """
    mysql_cur = mysql_conn.cursor()
    pg_cur    = pg_conn.cursor()

    # ── Schema drift check ────────────────────────────────────────────
    ok, drift_warnings, my_cols, pg_cols = check_schema_drift(mysql_cur, pg_cur, table)
    if drift_warnings:
        print(f"  {YELLOW('⚠ Schema drift detected:')} {table}")
        for w in drift_warnings: print(w)

    # Columns we can actually sync: intersection, in MySQL order
    # (PG columns that no longer exist in MySQL are simply left NULL/default)
    common_cols = [c for c in my_cols if c in set(pg_cols)]
    if not common_cols:
        print(f"  {RED('✗ No matching columns — skipping')} {table}")
        return 0, True

    col_list_quoted = ", ".join(f'"{c}"' for c in common_cols)
    placeholders    = ", ".join(["%s"] * len(common_cols))
    insert_sql      = (
        f'INSERT INTO "{table}" ({col_list_quoted}) '
        f'VALUES %s'
    )

    # ── Count source rows ─────────────────────────────────────────────
    mysql_cur.execute(f"SELECT COUNT(*) AS n FROM `{table}`")
    total_rows = mysql_cur.fetchone()["n"]

    # ── Stream from MySQL ─────────────────────────────────────────────
    select_cols = ", ".join(f"`{c}`" for c in common_cols)
    mysql_cur.execute(f"SELECT {select_cols} FROM `{table}`")

    inserted = 0
    batch    = []

    def flush_batch():
        nonlocal inserted
        if not batch:
            return
        psycopg2.extras.execute_values(pg_cur, insert_sql, batch,
                                       page_size=BATCH_SIZE)
        pg_conn.commit()
        inserted += len(batch)
        batch.clear()
        pct = inserted / total_rows * 100 if total_rows else 100
        print(f"    {inserted:>8,} / {total_rows:>8,}  ({pct:.0f}%)", end="\r")

    while True:
        rows = mysql_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            batch.append(tuple(clean_value(row[c]) for c in common_cols))
        flush_batch()

    flush_batch()  # any remainder
    print()  # newline after \r progress

    return inserted, False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    start = datetime.datetime.now()
    print(BOLD(f"\n{'─'*60}"))
    print(BOLD(f"  MySQL → PostgreSQL sync  ({start.strftime('%Y-%m-%d %H:%M:%S')})"))
    print(BOLD(f"{'─'*60}\n"))

    # ── Connect ───────────────────────────────────────────────────────
    print("Connecting to MySQL…", end=" ", flush=True)
    try:
        mysql_conn = make_mysql_connection()
        print(GREEN("✓"))
    except Exception as e:
        print(RED(f"✗  {e}"))
        sys.exit(1)

    print("Connecting to PostgreSQL…", end=" ", flush=True)
    try:
        pg_conn = make_pg_connection()
        print(GREEN("✓"))
    except Exception as e:
        print(RED(f"✗  {e}"))
        sys.exit(1)

    pg_cur = pg_conn.cursor()

    # ── Truncate all tables at once (CASCADE handles FK deps) ─────────
    print("\nTruncating PostgreSQL tables…", end=" ", flush=True)
    try:
        table_list = ", ".join(f'"{t}"' for t in TABLES_IN_ORDER)
        pg_cur.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")
        pg_conn.commit()
        print(GREEN("✓"))
    except Exception as e:
        print(RED(f"✗  {e}"))
        sys.exit(1)

    # ── Sync each table ───────────────────────────────────────────────
    print()
    total_inserted = 0
    errors = []

    for table in TABLES_IN_ORDER:
        print(BOLD(f"  {table}"))
        try:
            rows, had_error = sync_table(table, mysql_conn, pg_conn)
            total_inserted += rows
            status = RED("✗ error") if had_error else GREEN(f"✓ {rows:,} rows")
            print(f"    {status}\n")
            if had_error:
                errors.append(table)
        except Exception as e:
            pg_conn.rollback()
            print(f"    {RED(f'✗ {e}')}\n")
            errors.append(table)

    # ── Summary ───────────────────────────────────────────────────────
    elapsed = datetime.datetime.now() - start
    print(BOLD(f"{'─'*60}"))
    if errors:
        print(RED(f"  Completed with errors in: {', '.join(errors)}"))
    else:
        print(GREEN(f"  ✓ All tables synced successfully"))
    print(f"  Total rows inserted : {total_inserted:,}")
    print(f"  Time elapsed        : {elapsed.seconds}s")
    print(BOLD(f"{'─'*60}\n"))

    mysql_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
