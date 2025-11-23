#!/usr/bin/env python3
"""
===============================================================
 Postgres → CSV → S3 Export Utility
---------------------------------------------------------------
Exports one or more PostgreSQL tables to CSV format (NO gzip)
and uploads them to an AWS S3 bucket.

This script supports:
  • Environment-based configuration via .env (dotenv)
  • Optional CLI overrides (--bucket, --prefix, --tables)
  • Streaming COPY TO STDOUT for efficient CSV export
  • Uploading directly to S3 using boto3
  • Memory-safe temporary file handling via SpooledTemporaryFile
  • A timestamped, unique S3 key naming structure

Environment Variables (load from .env or export manually):
  PG_HOST        - Postgres host (required)
  PG_PORT        - Postgres port (default: 5432)
  PG_DATABASE    - Database name (required)
  PG_USER        - Database user (required)
  PG_PASSWORD    - Database password (required)

  S3_BUCKET      - Default bucket name (override with --bucket)
  AWS_REGION     - Optional AWS region (otherwise boto3 default)

Example .env:
  PG_HOST=localhost
  PG_PORT=5432
  PG_DATABASE=ecommerce_db
  PG_USER=postgres
  PG_PASSWORD=password123

  S3_BUCKET=my-export-bucket
  AWS_REGION=us-east-1

CLI Examples:
  python pg_to_s3.py
  python pg_to_s3.py --tables accounts
  python pg_to_s3.py --prefix daily/
  python pg_to_s3.py --bucket override-bucket
  python pg_to_s3.py --dry-run

S3 Output Format:
  <prefix>/<table>.<timestamp>.csv

Example:
  daily/accounts.20250213T121355Z.csv

===============================================================
"""

import os
import sys
import argparse
import tempfile
from datetime import datetime, timezone
from dotenv import load_dotenv

import boto3
import psycopg2
from psycopg2 import sql

# ---------------------------------------------------------------
# Load environment variables from .env
# ---------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
def pg_connect():
    """
    Establish a PostgreSQL connection using environment variables.
    Raises:
        EnvironmentError: When a required PG_* variable is missing.
        psycopg2.Error: For connection issues.
    """
    if not (PG_HOST and PG_DATABASE and PG_USER and PG_PASSWORD):
        raise EnvironmentError(
            "Missing Postgres configuration. "
            "Ensure PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD are set."
        )

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )


def s3_client():
    """
    Returns a boto3 S3 client.
    Uses AWS_REGION if provided; otherwise default boto3 config.
    """
    if AWS_REGION:
        return boto3.client("s3", region_name=AWS_REGION)
    return boto3.client("s3")


def timestamp_now():
    """
    Returns a UTC timestamp in a compact sortable format:
    YYYYMMDDTHHMMSSZ
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------
# Core Export Logic
# ---------------------------------------------------------------
def export_table_to_s3(table_name, s3_bucket, s3_prefix=None, conn=None, s3=None):
    """
    Export a Postgres table to CSV and upload to S3.

    Parameters:
        table_name (str)   - Name of the database table.
        s3_bucket (str)    - S3 bucket name.
        s3_prefix (str)    - Optional folder/key prefix.
        conn               - Optional existing PostgreSQL connection.
        s3                 - Optional existing S3 client.

    Returns:
        dict: {
            "rows": <number of rows exported>,
            "s3_key": <final s3 key>,
        }

    Behavior:
        • Performs COPY (SELECT * FROM table) TO STDOUT WITH CSV HEADER
        • Streams output into a spooled temp file (memory-efficient)
        • Uploads plain CSV (NO gzip) to S3
    """

    if conn is None:
        conn = pg_connect()
    if s3 is None:
        s3 = s3_client()

    cursor = conn.cursor()

    # Build SELECT
    select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))

    # S3 key construction
    prefix = (s3_prefix.rstrip("/") + "/") if s3_prefix else ""
    key = f"{prefix}{table_name}.{timestamp_now()}.csv"

    # Temporary CSV file (binary mode for S3 upload)
    with tempfile.SpooledTemporaryFile(mode="w+b", max_size=20 * 1024 * 1024) as spool:

        class _Writer:
            """Adapter to let COPY TO STDOUT write text or bytes to the spool."""
            def write(self, data):
                if isinstance(data, str):
                    data = data.encode("utf-8")
                spool.write(data)

        writer = _Writer()

        # COPY command for CSV export
        copy_sql = f"COPY ({select_query.as_string(conn)}) TO STDOUT WITH CSV HEADER"
        cursor.copy_expert(copy_sql, writer)

        spool.seek(0)

        if not s3_bucket:
            raise EnvironmentError("Missing S3 bucket name.")

        # Upload CSV directly
        s3.upload_fileobj(
            Fileobj=spool,
            Bucket=s3_bucket,
            Key=key,
            ExtraArgs={"ContentType": "text/csv"}
        )

    # Count rows
    cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
    rowcount = cursor.fetchone()[0]

    cursor.close()
    return {"rows": rowcount, "s3_key": key}


# ---------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------
def main():
    """
    Command-line interface for exporting 1 or more tables to S3.
    Supports dry-run mode, custom prefix, and bucket override.
    """
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL tables to CSV and upload to S3 (no gzip)."
    )

    parser.add_argument("--bucket", "-b", help="Override S3 bucket (default from S3_BUCKET env var)")
    parser.add_argument("--prefix", "-p", default="", help="Optional S3 key prefix")
    parser.add_argument("--tables", "-t", nargs="+",
                        default=["accounts", "transactions", "customers"],
                        help="Tables to export (default: accounts transactions customers)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show row counts only; do not write CSV or upload")

    args = parser.parse_args()

    s3_bucket = args.bucket or S3_BUCKET

    if not s3_bucket:
        print("ERROR: S3 bucket must be provided via --bucket or S3_BUCKET env var.", file=sys.stderr)
        sys.exit(2)

    # Connect to PostgreSQL
    try:
        conn = pg_connect()
    except Exception as e:
        print("Postgres connection failed:", e, file=sys.stderr)
        sys.exit(3)

    s3 = s3_client()
    results = {}

    # Process tables
    for table in args.tables:
        print(f"Exporting table '{table}' ...", end=" ", flush=True)

        try:
            if args.dry_run:
                cur = conn.cursor()
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                count = cur.fetchone()[0]
                cur.close()

                print(f"{count} rows (dry-run)")
                results[table] = {"rows": count, "s3_key": None}

            else:
                res = export_table_to_s3(
                    table_name=table,
                    s3_bucket=s3_bucket,
                    s3_prefix=args.prefix,
                    conn=conn,
                    s3=s3
                )
                print(f"uploaded → s3://{s3_bucket}/{res['s3_key']} ({res['rows']} rows)")
                results[table] = res

        except Exception as e:
            print(f"FAILED: {e}", file=sys.stderr)
            results[table] = {"error": str(e)}

    conn.close()

    # Summary
    print("\nSummary:")
    for table, info in results.items():
        if "error" in info:
            print(f" - {table}: ERROR → {info['error']}")
        else:
            print(f" - {table}: {info['rows']} rows → {info['s3_key']}")


if __name__ == "__main__":
    main()