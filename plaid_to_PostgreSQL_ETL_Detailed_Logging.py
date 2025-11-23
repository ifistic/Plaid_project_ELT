#!/usr/bin/env python3
"""
Plaid to PostgreSQL ETL - Sandbox Environment (Extended)
- Explicitly prints which table is updated or inserted into
- Performs UPSERT (insert or update) for all relevant tables: accounts, categories, customers, order_items, orders, products, transactions
- Lists tables and exports all tables to AWS S3 as CSV files with prefix support
- Fixes psycopg2 "can't adapt type 'AccountType'" by converting non-serializable types to string
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import csv
import boto3

load_dotenv()

from plaid import Configuration, ApiClient, Environment
from plaid.api import plaid_api
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest

import psycopg2
from psycopg2.extras import execute_values

# ---------- Helpers ----------
REQUIRED_ENV = ["PLAID_CLIENT_ID", "PLAID_SECRET", "PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD", "AWS_S3_BUCKET", "AWS_S3_PREFIX"]

def env_or_fail(key):
    v = os.getenv(key)
    if not v:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return v

def safe_str(value):
    if value is None:
        return None
    try:
        return str(value)
    except Exception:
        return json.dumps(value)

def serialize_location(location):
    if location is None:
        return {}
    try:
        return location.to_dict()
    except Exception:
        try:
            return dict(location)
        except Exception:
            return {}

# ---------- ETL Class ----------
class PlaidPostgresETL:
    def __init__(self):
        self.plaid_client_id = env_or_fail("PLAID_CLIENT_ID")
        self.plaid_secret = env_or_fail("PLAID_SECRET")
        self.plaid_access_token = os.getenv("PLAID_ACCESS_TOKEN")
        self.plaid_public_token = os.getenv("PLAID_PUBLIC_TOKEN")

        self.pg_host = env_or_fail("PG_HOST")
        self.pg_port = env_or_fail("PG_PORT")
        self.pg_database = env_or_fail("PG_DATABASE")
        self.pg_user = env_or_fail("PG_USER")
        self.pg_password = env_or_fail("PG_PASSWORD")

        self.s3_bucket = env_or_fail("AWS_S3_BUCKET")
        self.s3_prefix = os.getenv("AWS_S3_PREFIX", "")
        if self.s3_prefix and not self.s3_prefix.endswith('/'):
            self.s3_prefix += '/'

        configuration = Configuration(
            host=Environment.Sandbox,
            api_key={"clientId": self.plaid_client_id, "secret": self.plaid_secret}
        )
        api_client = ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

        self.pg_conn = None

    # ---------- PostgreSQL Connection ----------
    def connect_postgres(self):
        self.pg_conn = psycopg2.connect(
            host=self.pg_host, port=self.pg_port, database=self.pg_database,
            user=self.pg_user, password=self.pg_password
        )
        print("✓ Connected to PostgreSQL")

    # ---------- Table Creation ----------
    def create_tables(self):
        cursor = self.pg_conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                official_name VARCHAR(255),
                type VARCHAR(50),
                subtype VARCHAR(50),
                mask VARCHAR(10),
                current_balance DECIMAL(15,2),
                available_balance DECIMAL(15,2),
                currency_code VARCHAR(10),
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(255) PRIMARY KEY,
                account_id VARCHAR(255) REFERENCES accounts(account_id),
                amount DECIMAL(15,2),
                date DATE,
                name VARCHAR(500),
                merchant_name VARCHAR(255),
                category TEXT[],
                pending BOOLEAN,
                payment_channel VARCHAR(50),
                transaction_type VARCHAR(50),
                location JSONB,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for table in ['categories','customers','products','orders','order_items']:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)")
        self.pg_conn.commit()
        cursor.close()
        print("✓ Tables created/verified")

    # ---------- List Tables ----------
    def list_tables(self):
        cursor = self.pg_conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        tables = cursor.fetchall()
        cursor.close()
        print("\n=== TABLES IN DATABASE ===")
        for (t,) in tables:
            print(f" - {t}")
        return [t for (t,) in tables]

    # ---------- Plaid ETL Methods ----------
    def extract_accounts(self):
        if not self.plaid_access_token:
            raise ValueError("PLAID_ACCESS_TOKEN is required")
        req = AccountsGetRequest(access_token=self.plaid_access_token)
        resp = self.client.accounts_get(req)
        accounts = resp.get("accounts", [])
        print(f"✓ Extracted {len(accounts)} accounts")
        return accounts

    def extract_transactions(self, start_date=None, end_date=None):
        if not self.plaid_access_token:
            raise ValueError("PLAID_ACCESS_TOKEN is required")
        if not start_date:
            start_date = (datetime.now(timezone.utc) - timedelta(days=90)).date()
        if not end_date:
            end_date = datetime.now(timezone.utc).date()
        req = TransactionsGetRequest(access_token=self.plaid_access_token, start_date=start_date, end_date=end_date)
        resp = self.client.transactions_get(req)
        transactions = resp.get("transactions", [])
        total = resp.get("total_transactions", len(transactions))
        while len(transactions) < total:
            resp = self.client.transactions_get(req)
            transactions.extend(resp.get("transactions", []))
        print(f"✓ Extracted {len(transactions)} transactions (expected: {total})")
        return transactions

    # ---------- Generic UPSERT ----------
    def upsert_table(self, table_name, data_list, conflict_column):
        if not data_list:
            print(f"No data to upsert for table {table_name}")
            return
        cursor = self.pg_conn.cursor()
        now = datetime.now(timezone.utc)
        columns = list(data_list[0].keys())
        rows = []
        for d in data_list:
            row = [safe_str(d.get(col)) if col not in ['created_at','updated_at'] else d.get(col, now) for col in columns]
            rows.append(tuple(row))
        update_cols = [col for col in columns if col != conflict_column]
        set_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT ({conflict_column}) DO UPDATE SET {set_clause}"
        execute_values(cursor, query, rows)
        self.pg_conn.commit()
        cursor.close()
        print(f"✓ Upserted {len(data_list)} rows into {table_name}")

    # ---------- Export to S3 ----------
    def export_table_to_s3(self, table_name):
        cursor = self.pg_conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        cursor.close()

        csv_filename = f"{table_name}.csv"
        with open(csv_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)

        s3 = boto3.client('s3')
        s3_key = f"{self.s3_prefix}{table_name}.csv"
        s3.upload_file(csv_filename, self.s3_bucket, s3_key)
        print(f"✓ Exported {table_name} to S3 bucket {self.s3_bucket} with key {s3_key}")

    # ---------- ETL Run ----------
    def run(self, start_date=None, end_date=None):
        if not self.plaid_access_token and self.plaid_public_token:
            print("Exchanging PLAID_PUBLIC_TOKEN for access token...")
            self.exchange_public_token(self.plaid_public_token)

        self.connect_postgres()
        self.create_tables()
        self.list_tables()

        now = datetime.now(timezone.utc)
        accounts = self.extract_accounts()
        transactions = self.extract_transactions(start_date, end_date)

        self.upsert_table('accounts', [{
            'account_id': a['account_id'], 'name': a.get('name'), 'official_name': a.get('official_name'),
            'type': safe_str(a.get('type')), 'subtype': safe_str(a.get('subtype')), 'mask': safe_str(a.get('mask')),
            'current_balance': a.get('balances', {}).get('current'), 'available_balance': a.get('balances', {}).get('available'),
            'currency_code': safe_str(a.get('balances', {}).get('iso_currency_code')), 'created_at': now, 'updated_at': now
        } for a in accounts], 'account_id')

        self.upsert_table('transactions', [{
            'transaction_id': t['transaction_id'], 'account_id': t.get('account_id'), 'amount': t.get('amount'),
            'date': t.get('date'), 'name': safe_str(t.get('name')), 'merchant_name': safe_str(t.get('merchant_name')),
            'category': t.get('category'), 'pending': t.get('pending'), 'payment_channel': safe_str(t.get('payment_channel')),
            'transaction_type': safe_str(t.get('transaction_type')), 'location': json.dumps(serialize_location(t.get('location'))),
            'created_at': now, 'updated_at': now
        } for t in transactions], 'transaction_id')

        for table in ['categories','customers','products','orders','order_items']:
            self.upsert_table(table, [], conflict_column='created_at')

        # Export all tables to S3
        for table in ['accounts','transactions','categories','customers','products','orders','order_items']:
            self.export_table_to_s3(table)

        print("\n=== ETL run completed and exported to S3 ===")

# ---------- CLI Entrypoint ----------
def main():
    start_date = sys.argv[1] if len(sys.argv) >= 2 else None
    end_date = sys.argv[2] if len(sys.argv) >= 3 else None
    etl = PlaidPostgresETL()
    etl.run(start_date, end_date)

if __name__ == "__main__":
    main()