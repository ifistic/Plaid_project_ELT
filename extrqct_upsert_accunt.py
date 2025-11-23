#!/usr/bin/env python3
"""
Plaid to PostgreSQL ETL - Sandbox Environment

- Auto-exchanges PLAID_PUBLIC_TOKEN if PLAID_ACCESS_TOKEN is not set
- Loads accounts and transactions into Postgres with upserts
- Prints number of inserted and updated records
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
load_dotenv()

from plaid import Configuration, ApiClient, Environment
from plaid.api import plaid_api
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest

import psycopg2
from psycopg2.extras import execute_values

# ---------- Helpers ----------
REQUIRED_ENV = [
    "PLAID_CLIENT_ID", "PLAID_SECRET",
    "PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD"
]

def env_or_fail(key):
    v = os.getenv(key)
    if not v:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return v

def safe_str(value):
    """Convert value to string if not None, else None"""
    return str(value) if value is not None else None

def serialize_location(location):
    """Convert Plaid Location object to dict, safely"""
    if location is None:
        return {}
    try:
        return location.to_dict()
    except AttributeError:
        # fallback: treat as dict-like
        return dict(location)

# ---------- ETL Class ----------
class PlaidPostgresETL:
    def __init__(self):
        # Plaid credentials
        self.plaid_client_id = env_or_fail("PLAID_CLIENT_ID")
        self.plaid_secret = env_or_fail("PLAID_SECRET")
        self.plaid_access_token = os.getenv("PLAID_ACCESS_TOKEN")
        self.plaid_public_token = os.getenv("PLAID_PUBLIC_TOKEN")

        # Postgres credentials
        self.pg_host = env_or_fail("PG_HOST")
        self.pg_port = env_or_fail("PG_PORT")
        self.pg_database = env_or_fail("PG_DATABASE")
        self.pg_user = env_or_fail("PG_USER")
        self.pg_password = env_or_fail("PG_PASSWORD")

        # Plaid API client (sandbox)
        configuration = Configuration(
            host=Environment.Sandbox,
            api_key={"clientId": self.plaid_client_id, "secret": self.plaid_secret}
        )
        api_client = ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

        self.pg_conn = None

    # ---------- Plaid helpers ----------
    def exchange_public_token(self, public_token):
        """Exchange Plaid sandbox public_token for access_token"""
        if not public_token:
            raise ValueError("public_token is required to exchange for an access token.")
        req = ItemPublicTokenExchangeRequest(public_token=public_token)
        resp = self.client.item_public_token_exchange(req)
        self.plaid_access_token = resp["access_token"]
        item_id = resp.get("item_id")
        print(f"✓ Exchanged public_token for access_token (item_id: {item_id})")
        return self.plaid_access_token, item_id

    # ---------- Postgres ----------
    def connect_postgres(self):
        self.pg_conn = psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            database=self.pg_database,
            user=self.pg_user,
            password=self.pg_password,
        )
        print("✓ Connected to PostgreSQL")

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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)")
        self.pg_conn.commit()
        cursor.close()
        print("✓ Tables created/verified")

    # ---------- Extract ----------
    def extract_accounts(self):
        req = AccountsGetRequest(access_token=self.plaid_access_token)
        resp = self.client.accounts_get(req)
        accounts = resp["accounts"]
        print(f"✓ Extracted {len(accounts)} accounts")
        return accounts

    def extract_transactions(self, start_date=None, end_date=None):
        if not start_date:
            start_date = (datetime.now(timezone.utc) - timedelta(days=90)).date()
        if not end_date:
            end_date = datetime.now(timezone.utc).date()

        req = TransactionsGetRequest(
            access_token=self.plaid_access_token,
            start_date=start_date,
            end_date=end_date
        )
        resp = self.client.transactions_get(req)
        transactions = resp["transactions"]
        total = resp.get("total_transactions", len(transactions))

        # Pagination: fetch all transactions
        while len(transactions) < total:
            req = TransactionsGetRequest(
                access_token=self.plaid_access_token,
                start_date=start_date,
                end_date=end_date,
            )
            resp = self.client.transactions_get(req)
            transactions.extend(resp["transactions"])

        print(f"✓ Extracted {len(transactions)} transactions (total expected: {total})")
        return transactions

    # ---------- Load ----------
    def load_accounts(self, accounts):
        if not accounts:
            return
        cursor = self.pg_conn.cursor()
        rows = [
            (
                a["account_id"],
                safe_str(a.get("name")),
                safe_str(a.get("official_name")),
                safe_str(a.get("type")),
                safe_str(a.get("subtype")),
                safe_str(a.get("mask")),
                a["balances"].get("current"),
                a["balances"].get("available"),
                safe_str(a["balances"].get("iso_currency_code")),
                datetime.now(timezone.utc),
                datetime.now(timezone.utc)
            ) for a in accounts
        ]
        query = """
            INSERT INTO accounts (account_id, name, official_name, type, subtype, mask,
                                  current_balance, available_balance, currency_code, created_at, updated_at)
            VALUES %s
            ON CONFLICT (account_id) DO UPDATE SET
                name = EXCLUDED.name,
                official_name = EXCLUDED.official_name,
                type = EXCLUDED.type,
                subtype = EXCLUDED.subtype,
                mask = EXCLUDED.mask,
                current_balance = EXCLUDED.current_balance,
                available_balance = EXCLUDED.available_balance,
                currency_code = EXCLUDED.currency_code,
                updated_at = EXCLUDED.updated_at
            RETURNING (xmax = 0) AS inserted
        """
        execute_values(cursor, query, rows)
        result = cursor.fetchall()
        inserted = sum(r[0] for r in result)
        updated = len(result) - inserted
        self.pg_conn.commit()
        cursor.close()
        print(f"✓ Accounts loaded: {len(result)} (Inserted: {inserted}, Updated: {updated})")

    def load_transactions(self, transactions):
        if not transactions:
            return
        cursor = self.pg_conn.cursor()
        rows = [
            (
                t["transaction_id"],
                t["account_id"],
                t["amount"],
                t["date"],
                safe_str(t.get("name")),
                safe_str(t.get("merchant_name")),
                t.get("category", []),
                t.get("pending"),
                safe_str(t.get("payment_channel")),
                safe_str(t.get("transaction_type")),
                json.dumps(serialize_location(t.get("location"))),
                datetime.now(timezone.utc),
                datetime.now(timezone.utc)
            ) for t in transactions
        ]
        query = """
            INSERT INTO transactions (
                transaction_id, account_id, amount, date, name, merchant_name,
                category, pending, payment_channel, transaction_type, location,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                amount = EXCLUDED.amount,
                date = EXCLUDED.date,
                name = EXCLUDED.name,
                merchant_name = EXCLUDED.merchant_name,
                category = EXCLUDED.category,
                pending = EXCLUDED.pending,
                payment_channel = EXCLUDED.payment_channel,
                transaction_type = EXCLUDED.transaction_type,
                location = EXCLUDED.location,
                updated_at = EXCLUDED.updated_at
            RETURNING (xmax = 0) AS inserted
        """
        execute_values(cursor, query, rows)
        result = cursor.fetchall()
        inserted = sum(r[0] for r in result)
        updated = len(result) - inserted
        self.pg_conn.commit()
        cursor.close()
        print(f"✓ Transactions loaded: {len(result)} (Inserted: {inserted}, Updated: {updated})")

    # ---------- Run ETL ----------
    def run(self, start_date=None, end_date=None):
        # Auto-exchange sandbox public_token if access token is missing
        if not self.plaid_access_token:
            if self.plaid_public_token:
                print("No access token found — exchanging PLAID_PUBLIC_TOKEN for sandbox...")
                self.exchange_public_token(self.plaid_public_token)
            else:
                raise EnvironmentError("PLAID_ACCESS_TOKEN not set. Provide an access token or PLAID_PUBLIC_TOKEN.")

        try:
            self.connect_postgres()
            self.create_tables()
            accounts = self.extract_accounts()
            transactions = self.extract_transactions(start_date, end_date)
            self.load_accounts(accounts)
            self.load_transactions(transactions)
            print("\n=== ETL completed successfully ===\n")
        except Exception as e:
            print("✗ ETL failed:", e)
            if self.pg_conn:
                self.pg_conn.rollback()
            raise
        finally:
            if self.pg_conn:
                self.pg_conn.close()
                print("✓ Postgres connection closed")

# ---------- CLI Entrypoint ----------
def main():
    start_date = None
    end_date = None
    if len(sys.argv) >= 2:
        start_date = sys.argv[1]
    if len(sys.argv) >= 3:
        end_date = sys.argv[2]

    etl = PlaidPostgresETL()
    etl.run(start_date, end_date)

if __name__ == "__main__":
    main()
