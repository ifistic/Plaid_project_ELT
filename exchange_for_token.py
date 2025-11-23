from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.api import plaid_api
from plaid.api_client import ApiClient, Configuration
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID")
PLAID_SECRET = os.getenv("PLAID_SECRET")
PLAID_ENV = os.getenv("PLAID_ENV", "sandbox").lower()

# Plaid host
ENV_MAP = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com"
}
PLAID_HOST = ENV_MAP.get(PLAID_ENV)

# Initialize Plaid client
configuration = Configuration(
    host=PLAID_HOST,
    api_key={
        "clientId": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET
    }
)
client = plaid_api.PlaidApi(ApiClient(configuration))

# Exchange public token for access token
public_token = os.getenv("PLAID_ACCESS_TOKEN")  # this is currently your public token
exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
exchange_response = client.item_public_token_exchange(exchange_request)
access_token = exchange_response['access_token']

print("Access Token:", access_token)
