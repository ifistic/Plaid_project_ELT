import os
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.products import Products
from plaid.api_client import ApiClient, Configuration

# Load environment variables
load_dotenv()

PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID")
PLAID_SECRET = os.getenv("PLAID_SECRET")
PLAID_ENV = os.getenv("PLAID_ENV", "sandbox").lower()

if not PLAID_CLIENT_ID or not PLAID_SECRET:
    raise ValueError("PLAID_CLIENT_ID or PLAID_SECRET not set in .env")

# Map environment to Plaid host
ENV_MAP = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com"
}

PLAID_HOST = ENV_MAP.get(PLAID_ENV)
if not PLAID_HOST:
    raise ValueError(f"Invalid PLAID_ENV: {PLAID_ENV}")

# Initialize Plaid client
configuration = Configuration(
    host=PLAID_HOST,
    api_key={
        "clientId": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET
    }
)
client = plaid_api.PlaidApi(ApiClient(configuration))

# Create sandbox public token
try:
    request = SandboxPublicTokenCreateRequest(
        institution_id="ins_109508",  # Sandbox bank
        initial_products=[Products("transactions")]
        # ✅ Removed country_codes
    )
    response = client.sandbox_public_token_create(request)
    public_token = response['public_token']
    print("Public Token:", public_token)
except Exception as e:
    print("Error generating public token:", e)
