import os
from dotenv import load_dotenv


def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

load_dotenv()

# Postgres connection string
# Example: postgresql://user:pass@host:5432/pima?sslmode=require
PG_DSN = _req("PG_DSN")

# The existing GCP service endpoint that already handles sending to CommCare
# (equivalent to Label.GCP_Production_Inbox_URL in Salesforce)
GCP_PROD_INBOX_URL = _req("GCP_PROD_INBOX_URL")

# Optional bearer token if your inbox requires auth
GCP_AUTH_TOKEN = os.getenv("GCP_AUTH_TOKEN")

# HTTP
REQUEST_TIMEOUT_SECS = float(os.getenv("REQUEST_TIMEOUT_SECS", "20"))
