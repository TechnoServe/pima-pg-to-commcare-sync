from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

import requests

from app.config import GCP_PROD_INBOX_URL, GCP_AUTH_TOKEN, REQUEST_TIMEOUT_SECS


def post_to_gcp_inbox(payload: Dict[str, Any], *, auth_token: Optional[str] = None) -> requests.Response:
    # Salesforce JSONUtils injects an 'id' at top-level.
    if "id" not in payload:
        payload["id"] = str(uuid.uuid4())

    headers = {"content-type": "application/json"}
    token = auth_token if auth_token is not None else GCP_AUTH_TOKEN
    if token:
        headers["authorization"] = f"Bearer {token}"

    print(f"Posting to GCP payload: {payload}")
    print("*" * 50)

    return requests.post(
        GCP_PROD_INBOX_URL,
        json=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECS,
    )
