from typing import Dict, Any, Optional
import os
import json

try:
    import toml
except Exception:
    toml = None

# Attempt to import streamlit only when needed so configs can be imported in non-streamlit contexts
try:
    import streamlit as st
except Exception:
    st = None


def _load_from_st_secrets() -> Optional[Dict[str, Any]]:
    """Return a dict from st.secrets if available and non-empty."""
    if st is None:
        return None
    try:
        # st.secrets acts like a dict; convert to plain dict recursively
        def _to_native(obj):
            if hasattr(obj, "items"):
                return {k: _to_native(v) for k, v in obj.items()}
            return obj
            
        secrets = _to_native(st.secrets)
        # If empty, return None
        if not secrets:
            return None
        return secrets
    except Exception:
        return None


def _load_from_file(path: str = "secrets.toml") -> Optional[Dict[str, Any]]:
    """Load secrets from a local TOML file. Returns None if not possible."""
    if toml is None:
        return None
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = toml.load(f)
        return data
    except Exception:
        return None


# Cache loaded secrets in module scope
_secrets_cache: Optional[Dict[str, Any]] = None


def load_secrets() -> Dict[str, Any]:
    """Load secrets from st.secrets or local file; return empty dict if none found."""
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    # 1) Try streamlit secrets
    secrets = _load_from_st_secrets()
    if secrets:
        _secrets_cache = secrets
        return _secrets_cache

    # 2) Try local file (check both root and .streamlit)
    secrets = _load_from_file("secrets.toml")
    if not secrets:
        secrets = _load_from_file(".streamlit/secrets.toml")
        
    if secrets:
        _secrets_cache = secrets
        return _secrets_cache

    # 3) Fallback: minimal default
    _secrets_cache = {}
    return _secrets_cache


def get_r2_config() -> Dict[str, str]:
    """
    Returns the cloudflare_r2 table as a dict.
    Keys expected: account_id, endpoint_url, access_key_id, secret_access_key
    """
    secrets = load_secrets()
    r2 = secrets.get("cloudflare_r2") or secrets.get("cloudflare_r2".lower()) or {}
    # Normalize key names (support both variants)
    normalized = {
        "account_id": r2.get("account_id") or r2.get("accountId") or r2.get("account-id"),
        "endpoint_url": r2.get("endpoint_url") or r2.get("endpointUrl") or r2.get("endpoint"),
        "access_key_id": r2.get("access_key_id") or r2.get("accessKeyId") or r2.get("access_key"),
        "secret_access_key": r2.get("secret_access_key") or r2.get("secretAccessKey") or r2.get("secret_key"),
        "public_base_url": r2.get("public_base_url") or r2.get("publicBaseUrl") or None
    }
    return {k: v for k, v in normalized.items() if v is not None}


def get_users_db() -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict of users from the secrets file:
      { username: { password: str, r2_bucket: str, sheet_id?: str, dashboard_url?: str } }
    """
    secrets = load_secrets()
    users = secrets.get("users") or secrets.get("Users") or {}

    # If there's a 'users' table (toml) keys are already nested.
    if isinstance(users, dict) and users and any("password" in v for v in users.values()):
        return users

    # Try to assemble users from keys like "users.username" in top-level secrets (less common in TOML)
    assembled = {}
    
    # Final fallback: try to find any table-like entries that look like users
    for key, val in (secrets.items() if isinstance(secrets, dict) else []):
        if isinstance(val, dict) and "password" in val and "r2_bucket" in val:
            assembled[key] = val

    return assembled


def get_user_config(username: str) -> Optional[Dict[str, Any]]:
    """Return single user's config dict or None if not found."""
    users = get_users_db()
    return users.get(username)

def get_gcp_service_account() -> Optional[Dict[str, str]]:
    """
    Returns the google_service_account dictionary for BigQuery/Sheets auth.
    """
    secrets = load_secrets()
    # Support multiple common key names
    gcp_sa = secrets.get("gcp_service_account") or \
             secrets.get("google_service_account") or \
             secrets.get("GCP_SERVICE_ACCOUNT")
    
    if isinstance(gcp_sa, dict) and gcp_sa.get("private_key"):
        return gcp_sa
    return None

# The additional GCP client functions from your second snippet are not needed in configs.py, 
# as app.py handles client initialization directly using the dict returned by get_gcp_service_account().