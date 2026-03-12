"""
config.py
---------
Loads all environment variables required by the application.

Priority order:
  1. Streamlit secrets (st.secrets)  — used on Streamlit Cloud
  2. .env file / OS environment      — used for local development

All sensitive configuration is sourced from environment variables or
Streamlit secrets — never hardcoded.
"""

import json
import os

from dotenv import load_dotenv

# Load variables from .env file if present (local development only)
load_dotenv()


def _secrets_toml_exists() -> bool:
    """Return True if a Streamlit secrets.toml file exists in a known location."""
    import pathlib
    candidates = [
        # Global user secrets
        pathlib.Path.home() / ".streamlit" / "secrets.toml",
        # Project-level secrets
        pathlib.Path(__file__).resolve().parent /
        ".streamlit" / "secrets.toml",
    ]
    return any(p.exists() for p in candidates)


def _get_value(key: str, default: str = "") -> str:
    """
    Retrieve a configuration value.

    Priority:
      1. OS environment variable / .env file  — local development
      2. st.secrets                            — Streamlit Cloud
      3. default value

    st.secrets is only accessed when a secrets.toml file is present,
    preventing Streamlit from showing 'No secrets found' warnings
    during local development.
    """
    # 1. OS environment variables / .env file (fastest, no Streamlit overhead)
    env_val = os.getenv(key, "")
    if env_val:
        return env_val

    # 2. Streamlit secrets — only if secrets.toml actually exists on disk
    if _secrets_toml_exists():
        try:
            import streamlit as st
            if key in st.secrets:
                value = st.secrets[key]
                # st.secrets may return a dict for nested TOML; convert to JSON string
                if isinstance(value, dict):
                    return json.dumps(dict(value))
                return str(value)
        except Exception:
            pass  # Streamlit not running or key not found

    return default


def get_config() -> dict:
    """Return a dictionary of all required configuration values."""
    config = {
        "APIFY_API_TOKEN": _get_value("APIFY_API_TOKEN"),
        "TASK_US": _get_value("TASK_US"),
        "TASK_CANADA": _get_value("TASK_CANADA"),
        "GOOGLE_SHEET_ID": _get_value("GOOGLE_SHEET_ID"),
        "GOOGLE_SERVICE_ACCOUNT_JSON": _get_value(
            "GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json"
        ),
    }

    missing = [k for k, v in config.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    return config
