"""Shared Socrata (SODA3) API helpers for MTA ridership scripts.

Provides centralized token loading, header building, and HTTP request
logic with retry/backoff so that individual scripts don't duplicate this
infrastructure.

Usage from any script in the repo::

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[N]))  # repo root
    from scripts.utils.socrata import (
        repo_root,
        load_socrata_token,
        build_headers,
        request_json,
    )
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:  # python-dotenv is optional at runtime
    _load_dotenv = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_REQUEST_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 5


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def repo_root() -> Path:
    """Return the repository root directory.

    Assumes this file lives at ``<repo>/scripts/utils/socrata.py``.
    """
    return Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Token loading
# ---------------------------------------------------------------------------

def _ensure_dotenv_loaded() -> None:
    """Load ``.env`` from the repo root if *python-dotenv* is available."""
    if _load_dotenv is not None:
        _load_dotenv(repo_root() / ".env", override=False)


def load_socrata_token() -> str:
    """Return the Socrata app token.

    Resolution order:
    1. ``.env`` file at repository root (via *python-dotenv*).
    2. ``SOCRATA_APP_TOKEN`` environment variable.
    3. Empty string (anonymous / unauthenticated access).
    """
    _ensure_dotenv_loaded()
    return os.getenv("SOCRATA_APP_TOKEN", "")


def load_socrata_secret_token() -> str:
    """Return the Socrata secret token.

    Resolution order:
    1. ``.env`` file at repository root (via *python-dotenv*).
    2. ``SOCRATA_SECRET_TOKEN`` environment variable.
    3. Empty string (not sent).
    """
    _ensure_dotenv_loaded()
    return os.getenv("SOCRATA_SECRET_TOKEN", "")


# ---------------------------------------------------------------------------
# Header building
# ---------------------------------------------------------------------------

def build_headers(
    app_token: Optional[str] = None,
    secret_token: Optional[str] = None,
) -> Dict[str, str]:
    """Build HTTP headers for Socrata SODA3 requests.

    Parameters
    ----------
    app_token:
        Socrata application token.  When non-empty the ``X-App-Token``
        header is set, which raises API throttle limits.
    secret_token:
        Optional Socrata secret token.  When non-empty the
        ``X-App-Token-Secret`` header is set.
    """
    headers: Dict[str, str] = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    if secret_token:
        headers["X-App-Token-Secret"] = secret_token
    return headers


# ---------------------------------------------------------------------------
# HTTP request with retry
# ---------------------------------------------------------------------------

def request_json(
    session: requests.Session,
    endpoint: str,
    params: Dict[str, str],
    headers: Dict[str, str],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> List[Dict[str, str]]:
    """Perform a GET request with retry and exponential back-off.

    Retries on:
    - ``requests.RequestException`` (network errors)
    - HTTP 429 (rate-limited)

    Returns the parsed JSON list on success.

    Raises
    ------
    RuntimeError
        After *max_retries* consecutive failures, or if the response
        payload is not a JSON list, or if Socrata reports ``not_found``.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = session.get(
                endpoint,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            attempt += 1
            time.sleep(min(2 ** attempt, 60))
            if attempt >= max_retries:
                raise RuntimeError("Request failed after retries") from exc
            continue

        if response.status_code == 429:
            attempt += 1
            wait = min(2 ** attempt, 60)
            print(
                f"⚠️  Rate-limited by Socrata API (HTTP 429). "
                f"Retrying in {wait}s (attempt {attempt}/{max_retries})... "
                f"Tip: set SOCRATA_APP_TOKEN in .env for higher limits "
                f"(see references/docs/socrata_api_setup.md)."
            )
            time.sleep(wait)
            continue

        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") == "not_found":
            raise RuntimeError(f"SODA3 endpoint reported not_found: {payload}")
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected payload type: {type(payload)!r}")
        return payload

    raise RuntimeError(f"Request failed after {max_retries} retries.")
