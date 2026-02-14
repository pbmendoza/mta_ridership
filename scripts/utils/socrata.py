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
        get_soda_endpoint,
    )
"""

from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

import requests
from dotenv import load_dotenv as _load_dotenv

DEFAULT_REQUEST_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 5
RETRYABLE_STATUS_CODES: Set[int] = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_BASE_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_BACKOFF_JITTER_RATIO = 0.2
_dotenv_loaded = False
LOGGER = logging.getLogger(__name__)


class SocrataRequestError(RuntimeError):
    """Raised when a Socrata API request fails with explicit context."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str,
        status_code: Optional[int] = None,
        attempt: Optional[int] = None,
        max_retries: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> None:
        self.endpoint = endpoint
        self.status_code = status_code
        self.attempt = attempt
        self.max_retries = max_retries
        self.reason = reason

        parts = [f"endpoint={endpoint}"]
        if status_code is not None:
            parts.append(f"status_code={status_code}")
        if attempt is not None:
            parts.append(f"attempt={attempt}")
        if max_retries is not None:
            parts.append(f"max_retries={max_retries}")
        if reason:
            parts.append(f"reason={reason}")

        super().__init__(f"{message} ({', '.join(parts)})")


def repo_root() -> Path:
    """Find the project root by looking for the .git directory."""
    current = Path(__file__).resolve().parent
    for directory in [current, *current.parents]:
        if (directory / ".git").exists():
            return directory
    return current


def get_soda_endpoint(dataset_id: str) -> str:
    """Build SODA3 endpoint URL for a dataset."""
    return f"https://data.ny.gov/resource/{dataset_id}.json"


def _ensure_dotenv_loaded() -> None:
    """Load ``.env`` from the repo root (at most once per process)."""
    global _dotenv_loaded
    if not _dotenv_loaded:
        _load_dotenv(repo_root() / ".env", override=False)
        _dotenv_loaded = True


def load_socrata_token() -> str:
    """Return the Socrata app token.

    Resolution order (highest priority first):
    1. ``SOCRATA_APP_TOKEN`` environment variable (if already set).
    2. ``.env`` file at repository root (fills unset vars only).
    3. Empty string (anonymous / unauthenticated access).
    """
    _ensure_dotenv_loaded()
    return os.getenv("SOCRATA_APP_TOKEN", "")


def load_socrata_secret_token() -> str:
    """Return the Socrata secret token.

    Resolution order (highest priority first):
    1. ``SOCRATA_SECRET_TOKEN`` environment variable (if already set).
    2. ``.env`` file at repository root (fills unset vars only).
    3. Empty string (not sent).
    """
    _ensure_dotenv_loaded()
    return os.getenv("SOCRATA_SECRET_TOKEN", "")


def build_headers(
    app_token: Optional[str] = None,
    secret_token: Optional[str] = None,
) -> Dict[str, str]:
    """Build HTTP headers for Socrata SODA3 requests.

    Args:
        app_token: Socrata application token. When non-empty the
            ``X-App-Token`` header is set, raising API throttle limits.
        secret_token: Optional Socrata secret token. When non-empty the
            ``X-App-Token-Secret`` header is set.
    """
    headers: Dict[str, str] = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    if secret_token:
        headers["X-App-Token-Secret"] = secret_token
    return headers


def _compute_backoff_delay(
    *,
    attempt_index: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
    backoff_jitter_ratio: float,
) -> float:
    """Compute bounded exponential backoff with jitter."""
    base = min(max_backoff_seconds, base_backoff_seconds * (2 ** max(0, attempt_index - 1)))
    lower = max(0.0, base * (1 - backoff_jitter_ratio))
    upper = max(lower, base * (1 + backoff_jitter_ratio))
    return max(0.0, random.uniform(lower, upper))


def _parse_retry_after_seconds(value: Optional[str], *, max_backoff_seconds: float) -> Optional[float]:
    """Parse Retry-After header as seconds, returning None when invalid."""
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        seconds = int(raw)
    except ValueError:
        return None
    if seconds < 0:
        return None
    return min(float(seconds), max_backoff_seconds)


def _body_snippet(response: requests.Response, limit: int = 500) -> str:
    text = (response.text or "").strip().replace("\n", " ")
    if len(text) > limit:
        return text[:limit]
    return text


def request_json(
    session: requests.Session,
    endpoint: str,
    params: Mapping[str, Any],
    headers: Mapping[str, str],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
    base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    backoff_jitter_ratio: float = DEFAULT_BACKOFF_JITTER_RATIO,
    retryable_status_codes: Optional[Set[int]] = None,
) -> List[Dict[str, Any]]:
    """Perform a GET request with retry and exponential back-off.

    Retries on transient network exceptions and retryable HTTP statuses.
    Returns the parsed JSON list on success.

    Raises:
        SocrataRequestError: On retry exhaustion, non-retryable failures, bad JSON,
            unexpected payload shape, or Socrata ``not_found``.
    """
    if max_retries < 1:
        raise ValueError("max_retries must be >= 1")

    statuses = retryable_status_codes or RETRYABLE_STATUS_CODES
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            response = session.get(
                endpoint,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise SocrataRequestError(
                    "Request failed after retries",
                    endpoint=endpoint,
                    attempt=attempt,
                    max_retries=max_retries,
                    reason=f"{type(exc).__name__}: {exc}",
                ) from exc
            wait_seconds = _compute_backoff_delay(
                attempt_index=attempt,
                base_backoff_seconds=base_backoff_seconds,
                max_backoff_seconds=max_backoff_seconds,
                backoff_jitter_ratio=backoff_jitter_ratio,
            )
            LOGGER.warning(
                "Socrata request transport error, retrying in %.2fs "
                "(attempt %s/%s): endpoint=%s reason=%s",
                wait_seconds,
                attempt,
                max_retries,
                endpoint,
                f"{type(exc).__name__}: {exc}",
            )
            time.sleep(wait_seconds)
            continue

        status_code = response.status_code

        if status_code in statuses:
            if attempt >= max_retries:
                reason = f"HTTP {status_code} {response.reason}".strip()
                body = _body_snippet(response)
                if body:
                    reason = f"{reason}; body={body}"
                raise SocrataRequestError(
                    "Request failed after retries",
                    endpoint=endpoint,
                    status_code=status_code,
                    attempt=attempt,
                    max_retries=max_retries,
                    reason=reason,
                )

            retry_after = None
            if status_code == 429:
                retry_after = _parse_retry_after_seconds(
                    response.headers.get("Retry-After"),
                    max_backoff_seconds=max_backoff_seconds,
                )
            wait_seconds = (
                retry_after
                if retry_after is not None
                else _compute_backoff_delay(
                    attempt_index=attempt,
                    base_backoff_seconds=base_backoff_seconds,
                    max_backoff_seconds=max_backoff_seconds,
                    backoff_jitter_ratio=backoff_jitter_ratio,
                )
            )
            LOGGER.warning(
                "Socrata retryable response status=%s, retrying in %.2fs "
                "(attempt %s/%s): endpoint=%s",
                status_code,
                wait_seconds,
                attempt,
                max_retries,
                endpoint,
            )
            time.sleep(wait_seconds)
            continue

        if status_code >= 400:
            reason = f"HTTP {status_code} {response.reason}".strip()
            body = _body_snippet(response)
            if body:
                reason = f"{reason}; body={body}"
            raise SocrataRequestError(
                "Received non-retryable HTTP status",
                endpoint=endpoint,
                status_code=status_code,
                attempt=attempt,
                max_retries=max_retries,
                reason=reason,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            body = _body_snippet(response)
            reason = f"{type(exc).__name__}: {exc}"
            if body:
                reason = f"{reason}; body={body}"
            raise SocrataRequestError(
                "Failed to decode JSON response",
                endpoint=endpoint,
                status_code=status_code,
                attempt=attempt,
                max_retries=max_retries,
                reason=reason,
            ) from exc

        if isinstance(payload, dict) and payload.get("code") == "not_found":
            raise SocrataRequestError(
                "SODA3 endpoint reported not_found",
                endpoint=endpoint,
                status_code=status_code,
                attempt=attempt,
                max_retries=max_retries,
                reason=str(payload),
            )
        if not isinstance(payload, list):
            raise SocrataRequestError(
                "Unexpected payload type",
                endpoint=endpoint,
                status_code=status_code,
                attempt=attempt,
                max_retries=max_retries,
                reason=f"type={type(payload).__name__}",
            )
        return payload

    raise SocrataRequestError(
        "Request failed after retries",
        endpoint=endpoint,
        attempt=attempt,
        max_retries=max_retries,
        reason="Retry loop exhausted",
    )
