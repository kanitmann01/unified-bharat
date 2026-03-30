"""HTTP client for NDAP Open API GET requests."""

from __future__ import annotations

from typing import Any

import httpx

DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=30.0)


def fetch_openapi_json(
    base_url: str,
    params: dict[str, Any],
    *,
    timeout: httpx.Timeout | None = None,
    max_retries: int = 3,
) -> Any:
    """
    Perform a GET to the NDAP openapi endpoint and return parsed JSON.

    String and numeric param values are coerced to strings for the query string.
    Retries transient server errors (5xx) up to max_retries times.
    """
    t = timeout or DEFAULT_TIMEOUT
    str_params: dict[str, str] = {}
    for k, v in params.items():
        if v is None:
            continue
        str_params[str(k)] = str(v)

    with httpx.Client(timeout=t, follow_redirects=True) as client:
        for attempt in range(max_retries):
            try:
                response = client.get(base_url, params=str_params)
                if response.status_code >= 500 and attempt + 1 < max_retries:
                    continue
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if (
                    e.response is not None
                    and e.response.status_code >= 500
                    and attempt + 1 < max_retries
                ):
                    continue
                raise
            except httpx.RequestError:
                if attempt + 1 < max_retries:
                    continue
                raise
