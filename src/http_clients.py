from functools import lru_cache
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx
from httpx import Timeout
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from src.config import get_settings
from src.logging_setup import get_logger
from src.rate_limit import RateLimitError, enforce_rate_limit

logger = get_logger(component="http_client")


class HTTPClientError(Exception):
    """Raised when upstream HTTP data cannot be decoded."""


@lru_cache()
def get_http_client() -> httpx.Client:
    settings = get_settings()
    timeout = Timeout(settings.request_timeout_seconds)
    proxies = settings.http_proxy or None
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "application/json, application/xml;q=0.9, */*;q=0.8",
    }
    return httpx.Client(timeout=timeout, headers=headers, proxies=proxies)


def _host_from_url(url: str) -> str:
    return urlparse(url).netloc or url


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, RateLimitError)),
)
def request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Any] = None,
    data: Optional[Any] = None,
    min_interval: float = 0.0,
) -> httpx.Response:
    client = get_http_client()
    if min_interval > 0:
        enforce_rate_limit(_host_from_url(url), min_interval=min_interval)
    response = client.request(
        method,
        url,
        headers=headers,
        params=params,
        json=json,
        data=data,
    )
    response.raise_for_status()
    return response


def get_json(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    min_interval: float = 0.0,
) -> Any:
    response = request("GET", url, headers=headers, params=params, min_interval=min_interval)
    try:
        return response.json()
    except ValueError as exc:
        raise HTTPClientError(f"failed to decode JSON from {url}") from exc


def get_text(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    min_interval: float = 0.0,
) -> str:
    response = request("GET", url, headers=headers, params=params, min_interval=min_interval)
    return response.text
