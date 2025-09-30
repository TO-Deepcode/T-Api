import re
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(value: str) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip()


def normalize_title(value: str) -> str:
    return normalize_text(value).lower()


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parsed.path or "/"
    query = parsed.query
    return urlunparse((scheme, netloc, path, "", query, ""))


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
