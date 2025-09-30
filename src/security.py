import hashlib
import hmac
import uuid
from typing import Dict, Optional

from src.config import get_settings


class SecurityError(Exception):
    """Raised when a request fails security validation."""


class SignatureMissing(SecurityError):
    pass


class SignatureMismatch(SecurityError):
    pass


class OriginNotAllowed(SecurityError):
    pass


def extract_correlation_id(headers: Dict[str, str]) -> str:
    correlation_id = (
        headers.get("x-correlation-id")
        or headers.get("X-Correlation-Id")
        or headers.get("x-correlationid")
    )
    if correlation_id:
        return correlation_id
    return str(uuid.uuid4())


def verify_signature(headers: Dict[str, str], body: bytes) -> None:
    signature = headers.get("x-signature") or headers.get("X-Signature")
    if not signature:
        raise SignatureMissing("missing X-Signature header")
    secret = get_settings().hmac_shared_secret.encode("utf-8")
    expected = hmac.new(secret, body or b"", hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise SignatureMismatch("signature mismatch")


def validate_origin(headers: Dict[str, str]) -> Optional[str]:
    origin = headers.get("origin") or headers.get("Origin")
    allowed = get_settings().allowed_origins
    if not allowed:
        return origin
    if origin in allowed or origin is None:
        return origin
    raise OriginNotAllowed(f"origin {origin} is not allowed")


def build_cors_headers(origin: Optional[str]) -> Dict[str, str]:
    headers = {
        "Access-Control-Allow-Headers": "Content-Type,X-Signature,X-Correlation-Id",
        "Access-Control-Allow-Methods": "OPTIONS,GET,POST",
    }
    if origin:
        headers["Access-Control-Allow-Origin"] = origin
    else:
        allowed = get_settings().allowed_origins
        if allowed:
            headers["Access-Control-Allow-Origin"] = allowed[0]
    return headers
