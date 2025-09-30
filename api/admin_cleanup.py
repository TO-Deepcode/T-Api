import json
from datetime import datetime, timezone, timedelta

from src.config import get_settings
from src.logging_setup import bind_request, get_logger
from src.security import (
    SignatureMismatch,
    SignatureMissing,
    build_cors_headers,
    extract_correlation_id,
    validate_origin,
    verify_signature,
)
from src.storage import get_storage

settings = get_settings()
storage = get_storage()
logger = get_logger(component="admin_cleanup")

PREFIXES = [
    "news/raw",
    "news/clustered",
    "market",
    "logs",
    "gpt/actions",
]


def handler(request):
    method = (request.get("method") or "POST").upper()
    headers = request.get("headers") or {}
    body = request.get("body") or ""

    origin = None
    try:
        origin = validate_origin(headers)
    except Exception as exc:
        return {
            "statusCode": 403,
            "headers": build_cors_headers(None),
            "body": json.dumps({"error": str(exc)}),
        }

    cors_headers = build_cors_headers(origin)

    if method == "OPTIONS":
        return {"statusCode": 204, "headers": cors_headers, "body": ""}

    if method != "POST":
        cors_headers["Content-Type"] = "application/json"
        return {
            "statusCode": 405,
            "headers": cors_headers,
            "body": json.dumps({"error": "method not allowed"}),
        }

    body_bytes = body.encode("utf-8")
    try:
        verify_signature(headers, body_bytes)
    except (SignatureMissing, SignatureMismatch) as exc:
        cors_headers["Content-Type"] = "application/json"
        return {"statusCode": 401, "headers": cors_headers, "body": json.dumps({"error": str(exc)})}

    correlation_id = extract_correlation_id(headers)
    log = bind_request(logger, correlation_id, "/api/admin_cleanup")

    now = datetime.now(timezone.utc)
    deleted = 0
    inspected = 0

    for prefix in PREFIXES:
        objects = storage.list(prefix, limit=1000)
        for obj in objects:
            inspected += 1
            metadata = storage.get_json(obj.key) or {}
            ttl_days = metadata.get("ttl_days", settings.storage_ttl_default_days)
            ttl = timedelta(days=ttl_days)
            age = now - obj.created_at
            if age > ttl:
                storage.delete(obj.key)
                deleted += 1
                log.info("admin_cleanup.deleted", key=obj.key, age_days=age.days)

    log.info("admin_cleanup.completed", inspected=inspected, deleted=deleted)
    cors_headers["Content-Type"] = "application/json"
    return {
        "statusCode": 200,
        "headers": cors_headers,
        "body": json.dumps({"inspected": inspected, "deleted": deleted}),
    }
