import json
import os
from datetime import datetime, timezone

from src.config import get_settings
from src.logging_setup import bind_request, get_logger
from src.schemas import HealthResponse
from src.security import build_cors_headers, extract_correlation_id, validate_origin

settings = get_settings()
logger = get_logger(component="health")


def handler(request):
    method = (request.get("method") or "GET").upper()
    headers = request.get("headers") or {}
    origin = None
    try:
        origin = validate_origin(headers)
    except Exception as exc:
        return {
            "statusCode": 403,
            "headers": build_cors_headers(origin=None),
            "body": json.dumps({"error": str(exc)}),
        }

    cors_headers = build_cors_headers(origin)

    if method == "OPTIONS":
        return {"statusCode": 204, "headers": cors_headers, "body": ""}

    correlation_id = extract_correlation_id(headers)
    log = bind_request(logger, correlation_id, "/api/health")
    now = datetime.now(timezone.utc)
    response = HealthResponse(
        status="ok",
        time=now,
        version=os.environ.get("VERCEL_GIT_COMMIT_SHA", "dev"),
    )
    log.info("health.ok", timestamp=response.time, version=response.version)
    cors_headers["Content-Type"] = "application/json"
    return {"statusCode": 200, "headers": cors_headers, "body": response.json()}
