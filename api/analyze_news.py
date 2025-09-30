import json
from datetime import datetime, timezone

import ujson
from pydantic import ValidationError

from src.confirm import cluster_news_items
from src.config import get_settings
from src.logging_setup import bind_request, get_logger
from src.schemas import AnalyzeNewsRequest, AnalyzeNewsResponse
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
logger = get_logger(component="analyze_news")


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
    log = bind_request(logger, correlation_id, "/api/analyze_news")

    try:
        req = AnalyzeNewsRequest.parse_raw(body_bytes)
    except ValidationError as exc:
        cors_headers["Content-Type"] = "application/json"
        return {"statusCode": 422, "headers": cors_headers, "body": exc.json()}

    log.info(
        "analyze_news.received",
        items=len(req.items),
        window=req.confirm_window_minutes,
        threshold=req.similarity_threshold,
    )

    clusters = cluster_news_items(req.items, req.confirm_window_minutes, req.similarity_threshold)
    response = AnalyzeNewsResponse(clusters=clusters)

    persist_clusters(clusters)
    persist_analysis_payload(response, correlation_id)

    log.info("analyze_news.completed", clusters=len(clusters))
    cors_headers["Content-Type"] = "application/json"
    return {"statusCode": 200, "headers": cors_headers, "body": response.json()}


def persist_clusters(clusters):
    now = datetime.now(timezone.utc)
    for cluster in clusters:
        key = f"news/clustered/{now.strftime('%Y%m%d')}/{cluster.cluster_id}.json"
        payload = ujson.loads(cluster.json())
        payload.update(
            {
                "schema_version": 1,
                "created_at": now.isoformat(),
                "ttl_days": settings.storage_ttl_default_days,
            }
        )
        storage.put_json(key, payload)


def persist_analysis_payload(response, correlation_id):
    now = datetime.now(timezone.utc)
    payload = ujson.loads(response.json())
    payload.update({"created_at": now.isoformat(), "ttl_days": settings.storage_ttl_default_days})
    storage.put_json(f"logs/{now.date()}/{correlation_id}-analyze_news.json", payload)
    storage.put_json(
        f"gpt/actions/{now.date()}/{correlation_id}-analyze_news.json",
        {
            "schema_version": 1,
            "endpoint": "analyze_news",
            "created_at": now.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
            "payload": ujson.loads(response.json()),
        },
    )
