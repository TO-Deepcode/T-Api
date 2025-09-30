import json

from src.logging_setup import bind_request, get_logger
from src.schemas import StoreListResponse
from src.security import build_cors_headers, extract_correlation_id, validate_origin
from src.storage import get_storage

storage = get_storage()
logger = get_logger(component="store_list")


def handler(request):
    method = (request.get("method") or "GET").upper()
    headers = request.get("headers") or {}

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

    if method != "GET":
        cors_headers["Content-Type"] = "application/json"
        return {
            "statusCode": 405,
            "headers": cors_headers,
            "body": json.dumps({"error": "method not allowed"}),
        }

    query = request.get("query") or request.get("queryStringParameters") or {}
    prefix = query.get("prefix", "")
    limit = int(query.get("limit", 100))

    correlation_id = extract_correlation_id(headers)
    log = bind_request(logger, correlation_id, "/api/store_list")

    objects = storage.list(prefix, limit)
    response = StoreListResponse.from_objects(objects)
    log.info("store_list.completed", prefix=prefix, count=len(response.objects))

    cors_headers["Content-Type"] = "application/json"
    return {"statusCode": 200, "headers": cors_headers, "body": response.json()}
