import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ujson
from pydantic import ValidationError

from src.config import get_settings
from src.dedupe import dedupe_items, compute_hash
from src.extractors import rss
from src.extractors.html import extract_article
from src.extractors.sites import SITE_EXTRACTORS
from src.logging_setup import bind_request, get_logger
from src.normalization import canonicalize_url, ensure_utc, normalize_text
from src.rate_limit import RateLimitError
from src.schemas import NewsFetchRequest, NewsFetchResponse, NewsItem
from src.security import (
    SignatureMismatch,
    SignatureMissing,
    build_cors_headers,
    extract_correlation_id,
    validate_origin,
    verify_signature,
)
from src.storage import get_storage

# Legal compliance banner: only public pages, respect robots.txt, honour site ToS.
# Collector prioritises RSS, validates robots, and applies gentle rate limits.

settings = get_settings()
storage = get_storage()
logger = get_logger(component="news_fetch")

SOURCE_CATALOG: Dict[str, Dict[str, Optional[str]]] = {
    "coindesk": {
        "rss": "https://www.coindesk.com/arc/outboundfeeds/rss/?output=xml",
        "site": "https://www.coindesk.com",
    },
    "theblock": {
        "rss": "https://www.theblock.co/rss.xml",
        "site": "https://www.theblock.co",
    },
    "blockworks": {"rss": "https://blockworks.co/feed", "site": "https://blockworks.co"},
    "cointelegraph": {"rss": "https://cointelegraph.com/rss", "site": "https://cointelegraph.com"},
    "defiant": {"rss": "https://thedefiant.io/feed", "site": "https://thedefiant.io"},
    "dlnews": {"rss": "https://dlnews.com/feed", "site": "https://dlnews.com"},
    "protos": {"rss": "https://protos.com/feed", "site": "https://protos.com"},
    "decrypt": {"rss": "https://decrypt.co/feed", "site": "https://decrypt.co"},
    "cryptopanic": {"rss": None, "site": "https://cryptopanic.com/news/"},
    "messari": {"rss": None, "site": "https://messari.io/news"},
    "glassnode": {"rss": "https://insights.glassnode.com/rss/", "site": "https://insights.glassnode.com"},
}

MIN_INTERVAL_SECONDS = 2.0


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
    log = bind_request(logger, correlation_id, "/api/news_fetch")

    try:
        req = NewsFetchRequest.parse_raw(body_bytes)
    except ValidationError as exc:
        cors_headers["Content-Type"] = "application/json"
        return {"statusCode": 422, "headers": cors_headers, "body": exc.json()}

    since = req.since or ensure_utc(datetime.now(timezone.utc) - req.default_window())
    log.info("news_fetch.received", sources=req.sources, since=since.isoformat(), max=req.max_per_source)

    items: List[NewsItem] = []

    for source in req.sources:
        if source not in SOURCE_CATALOG:
            log.warning("news_fetch.unknown_source", source=source)
            continue
        try:
            items.extend(collect_source_news(source, req, since, log))
        except Exception as exc:
            log.warning("news_fetch.source_failed", source=source, error=str(exc))

    deduped = dedupe_items(items, threshold=0.9)
    response = NewsFetchResponse(items=deduped)
    persist_news_payload(response, correlation_id)
    log.info("news_fetch.completed", count=len(deduped))
    cors_headers["Content-Type"] = "application/json"
    return {"statusCode": 200, "headers": cors_headers, "body": response.json()}


def collect_source_news(source: str, req: NewsFetchRequest, since: datetime, log):
    catalog = SOURCE_CATALOG[source]
    limit = min(req.max_per_source, settings.max_items_per_source)
    entries = []
    if catalog.get("rss"):
        entries = rss.fetch_feed(catalog["rss"], min_interval=MIN_INTERVAL_SECONDS, logger=log)
    if not entries and catalog.get("site"):
        entries = rss.fetch_html_listing(catalog["site"], min_interval=MIN_INTERVAL_SECONDS, logger=log)

    collected: List[NewsItem] = []
    for entry in entries:
        if entry["published_at"] and ensure_utc(entry["published_at"]) < since:
            continue
        try:
            item = hydrate_entry(source, entry, log)
        except RateLimitError:
            log.info("news_fetch.rate_limited", source=source)
            continue
        except Exception as exc:
            log.warning("news_fetch.entry_failed", source=source, error=str(exc), url=entry.get("url"))
            continue
        collected.append(item)
        if len(collected) >= limit:
            break

    return collected


def hydrate_entry(source: str, entry: Dict, log) -> NewsItem:
    url = canonicalize_url(entry["url"])
    fetched_at = ensure_utc(datetime.now(timezone.utc))
    adapter = SITE_EXTRACTORS.get(source)

    article = extract_article(
        url,
        min_interval=MIN_INTERVAL_SECONDS,
        adapter=adapter,
        logger=log,
    )
    content = article.get("content_text") or entry.get("summary") or ""
    content_norm = normalize_text(content)
    news_id = entry.get("id") or str(uuid.uuid4())
    news_hash = compute_hash(entry.get("title", ""), content_norm)
    news_item = NewsItem(
        id=str(news_id),
        source=source,
        url=url,
        title=entry.get("title", ""),
        summary=entry.get("summary"),
        published_at=ensure_utc(entry.get("published_at") or fetched_at),
        fetched_at=fetched_at,
        content_text=content_norm,
        language=article.get("language", "en"),
        hash=news_hash,
        score_hint=entry.get("score_hint"),
    )
    persist_news_item(news_item)
    return news_item


def persist_news_item(item: NewsItem):
    fetched_date = item.fetched_at.strftime("%Y%m%d")
    storage_key = f"news/raw/{item.source}/{fetched_date}/{item.id}.json"
    payload = ujson.loads(item.json())
    payload.update(
        {
            "created_at": item.fetched_at.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
            "schema_version": 1,
        }
    )
    storage.put_json(storage_key, payload)


def persist_news_payload(response: NewsFetchResponse, correlation_id: str):
    now = datetime.now(timezone.utc)
    payload = ujson.loads(response.json())
    payload.update({"created_at": now.isoformat(), "ttl_days": settings.storage_ttl_default_days})
    storage.put_json(f"logs/{now.date()}/{correlation_id}-news_fetch.json", payload)
    storage.put_json(
        f"gpt/actions/{now.date()}/{correlation_id}-news_fetch.json",
        {
            "schema_version": 1,
            "endpoint": "news_fetch",
            "created_at": now.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
            "payload": ujson.loads(response.json()),
        },
    )
