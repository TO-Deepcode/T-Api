from datetime import datetime, timezone
from typing import Dict, List

import feedparser
from dateutil import parser as date_parser

from src.http_clients import get_text
from src.normalization import ensure_utc, normalize_text


def fetch_feed(url: str, *, min_interval: float, logger) -> List[Dict]:
    text = get_text(url, min_interval=min_interval)
    feed = feedparser.parse(text)
    entries: List[Dict] = []
    for entry in feed.entries:
        published = entry.get("published") or entry.get("updated")
        published_at = ensure_utc(
            date_parser.parse(published)
        ) if published else datetime.now(timezone.utc)
        entries.append(
            {
                "id": entry.get("id") or entry.get("guid") or entry.get("link"),
                "title": normalize_text(entry.get("title", "")),
                "summary": normalize_text(entry.get("summary", "")),
                "url": entry.get("link"),
                "published_at": published_at,
            }
        )
    logger.info("rss.fetch_complete", url=url, count=len(entries))
    return entries


def fetch_html_listing(url: str, *, min_interval: float, logger) -> List[Dict]:
    logger.info("rss.html_listing_fallback", url=url)
    return []
