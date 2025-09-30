import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from rapidfuzz import fuzz

from src.normalization import ensure_utc, normalize_title
from src.schemas import NewsCluster, NewsItem

SOURCE_WEIGHTS: Dict[str, float] = {
    "coindesk": 1.0,
    "theblock": 1.0,
    "blockworks": 1.0,
    "cryptopanic": 2.0,
    "messari": 1.0,
}

ENTITY_PATTERNS = [
    r"\bBTC\b",
    r"\bETH\b",
    r"\bSOL\b",
    r"\bXRP\b",
    r"\bSEC\b",
    r"\bETF\b",
    r"\bhack\b",
    r"\bfunding rate\b",
    r"\blisting\b",
]


def extract_entities(text: str) -> List[str]:
    entities = set()
    lowered = text.lower()
    for pattern in ENTITY_PATTERNS:
        if re.search(pattern, lowered, re.IGNORECASE):
            entities.add(pattern.strip(r"\b").upper())
    return sorted(entities)


def score_cluster(items: List[NewsItem], similarity: float) -> float:
    freshness = 0.0
    if items:
        latest = max(item.published_at for item in items)
        now = ensure_utc(datetime.now(timezone.utc))
        delta_hours = max(1, (now - ensure_utc(latest)).total_seconds() / 3600)
        freshness = max(0.0, 1.0 - (delta_hours / 24.0))
    sources = {item.source for item in items}
    source_weight = sum(SOURCE_WEIGHTS.get(source, 0.5) for source in sources)
    return round(0.6 * similarity + 0.3 * source_weight + 0.1 * freshness, 4)


def cluster_news_items(items: List[NewsItem], window_minutes: int, similarity_threshold: float) -> List[NewsCluster]:
    window = timedelta(minutes=window_minutes)
    sorted_items = sorted(items, key=lambda i: i.published_at)
    clusters: List[Dict] = []

    for item in sorted_items:
        item_title = normalize_title(item.title)
        matched_cluster = None
        for cluster in clusters:
            last_item = cluster["items"][-1]
            if abs((ensure_utc(item.published_at) - ensure_utc(last_item.published_at))) > window:
                continue
            similarity = fuzz.token_sort_ratio(
                normalize_title(last_item.title),
                item_title,
            ) / 100.0
            if similarity >= similarity_threshold:
                matched_cluster = cluster
                break
        if matched_cluster:
            matched_cluster["items"].append(item)
        else:
            clusters.append({"items": [item]})

    results: List[NewsCluster] = []
    for cluster in clusters:
        cluster_items = cluster["items"]
        similarity = 0.0
        if len(cluster_items) > 1:
            base = normalize_title(cluster_items[0].title)
            similarity = max(
                fuzz.token_sort_ratio(base, normalize_title(item.title)) / 100.0
                for item in cluster_items[1:]
            )
        entities = extract_entities(" ".join(item.title for item in cluster_items))
        cluster_id = str(uuid.uuid4())
        first_seen = min(item.published_at for item in cluster_items)
        last_seen = max(item.published_at for item in cluster_items)
        score = score_cluster(cluster_items, similarity)
        results.append(
            NewsCluster(
                cluster_id=cluster_id,
                canonical_title=cluster_items[0].title,
                summary=cluster_items[0].summary or cluster_items[0].title,
                score=score,
                source_count=len({item.source for item in cluster_items}),
                entities=entities,
                sentiment_hint=None,
                first_seen=first_seen,
                last_seen=last_seen,
                links=[
                    {"source": item.source, "url": item.url, "title": item.title}
                    for item in cluster_items
                ],
            )
        )

    return results
