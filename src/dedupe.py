import hashlib
from typing import List

from rapidfuzz import fuzz

from src.normalization import canonicalize_url, normalize_title
from src.schemas import NewsItem


def compute_hash(title: str, content: str) -> str:
    normalized = (normalize_title(title) + "::" + content).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def dedupe_items(items: List[NewsItem], threshold: float) -> List[NewsItem]:
    result: List[NewsItem] = []
    for item in items:
        duplicate = False
        for existing in result:
            if existing.hash == item.hash or canonicalize_url(existing.url) == canonicalize_url(item.url):
                duplicate = True
                break
            score = fuzz.token_sort_ratio(
                normalize_title(existing.title), normalize_title(item.title)
            ) / 100.0
            if score >= threshold:
                duplicate = True
                break
        if not duplicate:
            result.append(item)
    return result
