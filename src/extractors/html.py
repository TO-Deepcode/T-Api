from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from src.http_clients import get_text

# Compliance banner: only parse publicly accessible HTML and obey robots.txt elsewhere.


def extract_article(
    url: str,
    *,
    min_interval: float,
    adapter: Optional[Callable[[BeautifulSoup], Dict[str, str]]] = None,
    logger,
) -> Dict[str, str]:
    html = get_text(url, min_interval=min_interval, headers={"Accept": "text/html"})
    soup = BeautifulSoup(html, "html.parser")
    if adapter:
        data = adapter(soup)
    else:
        data = generic_adapter(soup)
    data.setdefault("content_text", "")
    data["language"] = detect_language(soup)
    if "published_at" not in data or not data["published_at"]:
        data["published_at"] = guess_time(soup)
    logger.info("html.extract_ok", url=url, content_length=len(data["content_text"]))
    return data


def generic_adapter(soup: BeautifulSoup) -> Dict[str, str]:
    article = soup.find("article") or soup.find("main") or soup.body
    parts = [
        element.get_text(" ", strip=True)
        for element in article.find_all(["p", "li"])
        if element.get_text(strip=True)
    ] if article else []
    content = "\n".join(parts)
    summary = soup.find("meta", attrs={"name": "description"})
    published = guess_time(soup)
    title = soup.find("meta", property="og:title")
    return {
        "content_text": content,
        "summary": summary["content"] if summary and summary.get("content") else "",
        "title": title["content"] if title and title.get("content") else "",
        "published_at": published,
    }


def guess_time(soup: BeautifulSoup):
    for selector in [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "date"}),
        ("time", {}),
    ]:
        node = soup.find(*selector)
        if not node:
            continue
        value = node.get("content") or node.get("datetime") or node.get_text(strip=True)
        if value:
            try:
                dt = date_parser.parse(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError):
                continue
    return datetime.now(timezone.utc)


def detect_language(soup: BeautifulSoup) -> str:
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        return html_tag["lang"][:2]
    return "en"
