from typing import Dict

from bs4 import BeautifulSoup

from src.extractors.html import guess_time


def adapter(soup: BeautifulSoup) -> Dict[str, str]:
    article = soup.select_one("article")
    body = []
    if article:
        for paragraph in article.select("div.article-desc p, div.article-content p"):
            text = paragraph.get_text(" ", strip=True)
            if text:
                body.append(text)
    content = "\n".join(body)
    summary = (
        soup.select_one("meta[name='description']")
        or soup.select_one("meta[property='og:description']")
    )
    title = soup.select_one("meta[property='og:title']")
    return {
        "content_text": content,
        "summary": summary["content"] if summary and summary.get("content") else "",
        "title": title["content"] if title and title.get("content") else "",
        "published_at": guess_time(soup),
    }
