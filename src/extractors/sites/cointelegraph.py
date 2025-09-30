from typing import Dict

from bs4 import BeautifulSoup

from src.extractors.html import guess_time


def adapter(soup: BeautifulSoup) -> Dict[str, str]:
    body = []
    article = soup.find("article")
    if article:
        for paragraph in article.select("div.post-content p"):
            text = paragraph.get_text(" ", strip=True)
            if text:
                body.append(text)
    content = "\n".join(body)
    summary = soup.find("meta", attrs={"name": "description"})
    title = soup.find("meta", property="og:title")
    return {
        "content_text": content,
        "summary": summary["content"] if summary and summary.get("content") else "",
        "title": title["content"] if title and title.get("content") else "",
        "published_at": guess_time(soup),
    }
