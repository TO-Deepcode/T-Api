import json
import os
from datetime import datetime, timezone

import pytest

from api import health, market_fetch, news_fetch
from src.schemas import MarketSnapshot, NewsItem

os.environ.setdefault("HMAC_SHARED_SECRET", "testsecret")
os.environ.setdefault("ALLOWED_ORIGINS", "https://chat.openai.com")


def signed_request(body: str):
    import hashlib
    import hmac

    signature = hmac.new(b"testsecret", body.encode("utf-8"), hashlib.sha256).hexdigest()
    return signature


def test_health_handler():
    request = {"method": "GET", "headers": {}}
    response = health.handler(request)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["status"] == "ok"
    assert "time" in payload


def test_market_fetch_handler(monkeypatch):
    body = json.dumps(
        {
            "exchanges": ["binance"],
            "symbols": ["BTCUSDT"],
            "granularity": "1h",
            "limit": 2,
        }
    )
    headers = {"X-Signature": signed_request(body)}

    def fake_fetch_binance(symbols, granularity, limit, fetched_at, log):
        snapshot = MarketSnapshot(
            source="binance",
            symbol="BTCUSDT",
            timeframe="1h",
            fetched_at=datetime.now(timezone.utc),
            candles=[],
            last_price=25000.0,
            change_24h=1.0,
            volume_24h=1000.0,
            metadata={},
        )
        return [snapshot]

    monkeypatch.setattr(market_fetch, "fetch_binance", fake_fetch_binance)
    monkeypatch.setattr(market_fetch, "persist_snapshot", lambda snapshot, fetched_at: None)
    monkeypatch.setattr(market_fetch, "persist_market_payload", lambda response, cid, fetched_at: None)

    request = {
        "method": "POST",
        "headers": headers,
        "body": body,
    }
    response = market_fetch.handler(request)
    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["snapshots"][0]["source"] == "binance"


def test_news_fetch_handler(monkeypatch):
    body = json.dumps(
        {
            "sources": ["coindesk"],
            "since": "2025-09-28T00:00:00Z",
            "max_per_source": 1,
        }
    )
    headers = {"X-Signature": signed_request(body)}

    fake_item = NewsItem(
        id="1",
        source="coindesk",
        url="https://example.com",
        title="Sample",
        summary="Summary",
        published_at=datetime.now(timezone.utc),
        fetched_at=datetime.now(timezone.utc),
        content_text="Content",
        language="en",
        hash="hash",
    )

    def fake_collect_source(source, req, since, log):
        return [fake_item]

    monkeypatch.setattr(news_fetch, "collect_source_news", fake_collect_source)
    monkeypatch.setattr(news_fetch, "persist_news_item", lambda item: None)
    monkeypatch.setattr(news_fetch, "persist_news_payload", lambda response, cid: None)

    request = {
        "method": "POST",
        "headers": headers,
        "body": body,
    }
    response = news_fetch.handler(request)
    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["items"][0]["source"] == "coindesk"
