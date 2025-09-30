import json
from datetime import datetime, timedelta, timezone
from typing import List

import ujson
from pydantic import ValidationError

from src.config import get_settings
from src.logging_setup import bind_request, get_logger
from src.schemas import (
    MarketCandle,
    MarketFetchRequest,
    MarketFetchResponse,
    MarketSnapshot,
)
from src.security import (
    SignatureMismatch,
    SignatureMissing,
    build_cors_headers,
    extract_correlation_id,
    validate_origin,
    verify_signature,
)
from src.storage import get_storage
from src.normalization import ensure_utc
from src import http_clients

settings = get_settings()
storage = get_storage()
logger = get_logger(component="market_fetch")

BINANCE_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

BYBIT_INTERVALS = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "1h": "60",
    "4h": "240",
    "1d": "1440",
}

CMC_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


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
        return {
            "statusCode": 401,
            "headers": cors_headers,
            "body": json.dumps({"error": str(exc)}),
        }

    correlation_id = extract_correlation_id(headers)
    log = bind_request(logger, correlation_id, "/api/market_fetch")

    try:
        req = MarketFetchRequest.parse_raw(body_bytes)
    except ValidationError as exc:
        cors_headers["Content-Type"] = "application/json"
        return {
            "statusCode": 422,
            "headers": cors_headers,
            "body": exc.json(),
        }

    log.info(
        "market_fetch.received",
        exchanges=req.exchanges,
        symbols=req.symbols,
        granularity=req.granularity,
        limit=req.limit,
    )

    snapshots: List[MarketSnapshot] = []
    fetched_at = datetime.now(timezone.utc)

    for exchange in req.exchanges:
        if exchange == "binance":
            snapshots.extend(
                fetch_binance(req.symbols, req.granularity, req.limit, fetched_at, log)
            )
        elif exchange == "bybit":
            snapshots.extend(
                fetch_bybit(req.symbols, req.granularity, req.limit, fetched_at, log)
            )
        elif exchange == "cmc":
            snapshots.extend(
                fetch_cmc(req.symbols, req.granularity, req.limit, fetched_at, log)
            )

    response = MarketFetchResponse(snapshots=snapshots)
    persist_market_payload(response, correlation_id, fetched_at)
    log.info("market_fetch.completed", count=len(snapshots))
    cors_headers["Content-Type"] = "application/json"
    return {"statusCode": 200, "headers": cors_headers, "body": response.json()}


def fetch_binance(symbols, granularity, limit, fetched_at, log):
    results = []
    for symbol in symbols:
        params = {"symbol": symbol, "interval": BINANCE_INTERVALS[granularity], "limit": min(limit, 1000)}
        data = http_clients.get_json(
            "https://api.binance.com/api/v3/klines",
            params=params,
            min_interval=0.3,
        )
        candles = [
            MarketCandle(
                open_time=ensure_utc(datetime.fromtimestamp(entry[0] / 1000, tz=timezone.utc)),
                close_time=ensure_utc(datetime.fromtimestamp(entry[6] / 1000, tz=timezone.utc)),
                open=float(entry[1]),
                high=float(entry[2]),
                low=float(entry[3]),
                close=float(entry[4]),
                volume=float(entry[5]),
            )
            for entry in data
        ]
        ticker = http_clients.get_json(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": symbol},
            min_interval=0.2,
        )
        snapshot = MarketSnapshot(
            source="binance",
            symbol=symbol,
            timeframe=granularity,
            fetched_at=fetched_at,
            candles=candles,
            last_price=float(ticker.get("lastPrice", 0.0)),
            change_24h=float(ticker.get("priceChangePercent", 0.0)),
            volume_24h=float(ticker.get("volume", 0.0)),
            metadata={"count": str(len(candles))},
        )
        results.append(snapshot)
        persist_snapshot(snapshot, fetched_at)
        log.info("market_fetch.binance_snapshot", symbol=symbol, candles=len(candles))
    return results


def fetch_bybit(symbols, granularity, limit, fetched_at, log):
    results = []
    for symbol in symbols:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": BYBIT_INTERVALS[granularity],
            "limit": min(limit, 1000),
        }
        payload = http_clients.get_json(
            "https://api.bybit.com/v5/market/kline",
            params=params,
            min_interval=0.3,
        )
        result = payload.get("result", {})
        candles_raw = result.get("list", [])
        candles = []
        for entry in candles_raw:
            open_time = ensure_utc(datetime.fromtimestamp(int(entry[0]) / 1000, tz=timezone.utc))
            close_time = open_time + timedelta(minutes=granularity_to_minutes(granularity))
            candles.append(
                MarketCandle(
                    open_time=open_time,
                    close_time=close_time,
                    open=float(entry[1]),
                    high=float(entry[2]),
                    low=float(entry[3]),
                    close=float(entry[4]),
                    volume=float(entry[5]),
                )
            )
        ticker = http_clients.get_json(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            min_interval=0.3,
        )
        ticker_info = (ticker.get("result") or {}).get("list") or [{}]
        last_info = ticker_info[0]
        snapshot = MarketSnapshot(
            source="bybit",
            symbol=symbol,
            timeframe=granularity,
            fetched_at=fetched_at,
            candles=candles,
            last_price=float(last_info.get("lastPrice", 0.0)),
            change_24h=float(last_info.get("price24hPcnt", 0.0)),
            volume_24h=float(last_info.get("turnover24h", 0.0)),
            metadata={"count": str(len(candles))},
        )
        results.append(snapshot)
        persist_snapshot(snapshot, fetched_at)
        log.info("market_fetch.bybit_snapshot", symbol=symbol, candles=len(candles))
    return results


def fetch_cmc(symbols, granularity, limit, fetched_at, log):
    results = []
    minutes = granularity_to_minutes(granularity)
    end_time = ensure_utc(datetime.now(timezone.utc))
    start_time = end_time - timedelta(minutes=minutes * limit)
    headers = {"X-CMC_PRO_API_KEY": settings.cmc_api_key or ""}
    for symbol in symbols:
        params = {
            "symbol": symbol,
            "interval": CMC_INTERVALS[granularity],
            "time_start": start_time.isoformat(),
            "time_end": end_time.isoformat(),
            "count": limit,
        }
        payload = http_clients.get_json(
            "https://pro-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical",
            headers=headers,
            params=params,
            min_interval=0.5,
        )
        data = ((payload.get("data") or {}).get("quotes")) or []
        candles = [
            MarketCandle(
                open_time=ensure_utc(datetime.fromisoformat(entry["time_open"])),
                close_time=ensure_utc(datetime.fromisoformat(entry["time_close"])),
                open=float(entry["quote"]["USD"]["open"]),
                high=float(entry["quote"]["USD"]["high"]),
                low=float(entry["quote"]["USD"]["low"]),
                close=float(entry["quote"]["USD"]["close"]),
                volume=float(entry["quote"]["USD"].get("volume", 0.0)),
            )
            for entry in data
        ]
        quote = http_clients.get_json(
            "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
            headers=headers,
            params={"symbol": symbol},
            min_interval=0.5,
        )
        info = ((quote.get("data") or {}).get(symbol) or {}).get("quote", {}).get("USD", {})
        snapshot = MarketSnapshot(
            source="cmc",
            symbol=symbol,
            timeframe=granularity,
            fetched_at=fetched_at,
            candles=candles,
            last_price=float(info.get("price", 0.0)),
            change_24h=float(info.get("percent_change_24h", 0.0)),
            volume_24h=float(info.get("volume_24h", 0.0)),
            metadata={"count": str(len(candles))},
        )
        results.append(snapshot)
        persist_snapshot(snapshot, fetched_at)
        log.info("market_fetch.cmc_snapshot", symbol=symbol, candles=len(candles))
    return results


def granularity_to_minutes(granularity: str) -> int:
    mapping = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }
    return mapping[granularity]


def persist_snapshot(snapshot: MarketSnapshot, fetched_at: datetime):
    date_key = fetched_at.strftime("%Y%m%d%H")
    storage_key = f"market/{snapshot.source}/{snapshot.symbol}/{date_key}/snapshot.json"
    payload = ujson.loads(snapshot.json())
    payload.update(
        {
            "created_at": fetched_at.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
        }
    )
    storage.put_json(storage_key, payload)


def persist_market_payload(response: MarketFetchResponse, correlation_id: str, fetched_at: datetime):
    log_key = f"logs/{fetched_at.date()}/{correlation_id}-market_fetch.json"
    storage.put_json(
        log_key,
        {
            "schema_version": 1,
            "created_at": fetched_at.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
            "type": "market_fetch",
            "payload": ujson.loads(response.json()),
        },
    )
    action_key = f"gpt/actions/{fetched_at.date()}/{correlation_id}-market_fetch.json"
    storage.put_json(
        action_key,
        {
            "schema_version": 1,
            "created_at": fetched_at.isoformat(),
            "ttl_days": settings.storage_ttl_default_days,
            "endpoint": "market_fetch",
            "payload": ujson.loads(response.json()),
        },
    )
