from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator

TIMEFRAME_CHOICES = {"1m", "5m", "15m", "1h", "4h", "1d"}
EXCHANGES = {"binance", "bybit", "cmc"}


class APIModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True
        extra = "forbid"
        json_encoders = {
            datetime: lambda v: (
                v.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                if v.tzinfo
                else v.replace(microsecond=0).isoformat() + "Z"
            )
        }


class HealthResponse(APIModel):
    schema_version: int = 1
    status: str
    time: datetime
    version: str


class MarketFetchRequest(APIModel):
    exchanges: List[str]
    symbols: List[str]
    granularity: str
    limit: int = Field(default=200, ge=1, le=1000)

    @validator("exchanges")
    def validate_exchanges(cls, value):
        unknown = [ex for ex in value if ex not in EXCHANGES]
        if unknown:
            raise ValueError(f"unsupported exchanges: {unknown}")
        return value

    @validator("symbols")
    def validate_symbols(cls, value):
        if not value:
            raise ValueError("symbols must not be empty")
        return value

    @validator("granularity")
    def validate_granularity(cls, value):
        if value not in TIMEFRAME_CHOICES:
            raise ValueError("invalid granularity")
        return value


class MarketCandle(APIModel):
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class MarketSnapshot(APIModel):
    schema_version: int = 1
    origin: str = "market_fetch"
    source: str
    symbol: str
    timeframe: str
    fetched_at: datetime
    candles: List[MarketCandle]
    last_price: float
    change_24h: float
    volume_24h: float
    metadata: Dict[str, str] = Field(default_factory=dict)


class MarketFetchResponse(APIModel):
    snapshots: List[MarketSnapshot]


class NewsFetchRequest(APIModel):
    sources: List[str]
    since: Optional[datetime] = None
    max_per_source: int = Field(default=50, ge=1, le=200)

    def default_window(self) -> timedelta:
        return timedelta(hours=12)


class NewsItem(APIModel):
    schema_version: int = 1
    origin: str = "news_fetch"
    id: str
    source: str
    url: str
    title: str
    summary: Optional[str] = None
    published_at: datetime
    fetched_at: datetime
    content_text: str
    language: str = "en"
    hash: str
    score_hint: Optional[float] = None


class NewsFetchResponse(APIModel):
    items: List[NewsItem]


class AnalyzeNewsRequest(APIModel):
    items: List[NewsItem]
    confirm_window_minutes: int = Field(default=180, ge=15, le=720)
    similarity_threshold: float = Field(default=0.82, ge=0.5, le=1.0)


class NewsCluster(APIModel):
    schema_version: int = 1
    cluster_id: str
    canonical_title: str
    summary: str
    score: float
    source_count: int
    entities: List[str]
    sentiment_hint: Optional[str]
    first_seen: datetime
    last_seen: datetime
    links: List[Dict[str, str]]


class AnalyzeNewsResponse(APIModel):
    clusters: List[NewsCluster]


class StoreListItem(APIModel):
    key: str
    size: int
    created_at: datetime


class StoreListResponse(APIModel):
    objects: List[StoreListItem]

    @classmethod
    def from_objects(cls, objs):
        return cls(
            objects=[
                StoreListItem(key=obj.key, size=obj.size, created_at=obj.created_at)
                for obj in objs
            ]
        )
