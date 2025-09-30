from functools import lru_cache
from typing import List, Literal, Optional

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    env: Literal["production", "development"] = Field("development", env="ENV")

    bybit_api_key: Optional[str] = Field(default=None, env="BYBIT_API_KEY")
    bybit_api_secret: Optional[str] = Field(default=None, env="BYBIT_API_SECRET")

    binance_api_key: Optional[str] = Field(default=None, env="BINANCE_API_KEY")
    binance_api_secret: Optional[str] = Field(default=None, env="BINANCE_API_SECRET")

    cmc_api_key: Optional[str] = Field(default=None, env="CMC_API_KEY")

    blob_base_url: Optional[str] = Field(default=None, env="BLOB_BASE_URL")
    blob_access_key: Optional[str] = Field(default=None, env="BLOB_ACCESS_KEY")
    blob_secret_key: Optional[str] = Field(default=None, env="BLOB_SECRET_KEY")
    blob_bucket: Optional[str] = Field(default=None, env="BLOB_BUCKET")

    hmac_shared_secret: str = Field(..., env="HMAC_SHARED_SECRET")
    allowed_origins: List[str] = Field(default_factory=list, env="ALLOWED_ORIGINS")
    http_proxy: Optional[str] = Field(default=None, env="HTTP_PROXY")

    user_agent: str = Field(
        default="ozel-gpt-collector/1.0 (+contact@your-domain)",
        env="USER_AGENT",
    )

    request_timeout_seconds: float = Field(default=12.0, env="REQUEST_TIMEOUT_SECONDS", ge=1.0)
    storage_ttl_default_days: int = Field(default=7, env="STORAGE_TTL_DEFAULT_DAYS", ge=1)
    max_items_per_source: int = Field(default=100, ge=1)

    class Config:
        env_file = ".env"
        case_sensitive = False

    @validator("allowed_origins", pre=True)
    def parse_allowed_origins(cls, value):
        if not value:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    def is_production(self) -> bool:
        return self.env == "production"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
