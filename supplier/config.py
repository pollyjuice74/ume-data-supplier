from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SUPPLIER_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8090
    public_url: str = "http://localhost:8090"
    enable_live: bool = True
    queue_size: int = Field(256, ge=1, le=10_000)
    stale_after_seconds: int = Field(30, ge=1)
    rpc_url: str = "http://127.0.0.1:8545"
    chain_id: int = 31337
    feed_access_address: str = ""
    session_secret: str = "development-only-change-me-please-32chars"
    ume_api_url: str = "http://127.0.0.1:8000"
    seller_private_key: str = ""
    owner_wallet: str = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    @property
    def allowed_origins(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def settings() -> Settings:
    return Settings()
