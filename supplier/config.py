from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SUPPLIER_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8090
    public_url: str = "http://localhost:8090"
    enable_live: bool = True
    enable_exchange_feeds: bool = False
    queue_size: int = Field(64, ge=1, le=10_000)
    rate_window_max_events: int = Field(4096, ge=60, le=100_000)
    stale_after_seconds: int = Field(30, ge=1)
    rpc_url: str = "http://127.0.0.1:8545"
    chain_id: int = 31337
    feed_access_address: str = ""
    session_secret: str = "development-only-change-me-please-32chars"
    ume_api_url: str = "http://127.0.0.1:8000"
    seller_private_key: str = ""
    owner_wallet: str = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
    ownership_proof_json: str = ""
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    database_path: str = "./data/supplier.sqlite3"
    storage_mode: str = "sqlite"
    live_window_events: int = Field(5000, ge=100, le=100_000)
    ethereum_rpc_url: str = "https://ethereum-rpc.publicnode.com"
    ethereum_fallback_rpc_urls: str = ""
    ethereum_ws_url: str = ""
    ethereum_fallback_ws_urls: str = ""
    ethereum_pending_transactions: bool = False
    ethereum_reconcile_seconds: float = Field(30, ge=5, le=300)
    ethereum_poll_seconds: float = Field(12, ge=1, le=300)
    ethereum_retry_max_seconds: float = Field(60, ge=1, le=600)
    ethereum_confirmations: int = Field(2, ge=0, le=64)
    ethereum_backfill_blocks: int = Field(2, ge=1, le=100)
    retention_days: int = Field(14, ge=1, le=365)
    max_events: int = Field(250_000, ge=1000, le=10_000_000)
    export_max_rows: int = Field(10_000, ge=1, le=1_000_000)
    expected_protocol_fee_bps: int = Field(250, ge=0, le=2000)

    @property
    def ethereum_rpc_urls(self) -> list[str]:
        """Ordered ingestion providers. Payment verification always uses rpc_url."""
        values = [self.ethereum_rpc_url, *self.ethereum_fallback_rpc_urls.split(",")]
        return [value.strip() for value in values if value.strip()]

    @property
    def ethereum_ws_urls(self) -> list[str]:
        """Ordered event providers. Empty keeps the HTTP reconciliation mode."""
        values = [self.ethereum_ws_url, *self.ethereum_fallback_ws_urls.split(",")]
        return [value.strip() for value in values if value.strip()]

    @property
    def allowed_origins(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def settings() -> Settings:
    return Settings()
