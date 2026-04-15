from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):  # type: ignore[misc]
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Binance
    binance_base_url: str = Field(
        default="https://api.binance.com/api/v3",
        description="Binance REST API base URL",
    )
    binance_max_concurrent: int = Field(
        default=5,
        description="Maximum concurrent requests to Binance API",
    )

    # PostgreSQL
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_port: int = Field(default=5432)

    # ClickHouse
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str
    clickhouse_port: int = Field(default=8123)
    clickhouse_tcp_port: int = Field(
        default=9000, description="ClickHouse native TCP port"
    )

    # Redis
    redis_password: str
    redis_port: int = Field(default=6379)

    @property
    def postgres_url(self) -> str:
        """Build PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:"
            f"{self.postgres_password}@localhost:"
            f"{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def clickhouse_url(self) -> str:
        """Build ClickHouse HTTP connection URL."""
        return (
            f"http://{self.clickhouse_user}:{self.clickhouse_password}"
            f"@localhost:{self.clickhouse_port}/{self.clickhouse_db}"
        )

    @property
    def clickhouse_tcp_url(self) -> str:
        """Build ClickHouse native TCP connection URL."""
        return (
            f"clickhouse://{self.clickhouse_user}:{self.clickhouse_password}"
            f"@localhost:{self.clickhouse_tcp_port}/{self.clickhouse_db}"
        )


settings = Settings()
