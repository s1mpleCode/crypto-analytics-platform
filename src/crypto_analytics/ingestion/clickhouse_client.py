from datetime import UTC, datetime

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from crypto_analytics.core.config import settings
from crypto_analytics.ingestion.binance_client import Kline


def get_clickhouse_client() -> Client:
    """Create and return a ClickHouse client."""
    return clickhouse_connect.get_client(
        host="localhost",
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def insert_klines(client: Client, klines: list[Kline]) -> None:
    """Insert klines batch into ClickHouse."""
    if not klines:
        return

    rows = [
        [
            kline.symbol,
            datetime.fromtimestamp(kline.open_time / 1000, tz=UTC),
            datetime.fromtimestamp(kline.close_time / 1000, tz=UTC),
            kline.open_price,
            kline.high_price,
            kline.low_price,
            kline.close_price,
            kline.volume,
        ]
        for kline in klines
    ]

    client.insert(
        "klines",
        rows,
        column_names=[
            "symbol",
            "open_time",
            "close_time",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ],
    )
