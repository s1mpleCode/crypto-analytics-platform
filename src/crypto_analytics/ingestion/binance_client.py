import asyncio
from typing import Any

import httpx
import structlog
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

logger = structlog.get_logger()

BINANCE_BASE_URL = "https://api.binance.com/api/v3"


class Kline(BaseModel):
    """Single candlestick data from Binance API."""

    symbol: str
    open_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    close_time: int

    @classmethod
    def from_api_response(cls, symbol: str, data: list[Any]) -> Kline:
        """Parse raw Binance kline list into a Kline model."""
        return cls(
            symbol=symbol,
            open_time=data[0],
            open_price=data[1],
            high_price=data[2],
            low_price=data[3],
            close_price=data[4],
            volume=data[5],
            close_time=data[6],
        )


@retry(  # type: ignore[misc]  # tenacity lacks complete type stubs
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
)
async def fetch_klines(
    client: httpx.AsyncClient,
    symbol: str,
    limit: int = 100,
    interval: str = "1m",
) -> list[Kline]:
    """Fetch klines for a single trading pair from Binance API."""
    response = await client.get(
        f"{BINANCE_BASE_URL}/klines",
        params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )
    response.raise_for_status()

    return [Kline.from_api_response(symbol, row) for row in response.json()]


async def fetch_all_pairs(
    pairs: list[str],
    limit: int = 100,
    interval: str = "1m",
    max_concurrent: int = 5,
) -> dict[str, list[Kline]]:
    """Fetch klines for multiple trading pairs concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_limit(
        client: httpx.AsyncClient,
        symbol: str,
    ) -> tuple[str, list[Kline]]:
        """Wrap fetch_klines with semaphore and return symbol alongside results."""
        async with semaphore:
            klines = await fetch_klines(client, symbol, limit, interval)
            return symbol, klines

    async with httpx.AsyncClient() as client:
        tasks = [fetch_with_limit(client, pair) for pair in pairs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    processed: dict[str, list[Kline]] = {}
    for pair, result in zip(pairs, results, strict=True):
        if isinstance(result, BaseException):
            logger.error(
                "failed to fetch klines",
                symbol=pair,
                error=str(result),
            )
            continue
        symbol, klines = result
        processed[symbol] = klines

    return processed
