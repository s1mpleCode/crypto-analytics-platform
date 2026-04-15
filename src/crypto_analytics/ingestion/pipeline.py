import asyncio
from datetime import UTC, datetime

import structlog

from crypto_analytics.core.database import AsyncSessionLocal
from crypto_analytics.ingestion.binance_client import fetch_all_pairs
from crypto_analytics.ingestion.clickhouse_client import (
    get_clickhouse_client,
    insert_klines,
)
from crypto_analytics.ingestion.repository import (
    TradingPairRepository,
)

logger = structlog.get_logger()


async def run_pipeline() -> None:
    """
    Main ingestion pipeline:
    1. Load active trading pairs from PostgreSQL
    2. Fetch klines from Binance
    3. Insert into ClickHouse
    """
    started_at = datetime.now(tz=UTC)
    logger.info("pipeline started", started_at=started_at.isoformat())

    # Step 1: get active pairs from PostgreSQL
    async with AsyncSessionLocal() as session:
        pair_repo = TradingPairRepository(session)
        active_pairs = await pair_repo.get_all_active()

    if not active_pairs:
        logger.warning("no active trading pairs found, skipping pipeline run")
        return

    symbols = [pair.symbol for pair in active_pairs]
    logger.info("loaded active pairs", count=len(symbols), symbols=symbols)

    # Step 2: fetch klines from Binance
    results = await fetch_all_pairs(symbols, limit=100)
    logger.info("fetched klines", pairs_count=len(results))

    # Step 3: insert into ClickHouse
    ch_client = get_clickhouse_client()
    total_inserted = 0

    for symbol, klines in results.items():
        insert_klines(ch_client, klines)
        total_inserted += len(klines)
        logger.info("inserted klines", symbol=symbol, count=len(klines))

    finished_at = datetime.now(tz=UTC)
    duration = (finished_at - started_at).total_seconds()

    logger.info(
        "pipeline finished",
        total_inserted=total_inserted,
        duration_seconds=round(duration, 2),
    )


if __name__ == "__main__":
    asyncio.run(run_pipeline())
