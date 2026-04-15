"""Seed initial exchanges and trading pairs into PostgreSQL."""

import asyncio

import structlog

from crypto_analytics.core.database import AsyncSessionLocal
from crypto_analytics.ingestion.repository import (
    ExchangeRepository,
    TradingPairRepository,
)

logger = structlog.get_logger()

PAIRS = [
    ("BTC", "USDT"),
    ("ETH", "USDT"),
    ("SOL", "USDT"),
    ("BNB", "USDT"),
    ("XRP", "USDT"),
    ("ADA", "USDT"),
    ("AVAX", "USDT"),
    ("DOT", "USDT"),
    ("MATIC", "USDT"),
    ("LINK", "USDT"),
    ("LTC", "USDT"),
    ("UNI", "USDT"),
    ("ATOM", "USDT"),
    ("ETC", "USDT"),
    ("XLM", "USDT"),
    ("ALGO", "USDT"),
    ("FIL", "USDT"),
    ("NEAR", "USDT"),
    ("APT", "USDT"),
    ("ARB", "USDT"),
]


async def seed() -> None:
    """Seed exchanges and trading pairs."""
    async with AsyncSessionLocal() as session, session.begin():
        exchange_repo = ExchangeRepository(session)
        pair_repo = TradingPairRepository(session)

        # Get or create Binance exchange
        exchange = await exchange_repo.get_by_name("Binance")
        if exchange is None:
            exchange = await exchange_repo.create("Binance")
            logger.info("created exchange", name="Binance", id=exchange.id)
        else:
            logger.info("exchange already exists", name="Binance", id=exchange.id)

        # Create missing pairs
        created = 0
        skipped = 0
        for base, quote in PAIRS:
            existing = await pair_repo.get_by_symbol(base, quote, exchange.id)
            if existing is None:
                await pair_repo.create(base, quote, exchange.id)
                created += 1
            else:
                skipped += 1

        logger.info(
            "seeding complete",
            created=created,
            skipped=skipped,
        )


if __name__ == "__main__":
    asyncio.run(seed())
