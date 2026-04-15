from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from crypto_analytics.core.models import Exchange, TradingPair


class ExchangeRepository:
    """Handles all database operations for Exchange model."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, name: str) -> Exchange:
        """Create a new exchange."""
        exchange = Exchange(name=name)
        self.session.add(exchange)
        await self.session.flush()  # получаем id без commit
        return exchange

    async def get_by_id(self, exchange_id: int) -> Exchange | None:
        """Get exchange by ID, returns None if not found."""
        return cast(Exchange | None, await self.session.get(Exchange, exchange_id))

    async def get_by_name(self, name: str) -> Exchange | None:
        """Get exchange by name, returns None if not found."""
        result = await self.session.execute(
            select(Exchange).where(Exchange.name == name)
        )
        return cast(Exchange | None, result.scalar_one_or_none())


class TradingPairRepository:
    """Handles all database operations for TradingPair model."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        base_asset: str,
        quote_asset: str,
        exchange_id: int,
    ) -> TradingPair:
        """Create a new trading pair."""
        pair = TradingPair(
            base_asset=base_asset.upper(),
            quote_asset=quote_asset.upper(),
            exchange_id=exchange_id,
        )
        self.session.add(pair)
        await self.session.flush()
        return pair

    async def get_by_id(self, pair_id: int) -> TradingPair | None:
        """Get trading pair by ID."""
        return cast(TradingPair | None, await self.session.get(TradingPair, pair_id))

    async def get_by_symbol(
        self,
        base_asset: str,
        quote_asset: str,
        exchange_id: int,
    ) -> TradingPair | None:
        """Get trading pair by symbol and exchange."""
        result = await self.session.execute(
            select(TradingPair).where(
                TradingPair.base_asset == base_asset.upper(),
                TradingPair.quote_asset == quote_asset.upper(),
                TradingPair.exchange_id == exchange_id,
            )
        )
        return cast(TradingPair | None, result.scalar_one_or_none())

    async def get_all_active(self) -> list[TradingPair]:
        """Get all active trading pairs."""
        result = await self.session.execute(
            select(TradingPair).where(TradingPair.active == True)  # noqa: E712
        )
        return list(result.scalars().all())

    async def get_by_exchange(self, exchange_id: int) -> list[TradingPair]:
        """Get all trading pairs for a specific exchange."""
        result = await self.session.execute(
            select(TradingPair).where(TradingPair.exchange_id == exchange_id)
        )
        return list(result.scalars().all())

    async def update_active(self, pair_id: int, active: bool) -> TradingPair | None:
        """Update active status of a trading pair."""
        pair = await self.get_by_id(pair_id)
        if pair is None:
            return None
        pair.active = active
        await self.session.flush()
        return pair
