from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from crypto_analytics.core.database import Base


class Exchange(Base):
    """Cryptocurrency exchange."""

    __tablename__ = "exchanges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    pairs: Mapped[list[TradingPair]] = relationship(
        "TradingPair", back_populates="exchange"
    )


class TradingPair(Base):
    """Trading pair on a specific exchange."""

    __tablename__ = "trading_pairs"
    __table_args__ = (
        Index("ix_trading_pairs_exchange_id", "exchange_id"),
        Index("ix_trading_pairs_base_asset", "base_asset"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    base_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    exchange_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("exchanges.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    exchange: Mapped[Exchange] = relationship("Exchange", back_populates="pairs")

    @property
    def symbol(self) -> str:
        """Return trading pair symbol e.g. BTCUSDT."""
        return f"{self.base_asset}{self.quote_asset}"
