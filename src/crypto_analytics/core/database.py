from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from crypto_analytics.core.config import settings

engine = create_async_engine(
    settings.postgres_url,
    echo=False,  # set True to log all SQL queries for debugging
    pool_size=10,  # number of connections in the pool
    max_overflow=20,  # additional connections allowed during peak load
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,  # objects remain accessible after commit
)


class Base(DeclarativeBase):  # type: ignore[misc]
    """Base class for all SQLAlchemy models."""

    pass
