from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from tenacity import RetryError

from crypto_analytics.ingestion.binance_client import (
    Kline,
    fetch_all_pairs,
    fetch_klines,
)

# Reusable valid raw response from Binance API
VALID_RAW_KLINE = [
    1736150400000,  # open_time
    "94832.01",  # open_price
    "94967.99",  # high_price
    "94700.00",  # low_price
    "94845.32",  # close_price
    "125.43",  # volume
    1736150459999,  # close_time
    "11893847.12",  # quote_volume (ignored)
    1547,  # trades count (ignored)
    "68.23",  # taker_buy_base_volume (ignored)
    "6472341.23",  # taker_buy_quote_volume (ignored)
    "0",  # ignore
]


class TestKlineFromApiResponse:
    """Tests for Kline.from_api_response parser."""

    def test_parses_valid_data_correctly(self) -> None:
        """Happy path: valid Binance response is parsed into correct Kline fields."""
        kline = Kline.from_api_response("BTCUSDT", VALID_RAW_KLINE)

        assert kline.symbol == "BTCUSDT"
        assert kline.open_time == 1736150400000
        assert kline.open_price == 94832.01
        assert kline.high_price == 94967.99
        assert kline.low_price == 94700.00
        assert kline.close_price == 94845.32
        assert kline.volume == 125.43
        assert kline.close_time == 1736150459999

    def test_converts_string_prices_to_float(self) -> None:
        """Pydantic coerces string prices from API into float values."""
        kline = Kline.from_api_response("ETHUSDT", VALID_RAW_KLINE)

        assert isinstance(kline.open_price, float)
        assert isinstance(kline.close_price, float)
        assert isinstance(kline.volume, float)

    def test_raises_on_invalid_price_format(self) -> None:
        """Non-numeric price string raises ValidationError."""
        invalid_data = VALID_RAW_KLINE.copy()
        invalid_data[1] = "not_a_number"

        with pytest.raises(ValidationError):
            Kline.from_api_response("BTCUSDT", invalid_data)

    def test_raises_on_missing_fields(self) -> None:
        """List shorter than expected raises IndexError."""
        short_data = VALID_RAW_KLINE[:3]

        with pytest.raises(IndexError):
            Kline.from_api_response("BTCUSDT", short_data)


class TestFetchKlines:
    """Tests for fetch_klines function."""

    async def test_returns_list_of_klines(self) -> None:
        """Successful response is parsed into list of Kline objects."""
        mock_client = AsyncMock()
        mock_client.get.return_value = MagicMock(
            json=lambda: [VALID_RAW_KLINE, VALID_RAW_KLINE],
            raise_for_status=lambda: None,
        )

        result = await fetch_klines(mock_client, "BTCUSDT", limit=2)

        assert len(result) == 2
        assert result[0].symbol == "BTCUSDT"
        assert result[0].open_price == 94832.01

    async def test_raises_on_http_error(self) -> None:
        """HTTP error raises RetryError after all attempts exhausted."""
        mock_client = AsyncMock()
        mock_client.get.return_value = MagicMock(
            raise_for_status=MagicMock(side_effect=Exception("429 Too Many Requests")),
        )

        with pytest.MonkeyPatch.context() as mp:
            # Remove wait between retries to keep tests fast
            mp.setattr(
                "crypto_analytics.ingestion.binance_client.fetch_klines.retry.wait",
                lambda retry_state: 0,
            )
            with pytest.raises(RetryError):
                await fetch_klines(mock_client, "BTCUSDT")


class TestFetchAllPairs:
    """Tests for fetch_all_pairs function."""

    async def test_returns_results_for_all_pairs(self) -> None:
        """All pairs are fetched and returned as dict."""
        mock_client = AsyncMock()
        mock_client.get.return_value = MagicMock(
            json=lambda: [VALID_RAW_KLINE],
            raise_for_status=lambda: None,
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "crypto_analytics.ingestion.binance_client.httpx.AsyncClient",
                lambda: mock_client,
            )
            # Use AsyncMock as context manager
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)

            result = await fetch_all_pairs(["BTCUSDT", "ETHUSDT"], limit=1)

        assert "BTCUSDT" in result
        assert "ETHUSDT" in result
        assert len(result["BTCUSDT"]) == 1

    async def test_skips_failed_pairs(self) -> None:
        """Pairs that fail to fetch are skipped, others are returned."""

        async def mock_fetch(
            client: AsyncMock, symbol: str, *args: Any, **kwargs: Any
        ) -> list[Kline]:
            if symbol == "ETHUSDT":
                raise Exception("API error")
            return [Kline.from_api_response(symbol, VALID_RAW_KLINE)]

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "crypto_analytics.ingestion.binance_client.fetch_klines",
                mock_fetch,
            )
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mp.setattr(
                "crypto_analytics.ingestion.binance_client.httpx.AsyncClient",
                lambda: mock_client,
            )

            with patch(
                "crypto_analytics.ingestion.binance_client.logger"
            ) as mock_logger:
                result = await fetch_all_pairs(["BTCUSDT", "ETHUSDT"], limit=1)

                # Verify failed pair is logged with symbol
                mock_logger.error.assert_called_once_with(
                    "failed to fetch klines",
                    symbol="ETHUSDT",
                    error="API error",
                )

        assert "BTCUSDT" in result
        assert "ETHUSDT" not in result
