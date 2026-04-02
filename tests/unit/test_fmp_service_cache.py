from __future__ import annotations

from data_providers.clients.base import EquityInfo, Provider
from data_providers.services.fundamental_data.FMP_service import FMPService


class DummyClient:
    """Dummy FMP client that counts snapshot requests for cache verification."""
    def __init__(self):
        self.calls = 0

    def get_equity_snapshot(self, symbol: str, exchange_name: str | None = None) -> EquityInfo:
        self.calls += 1
        return EquityInfo(
            provider=Provider.FMP,
            symbol=symbol,
            full_name=f"{symbol}-snapshot-{self.calls}",
            sector="Technology",
            industry="Software",
            cik="0000123456",
        )


def test_fmp_snapshot_cache_returns_cached_value() -> None:
    service = FMPService.__new__(FMPService)
    service._client = DummyClient()
    service._equity_snapshot_cache = {}

    from datetime import timedelta

    service._equity_snapshot_ttl = timedelta(minutes=5)

    first = service.fetch_equity_snapshot("AAPL", "NASDAQ", "USD")
    second = service.fetch_equity_snapshot("AAPL", "NASDAQ", "USD")

    assert first.full_name == second.full_name
    assert service._client.calls == 1
