from __future__ import annotations

import pytest

from data_providers import DataHub, DataHubConfig
from data_providers.clients.base import EquityInfo, IssuerInfo, Provider


class SnapshotService:
    """Mock basic-info service that exposes a dedicated snapshot fetch path."""
    name = "snapshot"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        return IssuerInfo(
            provider=Provider.FMP,
            symbol=symbol,
            exchange=exchange_name or "NASDAQ",
            currency="USD",
            full_name="Snapshot Inc",
            cik="0000000001",
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(provider=Provider.FMP, symbol=symbol)

    def fetch_equity_snapshot(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(
            provider=Provider.FMP,
            symbol=symbol,
            full_name="Snapshot Preferred",
            sector="Tech",
            industry="Software",
            cik="0000000001",
        )


class PlainService:
    """Mock basic-info service without snapshot support for routing fallback tests."""
    name = "plain"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        return IssuerInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            exchange=exchange_name or "NASDAQ",
            currency="USD",
            full_name="Plain Inc",
            sec_type="STK",
            timezone="America/New_York",
            rth_open="09:30:00",
            rth_close="16:00:00",
            lei="5493001KJTIIGC8Y1R12",
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            full_name="Plain Service",
            industry="Software",
            lei="5493001KJTIIGC8Y1R12",
        )


def test_priority_basic_info_prefers_snapshot_method_for_direct_fetch() -> None:
    hub = DataHub(
        DataHubConfig(basic_info_services=(PlainService(), SnapshotService()))
    )

    info = hub.basic_info.fetch_equity("AAPL", "NASDAQ", "USD")
    assert info.full_name == "Snapshot Preferred"
    assert info.sector == "Tech"


def test_pricing_services_must_be_single_provider() -> None:
    with pytest.raises(ValueError):
        DataHubConfig(
            basic_info_services=(PlainService(),),
            pricing_services=(object(), object()),
        )
