from __future__ import annotations

from datetime import datetime

import pandas as pd

from data_providers.clients.REST import FMP_client as fmp_client_module
from data_providers.clients.REST.FMP_client import FMPConfig, FMPProvider
from data_providers.clients.base import historical_prices_columns


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = "OK"

    def json(self) -> dict:
        return self._payload


def test_fmp_historical_prices_are_normalized_to_canonical_schema(monkeypatch) -> None:
    payload = {
        "historical": [
            {
                "date": "2024-01-03",
                "open": 102.0,
                "high": 108.0,
                "low": 101.0,
                "close": 107.0,
                "volume": 2_000,
            },
            {
                "date": "2024-01-02",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 104.0,
                "volume": 1_500,
            },
        ]
    }

    def fake_get(url: str):
        return FakeResponse(payload)

    monkeypatch.setattr(fmp_client_module.requests, "get", fake_get)

    provider = FMPProvider(FMPConfig(api_key="test-key"))
    frame = provider.get_equity_prices(
        "AAPL",
        "NASDAQ",
        datetime(2024, 1, 1),
        datetime(2024, 1, 5),
    )

    assert list(frame.columns) == historical_prices_columns
    assert frame["datetime"].tolist() == list(pd.to_datetime(["2024-01-02", "2024-01-03"]))
    assert frame.iloc[0]["open"] == 100.0
    assert frame.iloc[1]["close"] == 107.0
