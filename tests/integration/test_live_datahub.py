from __future__ import annotations

import pytest

from data_providers import DataHub, DataHubConfig


@pytest.mark.integration
@pytest.mark.live_api
def test_datahub_can_fetch_live_fmp_equity_and_statements(fmp_service) -> None:
    hub = DataHub(
        DataHubConfig(
            basic_info_services=(fmp_service,),
            fundamental_services=(fmp_service,),
        )
    )

    equity = hub.require_basic_info().fetch_equity("AAPL", "NASDAQ", "USD")
    assert equity.symbol == "AAPL"
    assert equity.full_name is not None

    statements = hub.require_fundamentals().fetch_statement(
        "AAPL",
        "income_statement",
        1,
        "annual",
    )
    assert statements
