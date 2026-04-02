from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.live_api
def test_database_connector_can_create_and_fetch_live_equity(live_db) -> None:
    equity = live_db.get_equity("AAPL", "NASDAQ", ensure=True)

    assert equity is not None
    assert equity.symbol == "AAPL"

    cur = live_db._connection.cursor()
    cur.execute("SELECT COUNT(*) FROM equities")
    assert cur.fetchone()[0] >= 1

    statements = equity.get_statements(
        statement_type="income_statement",
        period="annual",
        look_back=1,
        ensure=True,
    )
    assert statements is not None
    assert len(statements) == 1