from __future__ import annotations

import os
import sqlite3 as sql
import tempfile
from datetime import datetime, time

import pandas as pd

from data_providers import DataHubConfig
from data_providers.clients.base import EquityInfo, IssuerInfo, ExchangeInfo, Provider
from database_connector import DB, DataBase


class MockBasicFMP:
    """Mock basic-info provider that simulates FMP issuer and equity enrichment."""
    name = "mock_basic_fmp"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        return IssuerInfo(
            provider=Provider.FMP,
            symbol=symbol,
            exchange=exchange_name or "NASDAQ",
            currency="USD",
            full_name="Acme Corp",
            cik="0000123456",
            lei=None,
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(
            provider=Provider.FMP,
            symbol=symbol,
            full_name=f"{symbol} Common Stock",
            sector="Technology",
            industry="Software",

            cik="0000123456",
            lei=None,
        )

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open=None,
        rth_close=None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "datetime": datetime(2024, 1, 2, 0, 0, 0),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 108.0,
                    "volume": 1_000_000,
                }
            ]
        )


class MockBasicIBKR:
    """Mock basic-info provider that simulates IBKR issuer and equity enrichment."""
    name = "mock_basic_ibkr"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        return IssuerInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            exchange=exchange_name or "NASDAQ",
            currency="USD",
            full_name="Acme Corp",
            cik=None,
            lei="5493001KJTIIGC8Y1R12",
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            full_name=f"{symbol} (IBKR Name)",
            sector=None,
            industry="Software Infrastructure",

            cik=None,
            lei="5493001KJTIIGC8Y1R12",
        )

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open=None,
        rth_close=None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "datetime": datetime(2024, 1, 2, 0, 0, 0),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 108.0,
                    "volume": 1_000_000,
                }
            ]
        )


class MockBasicNoIds:
    """Mock basic-info provider with no cik/lei coverage."""
    name = "mock_basic_no_ids"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        return IssuerInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            exchange=exchange_name,
            currency="USD",
            full_name="Global Mining Corp",
            cik=None,
            lei=None,
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return EquityInfo(
            provider=Provider.IBKR,
            symbol=symbol,
            full_name=f"{symbol} ({exchange_name})",
            sector="Materials",
            industry="Metals & Mining",
            cik=None,
            lei=None,
        )

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open=None,
        rth_close=None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "datetime": datetime(2024, 1, 2, 0, 0, 0),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 108.0,
                    "volume": 1_000_000,
                }
            ]
        )


class MockPricingIBKR:
    """Mock pricing provider that returns a single deterministic daily bar."""
    name = "mock_pricing_ibkr"

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open=None,
        rth_close=None,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "datetime": datetime(2024, 1, 2, 0, 0, 0),
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 108.0,
                    "volume": 1_000_000,
                }
            ]
        )


class MockExchangeIBKR:
    """Mock exchange provider that returns exchange metadata."""
    name = "mock_exchange_ibkr"

    def fetch_exchange(self, symbol: str, exchange_name: str) -> ExchangeInfo:
        return ExchangeInfo(
            provider=Provider.IBKR,
            exchange_name=exchange_name or "NASDAQ",
            timezone="America/New_York",
            currency="USD",
            rth_open="09:30:00",
            rth_close="16:00:00",
        )


class MockFundamentalFMP:
    """Mock fundamental provider that returns one deterministic statement row."""
    name = "mock_fundamental_fmp"

    def fetch_statement(self, symbol: str, statement_type: str, prev_years: int, period: str):
        return [{"date": "2024-12-31", "statement_type": statement_type, "revenue": 1234567}]


def _build_db(path: str) -> DB:
    cfg = DataHubConfig(
        basic_info_services=(MockBasicFMP(), MockBasicIBKR()),
        exchange_services=(MockExchangeIBKR(),),
        pricing_services=(MockPricingIBKR(),),
        fundamental_services=(MockFundamentalFMP(),),
    )
    return DB(db_path=path, config=cfg)


def _build_db_without_ids(path: str) -> DB:
    cfg = DataHubConfig(
        basic_info_services=(MockBasicNoIds(),),
        exchange_services=(MockExchangeIBKR(),),
        pricing_services=(MockPricingIBKR(),),
        fundamental_services=(MockFundamentalFMP(),),
    )
    return DB(db_path=path, config=cfg)


def test_schema_service_creates_expected_tables() -> None:
    fd, temp_path = tempfile.mkstemp(prefix="unit_schema_", suffix=".db")
    os.close(fd)
    try:
        creator = DataBase(temp_path)
        creator.create_db()

        con = sql.connect(temp_path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        con.close()

        expected = {
            "provider_provenance",
            "exchanges",
            "issuers",
            "equities",
            "equity_prices_daily",
            "equity_intraday_coverage",
            "statements",
        }
        assert expected.issubset(tables)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_sqlite_db_flow_with_mock_providers() -> None:
    fd, temp_path = tempfile.mkstemp(prefix="unit_db_flow_", suffix=".db")
    os.close(fd)
    db = None

    try:
        DataBase(temp_path).create_db()
        db = _build_db(temp_path)

        equity = db.get_equity("AAPL", "NASDAQ", ensure=True)
        assert equity is not None
        assert equity.symbol == "AAPL"

        prices = equity.get_prices(
            start_date=datetime(2024, 1, 2),
            end_date=datetime(2024, 1, 3, 16, 0, 0),
            period="1 day",
            ensure=True,
        )
        cur = db._connection.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM equity_prices_daily WHERE equity_id = ?",
            (equity.equity_id,),
        )
        assert cur.fetchone()[0] >= 1

        statements = equity.get_statements(
            statement_type="income_statement",
            period="annual",
            look_back=1,
            ensure=True,
        )
        assert statements is not None
        assert len(statements) == 1
        assert statements[0].statement["statement_type"] == "income_statement"
    finally:
        if db is not None:
            db.close()
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_symbol_fallback_links_issuer_when_cik_lei_missing() -> None:
    fd, temp_path = tempfile.mkstemp(prefix="unit_symbol_fallback_", suffix=".db")
    os.close(fd)
    db = None

    try:
        DataBase(temp_path).create_db()
        db = _build_db_without_ids(temp_path)

        equity_asx = db.get_equity("BHP", "ASX", ensure=True)
        equity_nyse = db.get_equity("BHP", "NYSE", ensure=True)

        assert equity_asx is not None
        assert equity_nyse is not None
        assert equity_asx.issuer_id == equity_nyse.issuer_id

        cur = db._connection.cursor()
        cur.execute("SELECT COUNT(*) FROM issuers")
        assert cur.fetchone()[0] == 1
    finally:
        if db is not None:
            db.close()
        if os.path.exists(temp_path):
            os.remove(temp_path)
