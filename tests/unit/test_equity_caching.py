"""Test that equities are properly cached and not refetched from APIs unnecessarily."""
from __future__ import annotations

import os
import sqlite3 as sql
import tempfile
from datetime import datetime

import pytest

from data_providers import DataHubConfig
from data_providers.clients.base import EquityInfo, IssuerInfo, ExchangeInfo, Provider
from database_connector import DB, DataBase


class CountingMockProvider:
    """Mock provider that counts how many times methods are called."""
    
    def __init__(self):
        self.fetch_issuer_count = 0
        self.fetch_equity_count = 0
        self.fetch_exchange_count = 0
        
    def reset_counts(self):
        self.fetch_issuer_count = 0
        self.fetch_equity_count = 0
        self.fetch_exchange_count = 0


class CountingMockBasicProvider(CountingMockProvider):
    """Mock basic-info provider that counts fetches."""
    name = "counting_basic"

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
        self.fetch_issuer_count += 1
        return IssuerInfo(
            provider=Provider.FMP,
            symbol=symbol,
            exchange=exchange_name or "NASDAQ",
            currency="USD",
            full_name="Test Corp",
            cik="0000123456",
            lei=None,
        )

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        self.fetch_equity_count += 1
        return EquityInfo(
            provider=Provider.FMP,
            symbol=symbol,
            full_name=f"{symbol} Common Stock",
            sector="Technology",
            industry="Software",
            cik="0000123456",
            lei=None,
        )


class CountingMockExchangeProvider(CountingMockProvider):
    """Mock exchange provider that counts fetches."""
    name = "counting_exchange"

    def fetch_exchange(self, symbol: str, exchange_name: str | None = None) -> ExchangeInfo:
        self.fetch_exchange_count += 1
        return ExchangeInfo(
            provider=Provider.IBKR,
            exchange_name=exchange_name or "NASDAQ",
            timezone="America/New_York",
            currency="USD",
            rth_open="09:30:00",
            rth_close="16:00:00",
        )


def test_equity_caching_prevents_refetch():
    """
    Verify that calling get_or_create_ensure twice on the same equity
    does NOT refetch from the provider the second time.
    """
    fd, temp_path = tempfile.mkstemp(prefix="caching_test_", suffix=".db")
    os.close(fd)
    db = None
    
    # Create mock providers that count calls
    basic_provider = CountingMockBasicProvider()
    exchange_provider = CountingMockExchangeProvider()
    
    try:
        DataBase(temp_path).create_db()
        
        config = DataHubConfig(
            basic_info_services=(basic_provider,),
            exchange_services=(exchange_provider,),
            pricing_services=(),
            fundamental_services=(),
            market_services=(),
        )
        db = DB(temp_path, config=config)
        
        # First call: should fetch from providers
        exchange_provider.reset_counts()
        basic_provider.reset_counts()
        
        equity1 = db.get_equity("AAPL", "NASDAQ", ensure=True)
        assert equity1 is not None
        assert equity1.symbol == "AAPL"
        
        # Count how many times each was called on first fetch
        first_exchange_fetches = exchange_provider.fetch_exchange_count
        first_issuer_fetches = basic_provider.fetch_issuer_count
        first_equity_fetches = basic_provider.fetch_equity_count
        
        # Should have fetched each once
        assert first_exchange_fetches == 1, f"Expected 1 exchange fetch, got {first_exchange_fetches}"
        assert first_issuer_fetches == 1, f"Expected 1 issuer fetch, got {first_issuer_fetches}"
        assert first_equity_fetches == 1, f"Expected 1 equity fetch, got {first_equity_fetches}"
        
        # Second call: should NOT fetch from providers (use cached database version)
        exchange_provider.reset_counts()
        basic_provider.reset_counts()
        
        equity2 = db.get_equity("AAPL", "NASDAQ", ensure=True)
        assert equity2 is not None
        assert equity2.symbol == "AAPL"
        
        # Count how many times each was called on second fetch
        second_exchange_fetches = exchange_provider.fetch_exchange_count
        second_issuer_fetches = basic_provider.fetch_issuer_count
        second_equity_fetches = basic_provider.fetch_equity_count
        
        # Should NOT have fetched anything - should use cached DB version
        assert second_exchange_fetches == 0, f"Expected 0 exchange fetches on cache hit, got {second_exchange_fetches}"
        assert second_issuer_fetches == 0, f"Expected 0 issuer fetches on cache hit, got {second_issuer_fetches}"
        assert second_equity_fetches == 0, f"Expected 0 equity fetches on cache hit, got {second_equity_fetches}"
        
        # Verify both calls returned the same equity
        assert equity1.equity_id == equity2.equity_id
        
    finally:
        if db:
            db._connection.close()
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_different_symbols_still_fetch():
    """
    Verify that fetching different symbols still triggers provider calls.
    """
    fd, temp_path = tempfile.mkstemp(prefix="caching_test_2_", suffix=".db")
    os.close(fd)
    db = None
    
    basic_provider = CountingMockBasicProvider()
    exchange_provider = CountingMockExchangeProvider()
    
    try:
        DataBase(temp_path).create_db()
        
        config = DataHubConfig(
            basic_info_services=(basic_provider,),
            exchange_services=(exchange_provider,),
            pricing_services=(),
            fundamental_services=(),
            market_services=(),
        )
        db = DB(temp_path, config=config)
        
        # Fetch AAPL
        basic_provider.reset_counts()
        exchange_provider.reset_counts()
        equity1 = db.get_equity("AAPL", "NASDAQ", ensure=True)
        aapl_calls = basic_provider.fetch_equity_count
        assert aapl_calls == 1
        
        # Fetch different symbol on same exchange - SHOULD fetch from provider
        basic_provider.reset_counts()
        exchange_provider.reset_counts()
        equity2 = db.get_equity("MSFT", "NASDAQ", ensure=True)
        msft_calls = basic_provider.fetch_equity_count
        assert msft_calls == 1, f"Expected 1 fetch for MSFT, got {msft_calls}"
        
        # Verify they're different equities
        assert equity1.equity_id != equity2.equity_id
        
    finally:
        if db:
            db._connection.close()
        if os.path.exists(temp_path):
            os.unlink(temp_path)
