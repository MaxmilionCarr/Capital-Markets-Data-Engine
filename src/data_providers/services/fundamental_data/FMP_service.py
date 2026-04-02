from typing import List, Literal
from data_providers.clients import FMPConfig, FMPProvider
from data_providers.clients.base import IssuerInfo, ExchangeInfo, EquityInfo
from data_providers.exceptions import NotSupported, ProviderError, DataNotFound
import pandas as pd
from datetime import datetime, time, timedelta, timezone

class FMPService:
    name = "FMP"
    issuer_capabilities = FMPProvider.issuer_capabilities
    equity_capabilities = FMPProvider.equity_capabilities
    
    def __init__(self, config: FMPConfig):
        self._client = FMPProvider(config)
        self._equity_snapshot_cache: dict[tuple[str, str | None, str | None], tuple[datetime, EquityInfo]] = {}
        self._equity_snapshot_ttl = timedelta(minutes=5)

    def _snapshot_cache_key(self, symbol: str, exchange_name: str | None, currency: str | None) -> tuple[str, str | None, str | None]:
        return (
            symbol.upper().strip(),
            exchange_name.upper().strip() if exchange_name else None,
            currency.upper().strip() if currency else None,
        )

    def _get_cached_snapshot(self, key: tuple[str, str | None, str | None]) -> EquityInfo | None:
        cached = self._equity_snapshot_cache.get(key)
        if cached is None:
            return None

        cached_at, snapshot = cached
        if datetime.now(timezone.utc) - cached_at > self._equity_snapshot_ttl:
            self._equity_snapshot_cache.pop(key, None)
            return None

        return snapshot

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> IssuerInfo:
        return self._client.get_issuer_information(symbol, exchange_name)

    def fetch_exchange(self, symbol: str, exchange_name: str) -> ExchangeInfo:
        """FMP does not provide exchange information."""
        raise NotSupported("FMP does not provide exchange information")

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        return self.fetch_equity_snapshot(symbol, exchange_name, currency)

    def fetch_equity_snapshot(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
        *,
        refresh: bool = False,
    ) -> EquityInfo:
        cache_key = self._snapshot_cache_key(symbol, exchange_name, currency)
        if not refresh:
            cached_snapshot = self._get_cached_snapshot(cache_key)
            if cached_snapshot is not None:
                return cached_snapshot

        snapshot = self._client.get_equity_snapshot(symbol, exchange_name)
        self._equity_snapshot_cache[cache_key] = (datetime.now(timezone.utc), snapshot)
        return snapshot
    
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
        if start_date is None:
            raise ValueError("start_date is required")

        return self._client.get_equity_prices(
            symbol,
            exchange_name,
            start_date,
            end_date,
            bar_size,
            rth_open=rth_open,
            rth_close=rth_close,
        )

    # Service Functions
    def fetch_income_statement(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:
        self._client.connect()
        df = self._client.get_income_statement(symbol, prev_years, period)
        self._client.disconnect()
        return df
    
    def fetch_balance_sheet(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:
        self._client.connect()
        df = self._client.get_balance_sheet(symbol, prev_years, period)
        self._client.disconnect()
        return df
    
    def fetch_cash_flow(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:
        self._client.connect()
        df = self._client.get_cash_flow(symbol, prev_years, period)
        self._client.disconnect()
        return df
    
    def fetch_statement(self, symbol: str, statement_type: str, prev_years: int, period: str) -> pd.DataFrame:
        if statement_type == "income_statement":
            return self.fetch_income_statement(symbol, prev_years, period)
        elif statement_type == "balance_sheet":
            return self.fetch_balance_sheet(symbol, prev_years, period)
        elif statement_type == "cash_flow":
            return self.fetch_cash_flow(symbol, prev_years, period)
        else:
            raise ValueError(f"Invalid statement type: {statement_type}")
    