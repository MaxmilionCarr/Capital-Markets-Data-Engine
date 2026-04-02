from __future__ import annotations
import requests

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from data_providers.clients.base import (
    FundamentalDataProvider,
    IssuerInfo,
    IssuerCapabilities,
    EquityInfo,
    EquityCapabilities,
    MarketDataProvider,
    Provider,
    historical_prices_columns,
)
from data_providers.exceptions import DataNotFound, NotSupported

@dataclass
class FMPConfig:
    api_key: str
    base_url: str = "https://financialmodelingprep.com/stable/"

class FMPProvider(FundamentalDataProvider, MarketDataProvider):
    provider = Provider.FMP

    issuer_capabilities = IssuerCapabilities(
        symbol=True,
        exchange=True,
        currency=True,
        full_name=True,
        cik=True
    )

    equity_capabilities = EquityCapabilities(
        sector=True,
        industry=True,
    )
    # NEED TO CONFIGURE PROVIDER CAPABILITIES

    def __init__(self, config: FMPConfig):
        self.api_key = config.api_key
        self.base_url = config.base_url

    def _request_json(self, extension: str, *, allow_empty: bool = False) -> Any:
        url = self.base_url + extension
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from FMP: {response.text}")

        data = response.json()
        if not data and not allow_empty:
            raise Exception("FMP returned no data")
        return data

    @staticmethod
    def _first_record(data: Any) -> dict[str, Any]:
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict):
            return data
        return {}
        
    def connect(self):
        # No persistent connection needed for FMP, but we can validate the API key
        pass
    
    def disconnect(self):
        pass

    # Market Data
    def get_issuer_information(self, symbol: str, exchange: Optional[str]) -> IssuerInfo:
        '''extension = f"search-exchange-variants?symbol={symbol}&apikey={self.api_key}"''' # Not offered need to upgrade
        extension = f"profile?symbol={symbol}&apikey={self.api_key}"
        data = self._request_json(extension)
        
        if exchange:
            for entry in data:
                if entry.get("exchange") == exchange:
                    break
            else:
                raise Exception(f"No issuer information found for {symbol} on exchange {exchange}")
        else:
            # If no exchange specified, use the first result
            if not data:
                raise Exception(f"No issuer information found for {symbol}")
            entry = data[0]
        
        return IssuerInfo(
            provider=self.provider,
            symbol=entry.get("symbol"),
            exchange=entry.get("exchange"),
            currency=entry.get("currency"),
            full_name=entry.get("companyName"),
            cik=entry.get("cik"),
        )
    
    # TODO: Currently assuming exchange is given correctly each time, will need to change if i do this globally
    def get_equity_information(self, symbol: str, exchange: Optional[str]) -> EquityInfo:
        '''extension = f"search-exchange-variants?symbol={symbol}&apikey={self.api_key}"''' # Not offered need to upgrade
        extension = f"profile?symbol={symbol}&apikey={self.api_key}"
        data = self._request_json(extension)
        
        if exchange:
            for entry in data:
                if entry.get("exchange") == exchange:
                    break
            else:
                raise Exception(f"No equity information found for {symbol} on exchange {exchange}")
        else:
            # If no exchange specified, use the first result
            if not data:
                raise Exception(f"No equity information found for {symbol}")
            entry = data[0]
        
        #TODO Change dividend yeidl to dividend amount W
        return EquityInfo(
            provider=self.provider,
            symbol=entry.get("symbol"),
            full_name=entry.get("companyName"),
            sector=entry.get("sector"),
            industry=entry.get("industry"),
            cik=entry.get("cik"),
            dividend_yield=entry.get("dividendYield"),
        )

    def get_quote_information(self, symbol: str) -> dict[str, Any]:
        extension = f"quote/{symbol}?apikey={self.api_key}"
        data = self._request_json(extension)
        return self._first_record(data)

    def get_equity_prices(
        self,
        symbol: str,
        exchange_name: str | None,
        start_date: datetime,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open=None,
        rth_close=None,
    ) -> pd.DataFrame:
        if bar_size != "1 day":
            raise NotSupported("FMP historical prices currently support daily bars only")

        if start_date is None:
            raise ValueError("start_date is required")

        end_date = end_date or datetime.now()
        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        extension = (
            f"historical-price-full/{symbol}"
            f"?from={start_date:%Y-%m-%d}&to={end_date:%Y-%m-%d}&apikey={self.api_key}"
        )
        payload = self._request_json(extension, allow_empty=True)

        if isinstance(payload, dict):
            rows = payload.get("historical", [])
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = []

        df = pd.DataFrame(rows)
        if df.empty:
            raise DataNotFound(f"No historical prices found for {symbol}")

        if "date" in df.columns and "datetime" not in df.columns:
            df["datetime"] = pd.to_datetime(df["date"], utc=False)
        elif "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=False)
        else:
            df["datetime"] = pd.NaT

        for column in historical_prices_columns:
            if column not in df.columns:
                df[column] = pd.NA

        df = df[historical_prices_columns]
        df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        return df

    def get_equity_snapshot(self, symbol: str, exchange: str | None = None) -> EquityInfo:
        profile_extension = f"profile?symbol={symbol}&apikey={self.api_key}"
        profile_data = self._request_json(profile_extension)

        profile_entry = None
        for entry in profile_data:
            if exchange is None or entry.get("exchange") == exchange:
                profile_entry = entry
                break

        if profile_entry is None:
            if exchange is None and profile_data:
                profile_entry = profile_data[0]
            else:
                raise Exception(f"No equity information found for {symbol} on exchange {exchange}")

        quote_entry = {}
        try:
            quote_entry = self.get_quote_information(symbol)
        except Exception:
            quote_entry = {}

        def _first_value(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        return EquityInfo(
            provider=self.provider,
            symbol=profile_entry.get("symbol"),
            full_name=profile_entry.get("companyName"),
            sector=profile_entry.get("sector"),
            industry=profile_entry.get("industry"),
            dividend_yield=_first_value(
                profile_entry.get("dividendYield"),
                profile_entry.get("lastDividend"),
                quote_entry.get("dividendYield"),
            ),
            cik=profile_entry.get("cik"),
            lei=profile_entry.get("lei"),
        )
    

    # Fundamental Data
    def get_income_statement(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        extension = f"income-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        return self._request_json(extension)
    
    def get_balance_sheet(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        extension = f"balance-sheet-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        return self._request_json(extension)
    
    def get_cash_flow(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        extension = f"cash-flow-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        return self._request_json(extension)

        