from __future__ import annotations
import json
import requests

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Literal, Optional

import pandas as pd

from data_providers.clients.base import FundamentalDataProvider, IssuerInfo, IssuerCapabilities, EquityInfo, EquityCapabilities, MarketDataProvider, Provider

@dataclass
class FMPConfig:
    api_key: str
    base_url: Optional[str] = "https://financialmodelingprep.com/stable/"

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
        dividend_yield=True,
        beta=True,
        market_cap=True,
    )
    # NEED TO CONFIGURE PROVIDER CAPABILITIES

    def __init__(self, config: FMPConfig):
        self.api_key = config.api_key
        self.base_url = config.base_url
        
    def connect(self):
        # No persistent connection needed for FMP, but we can validate the API key
        pass
    
    def disconnect(self):
        pass

    # Market Data
    def get_issuer_information(self, symbol: str, exchange: str) -> IssuerInfo:
        '''extension = f"search-exchange-variants?symbol={symbol}&apikey={self.api_key}"''' # Not offered need to upgrade
        extension = f"profile?symbol={symbol}&apikey={self.api_key}"
        url = self.base_url + extension
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch issuer information for {symbol}: {response.text}")
        data = response.json()
        if not data:
            raise Exception(f"No issuer information found for {symbol}")
        
        for entry in data:
            if entry.get("exchange") == exchange:
                break
        else:
            raise Exception(f"No issuer information found for {symbol} on exchange {exchange}")
        print(entry)
        
        return IssuerInfo(
            provider=self.provider,
            symbol=entry.get("symbol"),
            exchange=entry.get("exchange"),
            currency=entry.get("currency"),
            full_name=entry.get("companyName"),
            cik=entry.get("cik"),
        )
    
    # TODO: Currently assuming exchange is given correctly each time, will need to change if i do this globally
    def get_equity_information(self, symbol: str, exchange: str) -> EquityInfo:
        '''extension = f"search-exchange-variants?symbol={symbol}&apikey={self.api_key}"''' # Not offered need to upgrade
        extension = f"profile?symbol={symbol}&apikey={self.api_key}"
        url = self.base_url + extension
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch equity information for {symbol}: {response.text}")
        data = response.json()
        if not data:
            raise Exception(f"No equity information found for {symbol}")
        for entry in data:
            if entry.get("exchange") == exchange:
                break
        else:
            raise Exception(f"No equity information found for {symbol} on exchange {exchange}")
        print(entry)
        print(f"Equity Information for {symbol}:")
        print(f"  Sector: {entry.get('sector')}")
        print(f"  Industry: {entry.get('industry')}")
        print(f"  Dividend Yield: {entry.get('lastDividend')}")
        print(f"  Beta: {entry.get('beta')}")
        print(f"  Market Cap: {entry.get('marketCap')}")
        print(f"  CIK: {entry.get('cik')}")
        #TODO Change dividend yeidl to dividend amount W
        return EquityInfo(
            provider=self.provider,
            symbol=entry.get("symbol"),
            full_name=entry.get("companyName"),
            sector=entry.get("sector"),
            industry=entry.get("industry"),
            dividend_yield=entry.get("lastDividend"),
            beta=entry.get("beta"),
            market_cap=entry.get("marketCap"),
            cik=entry.get("cik"),
        )
    

    # Fundamental Data
    def get_income_statement(self, symbol: str, prev_years: int, period: str) -> json:
        extension = f"income-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        url = self.base_url + extension
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch income statement for {symbol}: {response.text}")
        
        data = response.json()
        if not data:
            raise Exception(f"No income statement data found for {symbol}")
        return data
    
    def get_balance_sheet(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:
        extension = f"balance-sheet-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        url = self.base_url + extension
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch balance sheet for {symbol}: {response.text}")
        
        data = response.json()
        if not data:
            raise Exception(f"No balance sheet data found for {symbol}")
        return data
    
    def get_cash_flow(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:
        extension = f"cash-flow-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        url = self.base_url + extension
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch cash flow for {symbol}: {response.text}")
        
        data = response.json()
        if not data:
            raise Exception(f"No cash flow data found for {symbol}")
        return data

        