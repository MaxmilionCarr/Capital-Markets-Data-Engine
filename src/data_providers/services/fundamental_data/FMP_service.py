from typing import List, Literal
from data_providers.clients import FMPConfig, FMPProvider
from data_providers.clients.base import TickerInfo, EquityInfo
#from data_providers.exceptions import NotSupported, ProviderError, DataNotFound
import pandas as pd
from datetime import datetime, time, timedelta

class FMPService:
    def __init__(self, config: FMPConfig):
        self._client = FMPProvider(config)

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
    