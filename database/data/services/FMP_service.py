from typing import List, Literal
from database.data.providers.FMP_provider import FMPProvider
from database.data.providers.base import TickerInfo, EquityInfo
import pandas as pd
from datetime import datetime, time, timedelta



class FMPService:
    def __init__(self, FMPProvider: FMPProvider):
        self._client = FMPProvider

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
    