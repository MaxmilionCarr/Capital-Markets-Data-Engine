from __future__ import annotations
import requests

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Literal, Optional

import pandas as pd

from .base import FundamentalDataProvider, TickerInfo, EquityInfo

class FMPProvider(FundamentalDataProvider):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/stable/"
        
    def connect(self):
        # No persistent connection needed for FMP, but we can validate the API key
        pass
    
    def disconnect(self):
        pass
    
    def get_income_statement(self, symbol: str, prev_years: int, period: str) -> pd.DataFrame:

        extension = f"income-statement?symbol={symbol}&limit={prev_years}&period={period}&apikey={self.api_key}"
        url = self.base_url + extension
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch income statement for {symbol}: {response.text}")
        
        data = response.json()
        if not data:
            raise Exception(f"No income statement data found for {symbol}")
        return pd.DataFrame(data)
    
    def get_balance_sheet(self, symbol: str, prev_years: int) -> pd.DataFrame:
        pass
    
    def get_cash_flow(self, symbol):
        pass
        