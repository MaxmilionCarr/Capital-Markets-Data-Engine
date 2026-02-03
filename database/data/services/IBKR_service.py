from typing import List, Literal
from database.data.providers.IBKR_provider import IBKRProvider
from database.data.providers.base import TickerInfo, EquityInfo
import pandas as pd
from datetime import datetime, timedelta



class IBKRService:
    def __init__(self, IBKRProvider: IBKRProvider):
        self._client = IBKRProvider

    # Service Functions
    def fetch_ticker(self, symbol: str, exchange_name: str = None) -> TickerInfo | List[TickerInfo]:
        self._client.connect()
        info = self._client.get_ticker_information(symbol, exchange_name)
        self._client.disconnect()
        return info
    
    def fetch_equity(self, symbol: str, exchange_name: str = None, currency: str = None) -> EquityInfo:
        self._client.connect()
        info = self._client.get_equity_information(symbol, exchange_name, currency)
        self._client.disconnect()
        return info
    
    def fetch_equity_prices(self, symbol: str, exchange_name: str, start_date: datetime, end_date: datetime = None, bar_size: Literal["5 mins", "1 hour", "30 mins", "1 day"] = "1 day") -> pd.DataFrame:
        self._client.connect()
        df = self._client.get_equity_prices(symbol, exchange_name, start_date, end_date, bar_size)
        self._client.disconnect()
        return df

    def fetch_bond(self, symbol):
        self._client.connect()
        info = self._client.get_bond(symbol)
        self._client.disconnect()
        return info
    