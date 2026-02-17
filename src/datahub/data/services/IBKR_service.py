from typing import List, Literal
from datahub.data.providers.IBKR_provider import IBKRProvider
from datahub.data.providers.base import TickerInfo, EquityInfo
import pandas as pd
from datetime import datetime, time, timedelta



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
    
    def fetch_equity_prices(self, symbol: str, exchange_name: str, start_date: datetime, end_date: datetime = None, bar_size: Literal["5 mins", "1 hour", "1 day"] = "1 day", rth_open: time = None, rth_close: time = None) -> pd.DataFrame:
        self._client.connect()
        df = self._client.get_equity_prices(symbol, exchange_name, start_date, end_date, bar_size, rth_open=rth_open, rth_close=rth_close)
        self._client.disconnect()
        return df

    def fetch_bond(self, symbol):
        self._client.connect()
        info = self._client.get_bond(symbol)
        self._client.disconnect()
        return info
    