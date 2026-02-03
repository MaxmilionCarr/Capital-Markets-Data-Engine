from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
from abc import ABC, abstractmethod
import pandas as pd

class Provider(IntEnum):
    YFINANCE = 1
    IBKR = 2
    

@dataclass(frozen=True)
class TickerInfo:
    symbol: str
    exchange: Optional[str]
    currency: Optional[str]
    full_name: Optional[str]
    sec_type: Optional[str]
    timezone: Optional[str]
    provider: Provider

@dataclass(frozen=True)
class BondInfo:
    CUSID: str
    issuer: Optional[str]
    coupon: Optional[float]
    maturity_date: Optional[str]
    credit_rating: Optional[str]
    currency: Optional[str]
    provider: Provider
    
@dataclass(frozen=True)
class EquityInfo:
    sector: Optional[str]
    industry: Optional[str]
    dividend_yield: Optional[float]
    pe_ratio: Optional[float]
    eps: Optional[float]
    beta: Optional[float]
    market_cap: Optional[float]
    
historical_prices_columns = ["datetime", "open", "high", "low", "close", "volume"]

class MarketDataProvider(ABC):
    
    @abstractmethod
    def connect(self):
        """Establish connection to the data provider."""
        pass

    @abstractmethod
    def disconnect(self):
        """Terminate connection to the data provider."""
        pass

    @abstractmethod
    def get_ticker_information(self, symbol: str) -> TickerInfo:
        """Fetch ticker information for the given symbol."""
        pass

    '''
    @abstractmethod
    def get_historical_prices(self, symbol: str, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch historical price data for the given symbol."""
        pass
    '''