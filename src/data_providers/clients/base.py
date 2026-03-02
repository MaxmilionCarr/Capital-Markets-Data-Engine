from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
from abc import ABC, abstractmethod
import pandas as pd

class Provider(IntEnum):
    YFINANCE = 1
    IBKR = 2
    MASSIVE = 3
    FMP = 4
    

@dataclass(frozen=True)
class IssuerInfo:
    symbol: str
    exchange: Optional[str]
    currency: Optional[str]
    full_name: Optional[str]
    sec_type: Optional[str]
    timezone: Optional[str]
    provider: Provider
    rth_open: Optional[str] = None   # "HH:MM:SS"
    rth_close: Optional[str] = None  # "HH:MM:SS"
    cik: Optional[str] = None
    lei: Optional[str] = None
    
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
    def get_issuer_information(self, symbol: str) -> IssuerInfo:
        """Fetch issuer information for the given symbol."""
        pass

    '''
    @abstractmethod
    def get_historical_prices(self, symbol: str, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch historical price data for the given symbol."""
        pass
    '''
    
class FundamentalDataProvider(ABC):
    
    @abstractmethod
    def connect(self):
        """Establish connection to the data provider."""
        pass

    @abstractmethod
    def disconnect(self):
        """Terminate connection to the data provider."""
        pass

    @abstractmethod
    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """Fetch income statement data for the given symbol."""
        pass
    
    @abstractmethod
    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """Fetch balance sheet data for the given symbol."""
        pass
    
    @abstractmethod
    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        """Fetch cash flow data for the given symbol."""
        pass