from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional
from abc import ABC, abstractmethod
import pandas as pd

class Builder(ABC):

    @abstractmethod
    def merge_data(self, info):
        pass

    @abstractmethod
    def is_complete(self) -> bool:
        pass

    @abstractmethod
    def missing_fields(self) -> List[str]:
        pass

class Provider(IntEnum):
    IBKR = 1
    MASSIVE = 2
    FMP = 3

@dataclass(frozen=True)
class FieldSpec:
    name: str
    provider_priority: List[Provider]
    required: bool = False

@dataclass(frozen=True)
class IssuerInfo:
    provider: Provider
    symbol: str
    exchange: Optional[str] = None
    currency: Optional[str] = None
    full_name: Optional[str] = None
    sec_type: Optional[str] = None
    timezone: Optional[str] = None
    rth_open: Optional[str] = None   # "HH:MM:SS"
    rth_close: Optional[str] = None  # "HH:MM:SS"
    cik: Optional[str] = None
    lei: Optional[str] = None

@dataclass(frozen=True)
class EquityInfo:
    provider: Provider
    symbol: str
    full_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    dividend_yield: Optional[float] = None
    pe_ratio: Optional[float] = None
    eps: Optional[float] = None
    beta: Optional[float] = None
    market_cap: Optional[float] = None
    cik: Optional[str] = None
    lei: Optional[str] = None

@dataclass(frozen=True)
class IssuerCapabilities:
    symbol: bool = False
    exchange: bool = False
    currency: bool = False
    full_name: bool = False
    sec_type: bool = False
    timezone: bool = False
    rth_open: bool = False
    rth_close: bool = False
    cik: bool = False
    lei: bool = False

@dataclass(frozen=True)
class EquityCapabilities:
    symbol: bool = False
    full_name: bool = False
    sector: bool = False
    industry: bool = False
    dividend_yield: bool = False
    pe_ratio: bool = False
    eps: bool = False
    beta: bool = False
    market_cap: bool = False
    cik: bool = False
    lei: bool = False

# TODO: This will be the orchestration layer for all different providers, will handle merging and filling instead of doing it in the service layer. (Makes for better abstraction)
class IssuerBuilder(Builder):
    and_or_required_fields = [["cik", "lei"]]

    PRIORITY_MAPPING = [
        FieldSpec(name="symbol", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="exchange", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="currency", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="full_name", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="sec_type", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="timezone", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="rth_open", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="rth_close", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="cik", provider_priority=[Provider.FMP]),
        FieldSpec(name="lei", provider_priority=[Provider.FMP])
    ]

    def __init__(self):
        self.symbol = None
        self.exchange = None
        self.currency = None
        self.full_name = None
        self.sec_type = None
        self.timezone = None
        self.rth_open = None
        self.rth_close = None
        self.cik = None
        self.lei = None

        self.field_sources = {spec.name: None for spec in self.PRIORITY_MAPPING}
    
    def merge_data(self, info: IssuerInfo):
        for field_spec in self.PRIORITY_MAPPING:
            if info.provider not in field_spec.provider_priority:
                continue  # Skip if provider is not in the priority list for this field
            value = getattr(info, field_spec.name)
            # Update None fields
            if value is not None and getattr(self, field_spec.name) is None:
                setattr(self, field_spec.name, value)
                self.field_sources[field_spec.name] = info.provider
            # Update existing fields if the new provider has higher priority
            elif value is not None and self.field_sources[field_spec.name] is not None:
                current_provider = self.field_sources[field_spec.name]
                if field_spec.provider_priority.index(info.provider) < field_spec.provider_priority.index(current_provider):
                    setattr(self, field_spec.name, value)
                    self.field_sources[field_spec.name] = info.provider
    
    def is_complete(self):
        for field_spec in self.PRIORITY_MAPPING:
            value = getattr(self, field_spec.name)
            if field_spec.required and value is None:
                return False
        # Check AND/OR conditions
        for group in self.and_or_required_fields:
            if not any(getattr(self, field) is not None for field in group):
                return False
        return True
    
    def missing_fields(self):
        return [
            spec.name
            for spec in self.PRIORITY_MAPPING
            if spec.required and getattr(self, spec.name) is None
        ]

# PROVIDE PRIORITY MAPPING FOR THIS AS WELL and merge function just like issuer
class EquityBuilder(Builder):
    and_or_required_fields = [["cik", "lei"]]

    PRIORITY_MAPPING = [
        FieldSpec(name="symbol", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="full_name", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="sector", provider_priority=[Provider.FMP], required=True),
        FieldSpec(name="industry", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="dividend_yield", provider_priority=[Provider.FMP, Provider.IBKR]),
        FieldSpec(name="pe_ratio", provider_priority=[Provider.FMP, Provider.IBKR]),
        FieldSpec(name="eps", provider_priority=[Provider.FMP, Provider.IBKR]),
        FieldSpec(name="beta", provider_priority=[Provider.FMP, Provider.IBKR]),
        FieldSpec(name="market_cap", provider_priority=[Provider.FMP, Provider.IBKR]),
        FieldSpec(name="cik", provider_priority=[Provider.FMP]),
        FieldSpec(name="lei", provider_priority=[Provider.FMP])
    ]
    def __init__(self):
        self.symbol = None
        self.full_name = None
        self.sector = None
        self.industry = None
        self.dividend_yield = None
        self.pe_ratio = None
        self.eps = None
        self.beta = None
        self.market_cap = None
        self.cik = None
        self.lei = None

        self.field_sources = {spec.name: None for spec in self.PRIORITY_MAPPING}

    def merge_data(self, info: EquityInfo):
        for field_spec in self.PRIORITY_MAPPING:
            if info.provider not in field_spec.provider_priority:
                continue  # Skip if provider is not in the priority list for this field
            value = getattr(info, field_spec.name)
            # Update None fields
            if value is not None and getattr(self, field_spec.name) is None:
                setattr(self, field_spec.name, value)
                self.field_sources[field_spec.name] = info.provider
            # Update existing fields if the new provider has higher priority
            elif value is not None and self.field_sources[field_spec.name] is not None:
                current_provider = self.field_sources[field_spec.name]
                if field_spec.provider_priority.index(info.provider) < field_spec.provider_priority.index(current_provider):
                    setattr(self, field_spec.name, value)
                    self.field_sources[field_spec.name] = info.provider
    
    def is_complete(self):
        for field_spec in self.PRIORITY_MAPPING:
            value = getattr(self, field_spec.name)
            if field_spec.required and value is None:
                return False
        # Check AND/OR conditions
        for group in self.and_or_required_fields:
            if not any(getattr(self, field) is not None for field in group):
                return False
        return True

    def missing_fields(self):
        return [
            spec.name
            for spec in self.PRIORITY_MAPPING
            if spec.required and getattr(self, spec.name) is None
        ]
    
    
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

    @abstractmethod
    def get_equity_information(self, symbol: str, exchange) -> EquityInfo:
        """Fetch equity information for the given symbol."""
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