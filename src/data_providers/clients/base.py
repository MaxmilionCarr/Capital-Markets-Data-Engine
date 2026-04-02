from dataclasses import dataclass
from enum import IntEnum
from abc import ABC, abstractmethod
from datetime import datetime, time
from typing import Any, List, Optional

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
    cik: Optional[str] = None
    lei: Optional[str] = None


@dataclass(frozen=True)
class ExchangeInfo:
    provider: Provider
    exchange_name: str
    timezone: str
    currency: str
    rth_open: str   # "HH:MM:SS"
    rth_close: str  # "HH:MM:SS"


@dataclass(frozen=True)
class EquityInfo:
    provider: Provider
    symbol: str
    full_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    cik: Optional[str] = None
    lei: Optional[str] = None
    dividend_yield: Optional[float] = None


historical_prices_columns = ["datetime", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class HistoricalPriceInfo:
    provider: Provider
    symbol: str
    historical_prices: pd.DataFrame


@dataclass(frozen=True)
class IssuerCapabilities:
    symbol: bool = False
    exchange: bool = False
    currency: bool = False
    full_name: bool = False
    cik: bool = False
    lei: bool = False


@dataclass(frozen=True)
class ExchangeCapabilities:
    timezone: bool = False
    rth_open: bool = False
    rth_close: bool = False


@dataclass(frozen=True)
class EquityCapabilities:
    symbol: bool = False
    full_name: bool = False
    sector: bool = False
    industry: bool = False
    cik: bool = False
    lei: bool = False


# TODO: This will be the orchestration layer for all different providers, will handle merging and filling instead of doing it in the service layer. (Makes for better abstraction)
class IssuerBuilder(Builder):
    PRIORITY_MAPPING = [
        FieldSpec(name="symbol", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="exchange", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="currency", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="full_name", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="cik", provider_priority=[Provider.FMP]),
        FieldSpec(name="lei", provider_priority=[Provider.FMP]),
    ]

    def __init__(self):
        self.symbol = None
        self.exchange = None
        self.currency = None
        self.full_name = None
        self.cik = None
        self.lei = None

        self.field_sources: dict[str, Provider | None] = {spec.name: None for spec in self.PRIORITY_MAPPING}

    def merge_data(self, info: IssuerInfo):
        for field_spec in self.PRIORITY_MAPPING:
            if info.provider not in field_spec.provider_priority:
                continue
            value = getattr(info, field_spec.name)
            if value is not None and getattr(self, field_spec.name) is None:
                setattr(self, field_spec.name, value)
                self.field_sources[field_spec.name] = info.provider
            elif value is not None and self.field_sources[field_spec.name] is not None:
                current_provider = self.field_sources[field_spec.name]
                if current_provider is not None and field_spec.provider_priority.index(info.provider) < field_spec.provider_priority.index(current_provider):
                    setattr(self, field_spec.name, value)
                    self.field_sources[field_spec.name] = info.provider

    def is_complete(self):
        for field_spec in self.PRIORITY_MAPPING:
            value = getattr(self, field_spec.name)
            if field_spec.required and value is None:
                return False
        return True

    def missing_fields(self):
        return [
            spec.name
            for spec in self.PRIORITY_MAPPING
            if spec.required and getattr(self, spec.name) is None
        ]


class ExchangeBuilder(Builder):
    PRIORITY_MAPPING = [
        FieldSpec(name="exchange_name", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="timezone", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="currency", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="rth_open", provider_priority=[Provider.IBKR], required=True),
        FieldSpec(name="rth_close", provider_priority=[Provider.IBKR], required=True),
    ]

    def __init__(self):
        self.exchange_name = None
        self.timezone = None
        self.currency = None
        self.rth_open = None
        self.rth_close = None

        self.field_sources: dict[str, Provider | None] = {spec.name: None for spec in self.PRIORITY_MAPPING}

    def merge_data(self, info: ExchangeInfo):
        for field_spec in self.PRIORITY_MAPPING:
            if info.provider not in field_spec.provider_priority:
                continue
            value = getattr(info, field_spec.name)
            if value is not None and getattr(self, field_spec.name) is None:
                setattr(self, field_spec.name, value)
                self.field_sources[field_spec.name] = info.provider
            elif value is not None and self.field_sources[field_spec.name] is not None:
                current_provider = self.field_sources[field_spec.name]
                if current_provider is not None and field_spec.provider_priority.index(info.provider) < field_spec.provider_priority.index(current_provider):
                    setattr(self, field_spec.name, value)
                    self.field_sources[field_spec.name] = info.provider

    def is_complete(self):
        for field_spec in self.PRIORITY_MAPPING:
            value = getattr(self, field_spec.name)
            if field_spec.required and value is None:
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
    PRIORITY_MAPPING = [
        FieldSpec(name="symbol", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="full_name", provider_priority=[Provider.IBKR, Provider.FMP], required=True),
        FieldSpec(name="sector", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="industry", provider_priority=[Provider.FMP, Provider.IBKR], required=True),
        FieldSpec(name="cik", provider_priority=[Provider.FMP]),
        FieldSpec(name="lei", provider_priority=[Provider.FMP]),
    ]

    def __init__(self):
        self.symbol = None
        self.full_name = None
        self.sector = None
        self.industry = None
        self.cik = None
        self.lei = None

        self.field_sources: dict[str, Provider | None] = {spec.name: None for spec in self.PRIORITY_MAPPING}

    def merge_data(self, info: EquityInfo):
        for field_spec in self.PRIORITY_MAPPING:
            if info.provider not in field_spec.provider_priority:
                continue
            value = getattr(info, field_spec.name)
            if value is not None and getattr(self, field_spec.name) is None:
                setattr(self, field_spec.name, value)
                self.field_sources[field_spec.name] = info.provider
            elif value is not None and self.field_sources[field_spec.name] is not None:
                current_provider = self.field_sources[field_spec.name]
                if current_provider is not None and field_spec.provider_priority.index(info.provider) < field_spec.provider_priority.index(current_provider):
                    setattr(self, field_spec.name, value)
                    self.field_sources[field_spec.name] = info.provider

    def is_complete(self):
        for field_spec in self.PRIORITY_MAPPING:
            value = getattr(self, field_spec.name)
            if field_spec.required and value is None:
                return False
        return True

    def missing_fields(self):
        return [
            spec.name
            for spec in self.PRIORITY_MAPPING
            if spec.required and getattr(self, spec.name) is None
        ]


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
    def get_issuer_information(self, symbol: str, exchange: str) -> IssuerInfo:
        """Fetch issuer information for the given symbol."""
        pass

    @abstractmethod
    def get_equity_information(self, symbol: str, exchange) -> EquityInfo:
        """Fetch equity information for the given symbol."""
        pass

    def get_exchange_information(self, symbol: str, exchange: str) -> ExchangeInfo:
        """Fetch exchange information. Only some providers implement this."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support exchange information fetching")

    @abstractmethod
    def get_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        bar_size: str = "1 day",
        *,
        rth_open: Optional[time] = None,
        rth_close: Optional[time] = None,
    ) -> pd.DataFrame:
        """Fetch historical equity prices using the canonical OHLCV dataframe schema."""
        pass

    def get_historical_prices(self, *args, **kwargs) -> pd.DataFrame:
        """Backward-compatible alias for get_equity_prices."""
        return self.get_equity_prices(*args, **kwargs)


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
    def get_income_statement(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        """Fetch income statement data for the given symbol."""
        pass

    @abstractmethod
    def get_balance_sheet(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        """Fetch balance sheet data for the given symbol."""
        pass

    @abstractmethod
    def get_cash_flow(self, symbol: str, prev_years: int, period: str) -> list[dict[str, Any]]:
        """Fetch cash flow data for the given symbol."""
        pass
