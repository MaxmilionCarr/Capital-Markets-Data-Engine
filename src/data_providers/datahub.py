from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Sequence, Literal, Protocol
from .exceptions import NotSupported, ProviderError, DataNotFound

# ---- types ----
StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
Period = Literal["annual", "quarterly"]

class MarketService(Protocol):
    name: str
    def fetch_ticker(self, symbol: str, exchange_name: str | None = None): ...
    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None): ...
    def fetch_equity_prices(self, *args, **kwargs): ...


class FundamentalService(Protocol):
    name: str
    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period): ...


# ---- priority routers ----
@dataclass
class PriorityMarket:
    services: Sequence[MarketService]

    def fetch_ticker(self, symbol: str, exchange_name: str | None = None):
        last: Exception | None = None
        for s in self.services:
            try:
                return s.fetch_ticker(symbol, exchange_name)
            except (NotSupported, DataNotFound, ProviderError) as e:
                last = e
        raise last or ProviderError("No market services configured")

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None):
        last: Exception | None = None
        for s in self.services:
            try:
                return s.fetch_equity(symbol, exchange_name, currency)
            except (NotSupported, DataNotFound, ProviderError) as e:
                last = e
        raise last or ProviderError("No market services configured")
    
    def fetch_equity_prices(self, *args, **kwargs):
        last: Exception | None = None
        for s in self.services:
            try:
                return s.fetch_equity_prices(*args, **kwargs)
            except (NotSupported, DataNotFound, ProviderError) as e:
                last = e
        raise last or ProviderError("No market services configured")


@dataclass
class PriorityFundamentals:
    services: Sequence[FundamentalService]

    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period):
        last: Exception | None = None
        for s in self.services:
            try:
                out = s.fetch_statement(symbol, statement_type, prev_years, period)

                # accept either list[dict] or DataFrame; treat empty as not found
                if out is None:
                    raise DataNotFound(f"{s.name}: returned None")
                if hasattr(out, "empty") and out.empty:  # pandas DataFrame
                    raise DataNotFound(f"{s.name}: returned empty")
                if isinstance(out, list) and len(out) == 0:
                    raise DataNotFound(f"{s.name}: returned empty list")

                return out

            except (NotSupported, DataNotFound, ProviderError) as e:
                last = e

        raise last or ProviderError("No fundamental services configured")


# ---- config: users pass service instances (preferred) ----
@dataclass
class DataHubConfig:
    market_services: Sequence[MarketService] = field(default_factory=tuple)
    fundamental_services: Sequence[FundamentalService] = field(default_factory=tuple)


# ---- DataHub: DB-free core ----
class DataHub:
    def __init__(self, config: DataHubConfig):
        self.config = config

        self.market = PriorityMarket(config.market_services) if config.market_services else None
        self.fundamentals = PriorityFundamentals(config.fundamental_services) if config.fundamental_services else None

    def require_market(self) -> PriorityMarket:
        if self.market is None:
            raise ProviderError("No market services provided")
        return self.market

    def require_fundamentals(self) -> PriorityFundamentals:
        if self.fundamentals is None:
            raise ProviderError("No fundamental services provided")
        return self.fundamentals
