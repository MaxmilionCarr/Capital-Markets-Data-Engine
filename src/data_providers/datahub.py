from __future__ import annotations

from dataclasses import dataclass, field, replace, is_dataclass
import datetime
from typing import Any, Optional, Sequence, Literal, Protocol
from .exceptions import NotSupported, ProviderError, DataNotFound
from data_providers.clients.base import EquityBuilder, IssuerBuilder, IssuerInfo, EquityInfo

# ---- types ----
StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
Period = Literal["annual", "quarterly"]

class MarketService(Protocol):
    name: str

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo: ...
    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo: ...
    # TODO: FIX REQUIREMENTS FOR ARGUMENTS
    def fetch_equity_prices(self, args, kwargs): ...


class FundamentalService(Protocol):
    name: str
    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period): ...


# ---- priority routers ----
@dataclass
class PriorityMarket:
    services: Sequence[MarketService]

    def fetch_issuer_enriched(self, symbol: str, exchange_name: str | None = None) -> IssuerBuilder:
        last: Exception | None = None
        base = IssuerBuilder()
        received_any_data = False

        for s in self.services:
            print("Trying", s.name)
            try:
                info = s.fetch_issuer(symbol, exchange_name)
                print("Got info", info)
                received_any_data = True
            except (NotSupported, DataNotFound, ProviderError) as e:
                raise(e)

            base.merge_data(info)
            print("Merged to base", base)

        if base.is_complete():
            print("Base is complete!")
            return base

        if not received_any_data:
            raise last or ProviderError("No market services provided valid issuer data")

        print("Base is missing required fields:", base.missing_fields())
        raise ProviderError(
            f"Issuer data incomplete for {symbol} ({exchange_name}). "
            f"Missing: {base.missing_fields()}"
        )

    # TODO: An enriched equity fetch as well
    def fetch_equity_enriched(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityBuilder:
        last: Exception | None = None
        base = EquityBuilder()
        received_any_data = False

        for s in self.services:
            print("Trying", s.name)
            try:
                info = s.fetch_equity(symbol, exchange_name, currency)
                print("Got info", info)
                received_any_data = True
            except (NotSupported, DataNotFound, ProviderError) as e:
                print(f"{s.name} failed to fetch equity: {e}")
                last = e
                continue

            base.merge_data(info)
            print("Merged to base", base)

        if base.is_complete():
            print("Base is complete!")
            return base

        if not received_any_data:
            raise last or ProviderError("No market services provided valid equity data")

        print("Base is missing required fields:", base.missing_fields())
        raise ProviderError(
            f"Equity data incomplete for {symbol} ({exchange_name}). "
            f"Missing: {base.missing_fields()}"
        )
    
    def fetch_equity_prices(self, *args, **kwargs):
        last: Exception | None = None
        for s in self.services:
            try:
                return s.fetch_equity_prices(*args, **kwargs)
            except (NotSupported, DataNotFound, ProviderError) as e:
                last = e
                continue
            except Exception as e:
                print(e)
                continue
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

    #TODO: base on priority reference for each service type
    def _compute_provider_identifier(config: DataHubConfig) -> str:
        # create a unique identifier for this combination of services
        pass

    def __init__(self, config: DataHubConfig):
        self.config = config
        self.provider_identifier = self._compute_provider_identifier()

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
