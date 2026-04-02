from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
import hashlib
import json
import logging
from typing import Any, Literal, Protocol, Sequence, cast

import pandas as pd

from data_providers.clients.base import EquityBuilder, EquityInfo, IssuerBuilder, IssuerInfo, ExchangeBuilder, ExchangeInfo
from .exceptions import DataNotFound, NotSupported, ProviderError

logger = logging.getLogger(__name__)

# ---- types ----
StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
Period = Literal["annual", "quarterly"]


class BasicInfoService(Protocol):
    name: str

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo: ...
    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo: ...
    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open: time | None = None,
        rth_close: time | None = None,
    ) -> pd.DataFrame: ...


class ExchangeService(Protocol):
    name: str

    def fetch_exchange(self, symbol: str, exchange_name: str) -> ExchangeInfo: ...


class PricingService(Protocol):
    name: str

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open: time | None = None,
        rth_close: time | None = None,
    ) -> pd.DataFrame: ...


class FundamentalService(Protocol):
    name: str

    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period): ...


# ---- priority routers ----
@dataclass
class PriorityBasicInfo:
    services: Sequence[BasicInfoService]

    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo:
        last: Exception | None = None

        for service in self.services:
            snapshot_fetch = getattr(service, "fetch_equity_snapshot", None)
            if callable(snapshot_fetch):
                try:
                    return cast(EquityInfo, snapshot_fetch(symbol, exchange_name, currency))
                except (NotSupported, DataNotFound, ProviderError) as error:
                    last = error
                    continue

        for service in self.services:
            try:
                return service.fetch_equity(symbol, exchange_name, currency)
            except (NotSupported, DataNotFound, ProviderError) as error:
                last = error
                continue

        raise last or ProviderError("No market services configured")

    def fetch_issuer_enriched(self, symbol: str, exchange_name: str | None = None) -> IssuerBuilder:
        last: Exception | None = None
        base = IssuerBuilder()
        received_any_data = False

        for service in self.services:
            logger.debug("Trying issuer fetch via %s", service.name)
            try:
                info = service.fetch_issuer(symbol, exchange_name)
                logger.debug("Issuer fetch succeeded via %s", service.name)
                received_any_data = True
            except (NotSupported, DataNotFound, ProviderError) as error:
                last = error
                continue

            base.merge_data(info)
            logger.debug("Merged issuer data into builder")

        if base.is_complete():
            logger.debug("Issuer builder is complete for %s", symbol)
            return base

        if not received_any_data:
            raise last or ProviderError("No market services provided valid issuer data")

        logger.debug("Issuer builder missing required fields: %s", base.missing_fields())
        raise ProviderError(
            f"Issuer data incomplete for {symbol} ({exchange_name}). "
            f"Missing: {base.missing_fields()}"
        )

    def fetch_equity_enriched(
        self,
        symbol: str,
        exchange_name: str | None = None,
        currency: str | None = None,
    ) -> EquityBuilder:
        last: Exception | None = None
        base = EquityBuilder()
        received_any_data = False

        for service in self.services:
            logger.debug("Trying equity fetch via %s", service.name)
            try:
                info = service.fetch_equity(symbol, exchange_name, currency)
                logger.debug("Equity fetch succeeded via %s", service.name)
                received_any_data = True
            except (NotSupported, DataNotFound, ProviderError) as error:
                logger.debug("%s failed to fetch equity: %s", service.name, error)
                last = error
                continue

            base.merge_data(info)
            logger.debug("Merged equity data into builder")

        if base.is_complete():
            logger.debug("Equity builder is complete for %s", symbol)
            return base

        if not received_any_data:
            raise last or ProviderError("No market services provided valid equity data")

        logger.debug("Equity builder missing required fields: %s", base.missing_fields())
        raise ProviderError(
            f"Equity data incomplete for {symbol} ({exchange_name}). "
            f"Missing: {base.missing_fields()}"
        )

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open: time | None = None,
        rth_close: time | None = None,
    ) -> pd.DataFrame:
        last: Exception | None = None
        for service in self.services:
            try:
                return service.fetch_equity_prices(
                    symbol,
                    exchange_name=exchange_name,
                    start_date=start_date,
                    end_date=end_date,
                    bar_size=bar_size,
                    rth_open=rth_open,
                    rth_close=rth_close,
                )
            except (NotSupported, DataNotFound, ProviderError) as error:
                last = error
                continue
            except Exception:
                logger.exception("Unhandled error while fetching equity prices from basic info service")
                continue
        raise last or ProviderError("No market services configured")


@dataclass
class PriorityExchangeInfo:
    services: Sequence[ExchangeService]

    def fetch_exchange_enriched(self, symbol: str, exchange_name: str) -> ExchangeBuilder:
        last: Exception | None = None
        base = ExchangeBuilder()
        received_any_data = False

        for service in self.services:
            logger.debug("Trying exchange fetch via %s", service.name)
            try:
                info = service.fetch_exchange(symbol, exchange_name)
                logger.debug("Exchange fetch succeeded via %s", service.name)
                received_any_data = True
            except (NotSupported, DataNotFound, ProviderError) as error:
                logger.debug("%s failed to fetch exchange: %s", service.name, error)
                last = error
                continue

            base.merge_data(info)
            logger.debug("Merged exchange data into builder")

        if base.is_complete():
            logger.debug("Exchange builder is complete for %s", exchange_name)
            return base

        if not received_any_data:
            raise last or ProviderError("No exchange services provided valid data")

        logger.debug("Exchange builder missing required fields: %s", base.missing_fields())
        raise ProviderError(
            f"Exchange data incomplete for {exchange_name}. "
            f"Missing: {base.missing_fields()}"
        )


@dataclass
class PriorityPricing:
    services: Sequence[PricingService]

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: datetime | None = None,
        bar_size: str = "1 day",
        *,
        rth_open: time | None = None,
        rth_close: time | None = None,
    ) -> pd.DataFrame:
        last: Exception | None = None
        for service in self.services:
            try:
                return service.fetch_equity_prices(
                    symbol,
                    exchange_name=exchange_name,
                    start_date=start_date,
                    end_date=end_date,
                    bar_size=bar_size,
                    rth_open=rth_open,
                    rth_close=rth_close,
                )
            except (NotSupported, DataNotFound, ProviderError) as error:
                last = error
                continue
            except Exception:
                logger.exception("Unhandled error while fetching equity prices from pricing service")
                continue
        raise last or ProviderError("No pricing services configured")


@dataclass
class PriorityFundamentals:
    services: Sequence[FundamentalService]

    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period):
        last: Exception | None = None
        for service in self.services:
            try:
                out = service.fetch_statement(symbol, statement_type, prev_years, period)

                # accept either list[dict] or DataFrame; treat empty as not found
                if out is None:
                    raise DataNotFound(f"{service.name}: returned None")
                if hasattr(out, "empty") and out.empty:
                    raise DataNotFound(f"{service.name}: returned empty")
                if isinstance(out, list) and len(out) == 0:
                    raise DataNotFound(f"{service.name}: returned empty list")

                return out

            except (NotSupported, DataNotFound, ProviderError) as error:
                last = error

        raise last or ProviderError("No fundamental services configured")


# ---- config: users pass service instances (preferred) ----
@dataclass
class DataHubConfig:
    basic_info_services: Sequence[BasicInfoService] = field(default_factory=tuple)
    exchange_services: Sequence[ExchangeService] = field(default_factory=tuple)
    pricing_services: Sequence[PricingService] = field(default_factory=tuple)
    fundamental_services: Sequence[FundamentalService] = field(default_factory=tuple)
    # Backward-compatible alias: old callers can still pass market_services.
    market_services: Sequence[Any] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.basic_info_services and self.market_services:
            self.basic_info_services = tuple(self.market_services)

        if not self.exchange_services and self.market_services:
            self.exchange_services = tuple(self.market_services)

        if not self.pricing_services:
            candidates = tuple(self.market_services) + tuple(self.basic_info_services)
            first_pricing = next(
                (service for service in candidates if callable(getattr(service, "fetch_equity_prices", None))),
                None,
            )
            if first_pricing is not None:
                self.pricing_services = (first_pricing,)

        if len(self.pricing_services) > 1:
            raise ValueError(
                "pricing_services must contain exactly one provider for deterministic pricing provenance"
            )


# ---- DataHub: DB-free core ----
class DataHub:

    @staticmethod
    def _service_descriptor(service: Any) -> dict[str, str]:
        cls = service.__class__
        return {
            "name": getattr(service, "name", cls.__name__),
            "module": cls.__module__,
            "class": cls.__name__,
        }

    @classmethod
    def _service_manifest(cls, config: DataHubConfig) -> dict[str, list[dict[str, str]]]:
        return {
            "basic_info": [cls._service_descriptor(service) for service in config.basic_info_services],
            "exchange": [cls._service_descriptor(service) for service in config.exchange_services],
            "pricing": [cls._service_descriptor(service) for service in config.pricing_services],
            "fundamental": [cls._service_descriptor(service) for service in config.fundamental_services],
        }

    @staticmethod
    def _hash_manifest(scope: str, providers: list[dict[str, str]]) -> str:
        payload = {"scope": scope, "providers": providers}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        return f"prov_{scope}_{digest}"

    def __init__(self, config: DataHubConfig):
        self.config = config
        self.provider_manifest = self._service_manifest(config)
        self.provider_identifiers = {
            "basic_info": self._hash_manifest("basic_info", self.provider_manifest["basic_info"]),
            "exchange": self._hash_manifest("exchange", self.provider_manifest["exchange"]),
            "pricing": self._hash_manifest("pricing", self.provider_manifest["pricing"]),
            "fundamental": self._hash_manifest("fundamental", self.provider_manifest["fundamental"]),
        }
        self.provider_identifiers["all"] = self._hash_manifest(
            "all",
            self.provider_manifest["basic_info"]
            + self.provider_manifest["exchange"]
            + self.provider_manifest["pricing"]
            + self.provider_manifest["fundamental"],
        )
        # Backward-compatible alias
        self.provider_identifiers["market"] = self.provider_identifiers["basic_info"]
        # Backward compatible top-level identifier
        self.provider_identifier = self.provider_identifiers["all"]

        self.basic_info = PriorityBasicInfo(config.basic_info_services) if config.basic_info_services else None
        self.exchange = PriorityExchangeInfo(config.exchange_services) if config.exchange_services else None
        self.pricing = PriorityPricing(config.pricing_services) if config.pricing_services else None
        self.fundamentals = PriorityFundamentals(config.fundamental_services) if config.fundamental_services else None

        # Backward-compatible alias
        self.market = self.basic_info

    def require_basic_info(self) -> PriorityBasicInfo:
        if self.basic_info is None:
            raise ProviderError("No basic info services provided")
        return self.basic_info

    def require_exchange(self) -> PriorityExchangeInfo:
        if self.exchange is None:
            raise ProviderError("No exchange services provided")
        return self.exchange

    def require_pricing(self) -> PriorityPricing:
        if self.pricing is None:
            raise ProviderError("No pricing services provided")
        return self.pricing

    def require_market(self) -> PriorityBasicInfo:
        # Backward-compatible alias
        return self.require_basic_info()

    def require_fundamentals(self) -> PriorityFundamentals:
        if self.fundamentals is None:
            raise ProviderError("No fundamental services provided")
        return self.fundamentals
