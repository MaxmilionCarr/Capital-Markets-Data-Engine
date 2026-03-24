from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Sequence, Literal, Protocol
from .exceptions import NotSupported, ProviderError, DataNotFound
from data_providers.clients.base import EquityBuilder, IssuerBuilder, IssuerInfo, EquityInfo

# ---- types ----
StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
Period = Literal["annual", "quarterly"]

class BasicInfoService(Protocol):
    name: str

    def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo: ...
    def fetch_equity(self, symbol: str, exchange_name: str | None = None, currency: str | None = None) -> EquityInfo: ...


class PricingService(Protocol):
    name: str

    # TODO: FIX REQUIREMENTS FOR ARGUMENTS
    def fetch_equity_prices(self, args, kwargs): ...


class FundamentalService(Protocol):
    name: str
    def fetch_statement(self, symbol: str, statement_type: StatementType, prev_years: int, period: Period): ...


# ---- priority routers ----
@dataclass
class PriorityBasicInfo:
    services: Sequence[BasicInfoService]

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
class PriorityPricing:
    services: Sequence[PricingService]

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
        raise last or ProviderError("No pricing services configured")


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
    basic_info_services: Sequence[BasicInfoService] = field(default_factory=tuple)
    pricing_services: Sequence[PricingService] = field(default_factory=tuple)
    fundamental_services: Sequence[FundamentalService] = field(default_factory=tuple)
    # Backward-compatible alias: old callers can still pass market_services.
    market_services: Sequence[Any] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.basic_info_services and self.market_services:
            self.basic_info_services = tuple(self.market_services)

        if not self.pricing_services:
            candidates = tuple(self.market_services) + tuple(self.basic_info_services)
            first_pricing = next(
                (s for s in candidates if callable(getattr(s, "fetch_equity_prices", None))),
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
            "basic_info": [cls._service_descriptor(s) for s in config.basic_info_services],
            "pricing": [cls._service_descriptor(s) for s in config.pricing_services],
            "fundamental": [cls._service_descriptor(s) for s in config.fundamental_services],
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
            "pricing": self._hash_manifest("pricing", self.provider_manifest["pricing"]),
            "fundamental": self._hash_manifest("fundamental", self.provider_manifest["fundamental"]),
        }
        self.provider_identifiers["all"] = self._hash_manifest(
            "all",
            self.provider_manifest["basic_info"]
            + self.provider_manifest["pricing"]
            + self.provider_manifest["fundamental"],
        )
        # Backward-compatible alias
        self.provider_identifiers["market"] = self.provider_identifiers["basic_info"]
        # Backward compatible top-level identifier
        self.provider_identifier = self.provider_identifiers["all"]

        self.basic_info = PriorityBasicInfo(config.basic_info_services) if config.basic_info_services else None
        self.pricing = PriorityPricing(config.pricing_services) if config.pricing_services else None
        self.fundamentals = PriorityFundamentals(config.fundamental_services) if config.fundamental_services else None

        # Backward-compatible alias
        self.market = self.basic_info

    def require_basic_info(self) -> PriorityBasicInfo:
        if self.basic_info is None:
            raise ProviderError("No basic info services provided")
        return self.basic_info

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
