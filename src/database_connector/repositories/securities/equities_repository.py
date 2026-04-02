from __future__ import annotations

import sqlite3 as sql
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, List, TYPE_CHECKING

import pandas as pd
from typing_extensions import Literal

from database_connector.db import Hub
from database_connector.repositories.fundamental_data.statements_repository import Statement

if TYPE_CHECKING:
    from data_providers.clients.base import EquityInfo
from database_connector.repositories.core.issuer_repository import Issuer

STATEMENTS = Literal["income_statement", "balance_sheet", "cash_flow"]

# Standardise security types across providers
SECURITY_TYPES = {
    "EQUITY": ["STK"],
    "BOND": ["BOND"],
    # Add more as needed
}

# TODO: need to add an enriched state if all fields are filled so ensure can happily skip over or fill if fields are missing that current providers do not provide
# TODO: Add override for get_or_create_ensure or make get_or_create_override that forces overwrite of existing data with new data from provider, for cases where provider data is more complete than existing data and we want to update it. Or add an enrichment method that fills in missing fields without overwriting existing fields. Need to be careful with this though as it can lead to data quality issues if providers have conflicting data. Maybe best to just have a manual review process for updating existing data rather than automatic overwriting.

@dataclass
class Equity:
    equity_id: int
    issuer_id: int
    exchange_id: int

    symbol: str
    full_name: str | None
    sector: str | None
    industry: str | None

    _hub: Hub

    @cached_property
    def exchange(self):
        return self._hub.exchange_repo.get_info(exchange_id=self.exchange_id)
    
    @cached_property
    def issuer(self) -> Issuer | None:
        return self._hub.issuer_repo.get_info(issuer_id=self.issuer_id)

    def get_market_data(self) -> "EquityInfo":
        exchange = self.exchange
        return self._hub.basic_info_service.fetch_equity(
            self.symbol,
            getattr(exchange, "name", None),
            getattr(exchange, "currency", None),
        )

    def get_prices(
        self,
        start_date: datetime,
        end_date: datetime | None = None,
        *,
        period: Literal["5 mins", "1 hour", "1 day"] = "1 day",
        ensure: bool = False,
    ) -> pd.DataFrame:
        repo = self._hub.equity_prices_repo
        if not ensure:
            return repo.get_prices(
                equity=self,
                period=period,
                start=start_date,
                end=end_date or datetime.now(),
            )
        return repo.get_or_create_ensure(
            equity=self,
            period=period,
            start_date=start_date,
            end_date=end_date or datetime.now(),
        )

    def get_statements(
        self,
        statement_type: STATEMENTS,
        period: Literal["annual", "quarterly"],
        look_back: int = 0,
        *,
        ensure: bool = False,
    ) -> Optional[List[Statement]]:
        """
        Convenience: statements are on the issuer, but equities often call this.
        """
        issuer = self.issuer
        if issuer is None:
            return None
        return issuer.get_statements(
            statement_type=statement_type,
            period=period,
            look_back=look_back,
            ensure=ensure,
        )
    
class EquitiesRepository:
    """
    Data-access layer for equities table.

    Schema:
        equities(
            equity_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            full_name TEXT,
            sector TEXT,
            industry TEXT,
            provider_identifier TEXT,
            FOREIGN KEY (issuer_id) REFERENCES issuers(issuer_id) ON DELETE CASCADE,
            FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
        )

    Uniqueness:
        UNIQUE(exchange_id, symbol)
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub

    # ---------- READ ----------

    def _row_to_equity(self, row) -> Equity:

        return Equity(
            equity_id=row[0],
            issuer_id=row[1],
            exchange_id=row[2],
            symbol=row[3],
            full_name=row[4],
            sector=row[5],
            industry=row[6],
            _hub=self.hub,
        )
    

    def get_by_exchange_symbol(self, *, exchange_id: int, symbol: str) -> Equity | None:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry "
            "FROM equities WHERE exchange_id = ? AND symbol = ?",
            (exchange_id, symbol),
        )
        row = cur.fetchone()
        return None if not row else self._row_to_equity(row)

    def get_by_id(self, equity_id: int) -> Equity | None:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry "
            "FROM equities WHERE equity_id = ?",
            (equity_id,),
        )
        row = cur.fetchone()
        return None if not row else self._row_to_equity(row)

    def get_by_issuer(self, issuer_id: int) -> List[Equity]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry "
            "FROM equities WHERE issuer_id = ?",
            (issuer_id,),
        )
        return [self._row_to_equity(r) for r in cur.fetchall()]

    def get_by_exchange(self, exchange_id: int) -> List[Equity]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry "
            "FROM equities WHERE exchange_id = ?",
            (exchange_id,),
        )
        return [self._row_to_equity(r) for r in cur.fetchall()]

    # ---------- CREATE / UPSERT ----------

    def create(
        self,
        *,
        issuer_id: int,
        exchange_id: int,
        symbol: str,
        full_name: str | None = None,
        sector: str | None = None,
        industry: str | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        cur = self.hub.db_service.execute(
            "INSERT INTO equities (issuer_id, exchange_id, symbol, full_name, sector, industry, provider_identifier) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                issuer_id,
                exchange_id,
                symbol,
                full_name,
                sector,
                industry,
                provider_identifier,
            ),
        )
        self.hub.db_service.commit()
        return int(cur.lastrowid)

    def upsert_by_exchange_symbol(
        self,
        *,
        issuer_id: int,
        exchange_id: int,
        symbol: str,
        full_name: str | None = None,
        sector: str | None = None,
        industry: str | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        """
        Upsert for the natural key (exchange_id, symbol).
        Requires SQLite 3.24+ (ON CONFLICT DO UPDATE).
        """
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        upsert_sql = self.hub.db_service.build_upsert(
            table="equities",
            columns=(
                "issuer_id",
                "exchange_id",
                "symbol",
                "full_name",
                "sector",
                "industry",
                "provider_identifier",
            ),
            conflict_columns=("exchange_id", "symbol"),
            update_columns=(
                "issuer_id",
                "full_name",
                "sector",
                "industry",
                "provider_identifier",
            ),
            coalesce_update_columns=(
                "full_name",
                "sector",
                "industry",
            ),
        )

        self.hub.db_service.execute(
            upsert_sql,
            (
                issuer_id,
                exchange_id,
                symbol,
                full_name,
                sector,
                industry,
                provider_identifier,
            ),
        )
        self.hub.db_service.commit()

        # If inserted, lastrowid is new equity_id; if updated, lastrowid can be 0-ish.
        # Return the actual equity_id deterministically:
        eq = self.get_by_exchange_symbol(exchange_id=exchange_id, symbol=symbol)
        if not eq:
            raise sql.Error("Upsert failed to return equity")
        return eq.equity_id

    # ---------- ENSURE (high-level convenience) ----------

    def get_or_create_ensure(self, *, symbol: str, exchange_name: str) -> Equity | None:
        """
        Get or create an equity, minimizing API calls through intelligent caching.
        
        Flow:
        1. Check if equity exists in DB (by exchange + symbol) → return immediately
        2. Check if exchange exists in DB → fetch from API only if missing
        3. Check if issuer exists in DB → fetch from API only if missing
        4. Create or update equity in DB
        """
        exchange_name = exchange_name.strip()
        provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        # Step 0.5: Get exchange (may be None)
        ex = self.hub.exchange_repo.get_info(exchange_name=exchange_name)

        # Step 1: Check if equity already exists in database (by exchange_name + symbol)
        # This is the PRIMARY CACHE CHECK - avoids all API calls if equity is already known
        if ex:
            existing_equity = self.get_by_exchange_symbol(exchange_id=ex.exchange_id, symbol=symbol)
            if existing_equity:
                return existing_equity

        # Step 2: Exchange doesn't exist, fetch from provider and create it
        if not ex:
            exchange_enriched = self.hub.exchange_service.fetch_exchange_enriched(
                symbol=symbol,
                exchange_name=exchange_name
            )

            ex_id = self.hub.exchange_repo.get_or_create(
                exchange_name=exchange_name,
                timezone=exchange_enriched.timezone,
                currency=exchange_enriched.currency,
                rth_open=exchange_enriched.rth_open,
                rth_close=exchange_enriched.rth_close,
                provider_identifier=provider_identifier,
            )

            ex = self.hub.exchange_repo.get_info(exchange_id=ex_id)

            if not ex:
                return None

        # Step 3: Fetch issuer information from provider
        issuer_enriched = self.hub.basic_info_service.fetch_issuer_enriched(
            symbol=symbol,
            exchange_name=exchange_name
        )

        # Step 4: Fetch equity enrichment from provider
        equity_enriched = self.hub.basic_info_service.fetch_equity_enriched(
            symbol=symbol,
            exchange_name=exchange_name,
            currency=ex.currency
        )

        # Step 5: Resolve or create issuer.
        # If cik/lei are unavailable (common for non-US instruments),
        # use existing symbol linkage as the fallback identity path.
        if issuer_enriched.cik is None and issuer_enriched.lei is None:
            existing_issuer = self.hub.issuer_repo.get_info(main_symbol=symbol)
            if existing_issuer is not None:
                issuer_id = existing_issuer.issuer_id
                self.hub.issuer_repo.upsert(
                    issuer_id,
                    main_symbol=symbol,
                    full_name=issuer_enriched.full_name,
                    provider_identifier=provider_identifier,
                )
            else:
                issuer_id = self.hub.issuer_repo.get_or_create(
                    main_symbol=symbol,
                    full_name=issuer_enriched.full_name,
                    provider_identifier=provider_identifier,
                )
        else:
            issuer_id = self.hub.issuer_repo.get_or_create(
                main_symbol=symbol,
                full_name=issuer_enriched.full_name,
                cik=issuer_enriched.cik,
                lei=issuer_enriched.lei,
                provider_identifier=provider_identifier,
            )

        # Step 6: Create or update equity
        equity_id = self.upsert_by_exchange_symbol(
            issuer_id=issuer_id,
            exchange_id=ex.exchange_id,
            symbol=equity_enriched.symbol,
            full_name=equity_enriched.full_name,
            sector=equity_enriched.sector,
            industry=equity_enriched.industry,
            provider_identifier=provider_identifier,
        )

        return self.get_by_id(equity_id=equity_id)