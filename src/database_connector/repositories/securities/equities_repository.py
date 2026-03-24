from __future__ import annotations

import sqlite3 as sql
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, List

import pandas as pd
from typing_extensions import Literal

from database_connector.db import Hub
from database_connector.repositories.fundamental_data.statements_repository import Statement
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
    dividend_yield: float | None
    pe_ratio: float | None
    eps: float | None
    beta: float | None
    market_cap: float | None

    _hub: Hub

    @cached_property
    def exchange(self):
        return self._hub.exchange_repo.get_info(exchange_id=self.exchange_id)
    
    @cached_property
    def issuer(self) -> Issuer | None:
        return self._hub.issuer_repo.get_info(issuer_id=self.issuer_id)

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
            dividend_yield REAL,
            pe_ratio REAL,
            eps REAL,
            beta REAL,
            market_cap REAL,
            FOREIGN KEY (issuer_id) REFERENCES issuers(issuer_id) ON DELETE CASCADE,
            FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
        )

    Uniqueness:
        UNIQUE(exchange_id, symbol)
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

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
            dividend_yield=row[7],
            pe_ratio=row[8],
            eps=row[9],
            beta=row[10],
            market_cap=row[11],
            _hub=self.hub,
        )
    

    def get_by_exchange_symbol(self, *, exchange_id: int, symbol: str) -> Equity | None:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap "
            "FROM equities WHERE exchange_id = ? AND symbol = ?",
            (exchange_id, symbol),
        )
        row = cur.fetchone()
        return None if not row else self._row_to_equity(row)

    def get_by_id(self, equity_id: int) -> Equity | None:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap "
            "FROM equities WHERE equity_id = ?",
            (equity_id,),
        )
        row = cur.fetchone()
        return None if not row else self._row_to_equity(row)

    def get_by_issuer(self, issuer_id: int) -> List[Equity]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap "
            "FROM equities WHERE issuer_id = ?",
            (issuer_id,),
        )
        return [self._row_to_equity(r) for r in cur.fetchall()]

    def get_by_exchange(self, exchange_id: int) -> List[Equity]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap "
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
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equities (issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap, provider_identifier) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                issuer_id,
                exchange_id,
                symbol,
                full_name,
                sector,
                industry,
                dividend_yield,
                pe_ratio,
                eps,
                beta,
                market_cap,
                provider_identifier,
            ),
        )
        self.connection.commit()
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
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        """
        Upsert for the natural key (exchange_id, symbol).
        Requires SQLite 3.24+ (ON CONFLICT DO UPDATE).
        """
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        cur = self.connection.cursor()
        cur.execute(
            """
            INSERT INTO equities (issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap, provider_identifier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(exchange_id, symbol) DO UPDATE SET
                issuer_id = excluded.issuer_id,
                full_name = COALESCE(excluded.full_name, equities.full_name),
                sector = COALESCE(excluded.sector, equities.sector),
                industry = COALESCE(excluded.industry, equities.industry),
                dividend_yield = COALESCE(excluded.dividend_yield, equities.dividend_yield),
                pe_ratio = COALESCE(excluded.pe_ratio, equities.pe_ratio),
                eps = COALESCE(excluded.eps, equities.eps),
                beta = COALESCE(excluded.beta, equities.beta),
                market_cap = COALESCE(excluded.market_cap, equities.market_cap),
                provider_identifier = excluded.provider_identifier
            """,
            (
                issuer_id,
                exchange_id,
                symbol,
                full_name,
                sector,
                industry,
                dividend_yield,
                pe_ratio,
                eps,
                beta,
                market_cap,
                provider_identifier,
            ),
        )
        self.connection.commit()

        # If inserted, lastrowid is new equity_id; if updated, lastrowid can be 0-ish.
        # Return the actual equity_id deterministically:
        eq = self.get_by_exchange_symbol(exchange_id=exchange_id, symbol=symbol)
        if not eq:
            raise sql.Error("Upsert failed to return equity")
        return eq.equity_id

    # ---------- ENSURE (high-level convenience) ----------

    # NEED TO FIX THIS TODO
    def get_or_create_ensure(self, *, symbol: str, exchange_name: str) -> Equity | None:
        exchange_name = exchange_name.strip()
        provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        # check if exchange exists
        ex = self.hub.exchange_repo.get_info(exchange_name=exchange_name)

        if not ex:
            # need issuer data to create exchange
            issuer_enriched = self.hub.basic_info_service.fetch_issuer_enriched(
                symbol=symbol,
                exchange_name=exchange_name
            )

            ex_id = self.hub.exchange_repo.get_or_create(
                exchange_name=exchange_name,
                timezone=issuer_enriched.timezone,
                currency=issuer_enriched.currency,
                rth_open=issuer_enriched.rth_open,
                rth_close=issuer_enriched.rth_close,
                provider_identifier=provider_identifier,
            )

            ex = self.hub.exchange_repo.get_info(exchange_id=ex_id)

            if not ex:
                return None

        # fetch equity enrichment
        equity = self.hub.basic_info_service.fetch_equity_enriched(
            symbol=symbol,
            exchange_name=exchange_name,
            currency=ex.currency
        )

        # resolve issuer
        issuer = self.hub.issuer_repo.get_info(cik=equity.cik, lei=equity.lei)

        if issuer:
            issuer_id = issuer.issuer_id
            self.hub.issuer_repo.upsert(
                issuer_id,
                full_name=equity.full_name,
                cik=equity.cik,
                lei=equity.lei,
                provider_identifier=provider_identifier,
            )
        else:
            issuer_enriched = self.hub.basic_info_service.fetch_issuer_enriched(
                symbol=symbol,
                exchange_name=exchange_name
            )

            issuer_id = self.hub.issuer_repo.get_or_create(
                full_name=issuer_enriched.full_name,
                cik=issuer_enriched.cik,
                lei=issuer_enriched.lei,
                provider_identifier=provider_identifier,
            )

        # upsert equity every ensure call so provider provenance always reflects
        # the active market provider combination.
        equity_id = self.upsert_by_exchange_symbol(
            issuer_id=issuer_id,
            exchange_id=ex.exchange_id,
            symbol=equity.symbol,
            full_name=equity.full_name,
            sector=equity.sector,
            industry=equity.industry,
            dividend_yield=equity.dividend_yield,
            pe_ratio=equity.pe_ratio,
            eps=equity.eps,
            beta=equity.beta,
            market_cap=equity.market_cap,
            provider_identifier=provider_identifier,
        )

        return self.get_by_id(equity_id=equity_id)