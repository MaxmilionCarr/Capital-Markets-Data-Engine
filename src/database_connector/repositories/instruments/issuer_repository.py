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

STATEMENTS = Literal["income_statement", "balance_sheet", "cash_flow"]

# Standardise security types across providers
SECURITY_TYPES = {
    "EQUITY": ["STK"],
    "BOND": ["BOND"],
    # Add more as needed
}

@dataclass
class Issuer:
    issuer_id: int
    full_name: str | None
    cik: str | None
    lei: str | None
    _hub: Hub

    def get_statements(
        self,
        statement_type: STATEMENTS,
        period: Literal["annual", "quarterly"],
        look_back: int = 0,
        *,
        ensure: bool = False,
    ) -> Optional[List[Statement]]:
        """
        Statements are keyed by issuer_id (entity-level), not by equity/listing.
        """
        repo = self._hub.statements_repo
        if ensure:
            return repo.ensure_statements(
                issuer_id=self.issuer_id,
                statement_type=statement_type,
                period=period,
                count=look_back,
            )
        return repo.get_statements(
            issuer_id=self.issuer_id,
            statement_type=statement_type,
            period=period,
            count=look_back,
        )
    
    def get_equities(self) -> List[Equity]:
        """Return all equities (listings) associated with this issuer."""
        return self._hub.equities_repo.get_by_issuer(self.issuer_id)


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
                start_date=start_date,
                end_date=end_date or datetime.now(),
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


class IssuerRepository:
    """
    Data-access layer for issuers table.

    Schema:
        issuers(
            issuer_id INTEGER PRIMARY KEY,
            full_name TEXT,
            cik TEXT UNIQUE,
            lei TEXT UNIQUE
        )
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def get_info(
        self,
        *,
        issuer_id: int | None = None,
        cik: str | None = None,
        lei: str | None = None,
    ) -> Issuer | None:
        if issuer_id is None and cik is None and lei is None:
            raise ValueError("Provide issuer_id or cik or lei")

        cur = self.connection.cursor()

        if issuer_id is not None:
            cur.execute(
                "SELECT issuer_id, full_name, cik, lei FROM issuers WHERE issuer_id = ?",
                (issuer_id,),
            )
        elif cik is not None:
            cur.execute(
                "SELECT issuer_id, full_name, cik, lei FROM issuers WHERE cik = ?",
                (cik,),
            )
        else:
            cur.execute(
                "SELECT issuer_id, full_name, cik, lei FROM issuers WHERE lei = ?",
                (lei,),
            )

        row = cur.fetchone()
        return None if not row else Issuer(
            issuer_id=row[0],
            full_name=row[1],
            cik=row[2],
            lei=row[3],
            _hub=self.hub,
        )

    def get_all(self) -> List[Issuer]:
        cur = self.connection.cursor()
        cur.execute("SELECT issuer_id, full_name, cik, lei FROM issuers")
        rows = cur.fetchall()
        return [
            Issuer(issuer_id=r[0], full_name=r[1], cik=r[2], lei=r[3], _hub=self.hub)
            for r in rows
        ]

    # ---------- CREATE / UPSERT ----------

    def create(
        self,
        *,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
    ) -> int:
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO issuers (full_name, cik, lei) VALUES (?, ?, ?)",
            (full_name, cik, lei),
        )
        self.connection.commit()
        return int(cur.lastrowid)

    def get_or_create(
        self,
        *,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
    ) -> int:
        # Prefer deterministic identifiers
        if cik:
            existing = self.get_info(cik=cik)
            if existing:
                return existing.issuer_id
        if lei:
            existing = self.get_info(lei=lei)
            if existing:
                return existing.issuer_id

        # Fallback: name match (best-effort). Keep it conservative.
        if full_name:
            cur = self.connection.cursor()
            cur.execute(
                "SELECT issuer_id, full_name, cik, lei FROM issuers WHERE full_name = ?",
                (full_name,),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])

        return self.create(full_name=full_name, cik=cik, lei=lei)

    def upsert(
        self,
        issuer_id: int,
        *,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
    ) -> int:
        if not (full_name or cik or lei):
            return 0

        fields: list[str] = []
        values: list[object] = []

        if full_name is not None:
            fields.append("full_name = ?")
            values.append(full_name)
        if cik is not None:
            fields.append("cik = ?")
            values.append(cik)
        if lei is not None:
            fields.append("lei = ?")
            values.append(lei)

        values.append(issuer_id)

        cur = self.connection.cursor()
        cur.execute(
            f"UPDATE issuers SET {', '.join(fields)} WHERE issuer_id = ?",
            tuple(values),
        )
        self.connection.commit()
        return cur.rowcount


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
    ) -> int:
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equities (issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    ) -> int:
        """
        Upsert for the natural key (exchange_id, symbol).
        Requires SQLite 3.24+ (ON CONFLICT DO UPDATE).
        """
        cur = self.connection.cursor()
        cur.execute(
            """
            INSERT INTO equities (issuer_id, exchange_id, symbol, full_name, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(exchange_id, symbol) DO UPDATE SET
                issuer_id = excluded.issuer_id,
                full_name = COALESCE(excluded.full_name, equities.full_name),
                sector = COALESCE(excluded.sector, equities.sector),
                industry = COALESCE(excluded.industry, equities.industry),
                dividend_yield = COALESCE(excluded.dividend_yield, equities.dividend_yield),
                pe_ratio = COALESCE(excluded.pe_ratio, equities.pe_ratio),
                eps = COALESCE(excluded.eps, equities.eps),
                beta = COALESCE(excluded.beta, equities.beta),
                market_cap = COALESCE(excluded.market_cap, equities.market_cap)
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

    def get_or_create_ensure(self, *, symbol: str, exchange_name: str) -> Equity | None:
        """
        Ensure exchange exists, issuer exists, and equity listing exists for (exchange, symbol).
        Does at most ONE market_data_service.fetch_ticker() call.
        """
        exchange_name = exchange_name.strip()

        # 0) Try find exchange first (no service call)
        ex = self.hub.exchange_repo.get_info(exchange_name=exchange_name)

        # 1) If exchange exists, we can check equity existence immediately (no service call)
        if ex:
            existing = self.get_by_exchange_symbol(exchange_id=ex.exchange_id, symbol=symbol)
            if existing:
                return existing

        # 2) Single provider call (used for: create exchange if missing, and create equity/issuer if missing)
        tinfo = self.hub.market_data_service.fetch_ticker(symbol=symbol, exchange_name=exchange_name)
        if not tinfo:
            return None

        # 3) Ensure exchange exists (create if missing) using provider metadata
        if not ex:
            ex_id = self.hub.exchange_repo.get_or_create(
                exchange_name=exchange_name,
                timezone=getattr(tinfo, "timezone", "UTC"),
                currency=getattr(tinfo, "currency", "USD"),
                rth_open=getattr(tinfo, "rth_open", None) or "09:30:00",
                rth_close=getattr(tinfo, "rth_close", None) or "16:00:00",
            )
            ex = self.hub.exchange_repo.get_info(exchange_id=ex_id)
            if not ex:
                return None

        # 4) Re-check equity now that we definitely have exchange_id (still no extra service call)
        existing = self.get_by_exchange_symbol(exchange_id=ex.exchange_id, symbol=symbol)
        if existing:
            return existing

        # 5) Resolve issuer (entity-level). Prefer cik/lei; fallback to full_name.
        issuer_id = self.hub.issuer_repo.get_or_create(
            full_name=getattr(tinfo, "full_name", None),
            cik=getattr(tinfo, "cik", None),
            lei=getattr(tinfo, "lei", None),
        )

        self.create(
                issuer_id=issuer_id,
                exchange_id=ex.exchange_id,
                symbol=getattr(tinfo, "symbol", symbol),
                full_name=getattr(tinfo, "full_name", None),
                sector=getattr(tinfo, "sector", None),
                industry=getattr(tinfo, "industry", None),
                dividend_yield=getattr(tinfo, "dividend_yield", None),
                pe_ratio=getattr(tinfo, "pe_ratio", None),
                eps=getattr(tinfo, "eps", None),
                beta=getattr(tinfo, "beta", None),
                market_cap=getattr(tinfo, "market_cap", None),
            )
        
        # 6) Insert equity listing
        if getattr(tinfo, "sec_type", None) not in SECURITY_TYPES["EQUITY"]:
            return None      

        return self.get_by_exchange_symbol(exchange_id=ex.exchange_id, symbol=symbol)