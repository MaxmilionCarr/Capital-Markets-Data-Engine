from __future__ import annotations

import sqlite3 as sql
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, List

import pandas as pd
from typing_extensions import Literal

from database.db import Hub


@dataclass
class Ticker:
    _id: int
    _underlying_id: int
    _exchange_id: int

    symbol: str
    full_name: str
    currency: str

    _source: str
    _hub: Hub

    @cached_property
    def exchange_name(self) -> str:
        """Return the exchange name for this ticker."""
        ex = self._hub.exchange_repo.get_info(exchange_id=self._exchange_id)
        return ex.name if ex else "Unknown"

    def get_exchange(self):
        """Return the exchange for this ticker."""
        return self._hub.exchange_repo.get_info(exchange_id=self._exchange_id)

    def get_equity(self, *, ensure: bool = False) -> "Equity" | None:
        """Return equity-specific info if this ticker is an equity."""
        equity_repo = self._hub.equities_repo
        equity = equity_repo.get_info(self)
        if equity is None and ensure:
            equity = equity_repo.get_or_create_ensure(self)
        return equity


class TickerRepository:
    """
    Data-access layer for the `tickers` table.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        underlying_id INTEGER NOT NULL,
        exchange_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        full_name TEXT,
        currency TEXT NOT NULL,
        source TEXT NOT NULL,
        UNIQUE(symbol, exchange_id),
        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
        FOREIGN KEY (underlying_id) REFERENCES underlyings(underlying_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- Private ----------

    def _get_or_create_underlying(self, symbol: str) -> int:
        cur = self.connection.cursor()
        cur.execute("SELECT underlying_id FROM underlyings WHERE symbol = ?", (symbol,))
        row = cur.fetchone()
        if row:
            return int(row[0])

        cur.execute("INSERT INTO underlyings (symbol) VALUES (?)", (symbol,))
        self.connection.commit()
        return int(cur.lastrowid)

    # ---------- READ ----------

    def get_all(self) -> List[Ticker]:
        cur = self.connection.cursor()
        cur.execute("SELECT ticker_id, underlying_id, exchange_id, symbol, full_name, currency, source FROM tickers")
        rows = cur.fetchall()
        return [Ticker(*row, _hub=self.hub) for row in rows]

    def get_by_symbol(self, symbol: str) -> List[Ticker]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, underlying_id, exchange_id, symbol, full_name, currency, source "
            "FROM tickers WHERE symbol = ?",
            (symbol,),
        )
        rows = cur.fetchall()
        return [Ticker(*row, _hub=self.hub) for row in rows]

    def get_info(self, exchange_id: int, *, symbol: str | None = None, ticker_id: int | None = None) -> Optional[Ticker]:
        """
        Return a single ticker (by ticker_id or symbol) scoped to an exchange_id, or None.
        """
        if symbol is None and ticker_id is None:
            raise ValueError("Must provide symbol or ticker_id")

        cur = self.connection.cursor()

        if ticker_id is not None:
            cur.execute(
                "SELECT ticker_id, underlying_id, exchange_id, symbol, full_name, currency, source "
                "FROM tickers WHERE ticker_id = ? AND exchange_id = ?",
                (ticker_id, exchange_id),
            )
            row = cur.fetchone()
            return Ticker(*row, _hub=self.hub) if row else None

        cur.execute(
            "SELECT ticker_id, underlying_id, exchange_id, symbol, full_name, currency, source "
            "FROM tickers WHERE symbol = ? AND exchange_id = ?",
            (symbol, exchange_id),
        )
        row = cur.fetchone()
        return Ticker(*row, _hub=self.hub) if row else None

    def get_by_exchange(self, exchange_id: int) -> List[Ticker]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, underlying_id, exchange_id, symbol, full_name, currency, source "
            "FROM tickers WHERE exchange_id = ?",
            (exchange_id,),
        )
        rows = cur.fetchall()
        return [Ticker(*row, _hub=self.hub) for row in rows]

    # ---------- CREATE ----------

    def create(self, symbol: str, exchange_id: int, *, currency: str, full_name: str | None = None, source: str) -> int:
        cur = self.connection.cursor()
        underlying_id = self._get_or_create_underlying(symbol)
        cur.execute(
            "INSERT INTO tickers (underlying_id, exchange_id, symbol, full_name, currency, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (underlying_id, exchange_id, symbol, full_name, currency, source),
        )
        self.connection.commit()
        return int(cur.lastrowid)

    def get_or_create(self, symbol: str, exchange_id: int, *, currency: str, full_name: str | None = None, source: str) -> Ticker:
        """
        Return the ticker where (symbol, exchange_id) match, else create it.
        """
        existing = self.get_info(exchange_id=exchange_id, symbol=symbol)
        if existing:
            return existing

        ticker_id = self.create(symbol=symbol, exchange_id=exchange_id, currency=currency, full_name=full_name, source=source)
        out = self.get_info(exchange_id=exchange_id, ticker_id=ticker_id)
        if out is None:
            raise sql.Error("Failed to create ticker")
        return out

    def get_or_create_ensure(
        self,
        symbol: str,
        *,
        exchange_name: str,
    ) -> Optional[Ticker]:
        """
        Ensure (exchange, ticker, equity) exist.
        exchange_name is authoritative (what the user asked for).
        """

        exchange_name = exchange_name.strip()

        # --- 0) Ensure exchange exists (or create it) ---
        try:
            ex = self.hub.exchange_repo.get_info(exchange_name=exchange_name)
            ensured_exchange_id = ex._id
        except sql.Error:
            # Exchange missing: use provider to get timezone for this exchange_name.
            # We'll fetch ticker details with exchange_name constraint to get timeZoneId.
            tinfo = self.hub._service.fetch_ticker(symbol=symbol, exchange_name=exchange_name)
            if not tinfo:
                print("No ticker with that exchange exists (cannot create exchange)")
                return None

            ensured_exchange_id = self.hub.exchange_repo.get_or_create(
                exchange_name=exchange_name,
                timezone=tinfo.timezone,
                rth_open="09:30:00",
                rth_close="16:00:00",
            )

        # --- 1) If ticker already exists on this exchange_id, return it ---
        existing = self.get_info(exchange_id=ensured_exchange_id, symbol=symbol)
        if existing is not None:
            return existing

        # --- 2) "Repair path": if you somehow have a ticker row but exchange row was missing ---
        # This shouldn't happen with enforced FK, but if it does, you can attempt to locate it by symbol only.
        # If found, you can re-insert exchange row and then re-insert ticker properly.
        # (Optional: skip this if you don't want auto repair.)
        candidates = self.get_by_symbol(symbol)
        for c in candidates:
            # If the ticker already uses an exchange_id that now doesn't exist, you'd fix the exchange,
            # but since we now ensured the exchange row above, just continue.
            if c.exchange_name == exchange_name:
                return c

        # --- 3) Fetch from provider and insert ticker ---
        ticker = self.hub._service.fetch_ticker(symbol=symbol, exchange_name=exchange_name)
        if not ticker:
            print("No ticker with that exchange exists")
            return None

        ticker_id = self.create(
            symbol=ticker.symbol,
            exchange_id=ensured_exchange_id,
            currency=ticker.currency,
            full_name=ticker.full_name,
            source=ticker.provider.name,
        )

        created = self.get_info(exchange_id=ensured_exchange_id, ticker_id=ticker_id)
        if created is None:
            return None

        if getattr(ticker, "sec_type", None) == "STK":
            self.hub.equities_repo.get_or_create_ensure(created)

        return created
        

    # ---------- UPSERT ----------

    def upsert(self, symbol: str, exchange_id: int, *, currency: str, full_name: str | None = None, source: str) -> int:
        existing = self.get_info(exchange_id=exchange_id, symbol=symbol)
        if existing:
            self.update(
                existing._id,
                symbol=symbol,
                exchange_id=exchange_id,
                currency=currency,
                full_name=full_name,
                source=source,
            )
            return existing._id
        return self.create(
            symbol=symbol,
            exchange_id=exchange_id,
            currency=currency,
            full_name=full_name,
            source=source,
        )

    def upsert_ensure(self, symbol: str, exchange_name: str) -> Optional[Ticker]:
        """
        Return the ticker where (symbol, exchange_name) match,
        or create/update it using data fetched from the service.
        """
        exchange = self.hub.exchange_repo.get_info(exchange_name=exchange_name)
        if exchange:
            existing = self.get_info(exchange_id=exchange._id, symbol=symbol)
            if existing:
                return existing

        ticker = self.hub._service.fetch_ticker(symbol=symbol, exchange_name=exchange_name)
        if not ticker:
            print("No ticker with that exchange exists")
            return None

        ensured_exchange_id = self.hub.exchange_repo.get_or_create(
            exchange_name=ticker.exchange,
            timezone=ticker.timezone,
            rth_open="09:30:00",
            rth_close="16:00:00",
        )

        self.upsert(
            symbol=ticker.symbol,
            exchange_id=ensured_exchange_id,
            currency=ticker.currency,
            full_name=ticker.full_name,
            source=ticker.provider.name,
        )

        return self.get_info(exchange_id=ensured_exchange_id, symbol=symbol)

    # ---------- UPDATE ----------

    def update(
        self,
        ticker_id: int,
        *,
        symbol: str | None = None,
        exchange_id: int | None = None,
        currency: str | None = None,
        full_name: str | None = None,
        description: str | None = None,
        source: str | None = None,
    ) -> int:
        if not (symbol or exchange_id or currency or full_name or description or source):
            raise ValueError("Must provide at least one field to update")

        cur = self.connection.cursor()
        fields, values = [], []

        if symbol is not None:
            fields.append("symbol = ?")
            values.append(symbol)
        if exchange_id is not None:
            fields.append("exchange_id = ?")
            values.append(exchange_id)
        if currency is not None:
            fields.append("currency = ?")
            values.append(currency)
        if full_name is not None:
            fields.append("full_name = ?")
            values.append(full_name)
        if description is not None:
            fields.append("description = ?")
            values.append(description)
        if source is not None:
            fields.append("source = ?")
            values.append(source)

        values.append(ticker_id)
        cur.execute(f"UPDATE tickers SET {', '.join(fields)} WHERE ticker_id = ?", tuple(values))
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker_id: int | None = None, symbol: str | None = None) -> int:
        if ticker_id is None and symbol is None:
            raise ValueError("Must provide ticker_id or symbol")

        cur = self.connection.cursor()
        if ticker_id is not None:
            cur.execute("DELETE FROM tickers WHERE ticker_id = ?", (ticker_id,))
        else:
            cur.execute("DELETE FROM tickers WHERE symbol = ?", (symbol,))

        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM tickers")
        self.connection.commit()
        return cur.rowcount


# -------------------- Equity / EquitiesRepository (unchanged) --------------------
# Keep your Equity + EquitiesRepository as-is; no change required for exchange hours.
# (You will update EquityPricesRepository + ExchangeRepository separately.)
@dataclass
class Equity:
    _id: int
    _ticker: Ticker
    sector: Optional[str]
    industry: Optional[str]
    dividend_yield: Optional[float]
    pe_ratio: Optional[float]
    eps: Optional[float]
    beta: Optional[float]
    market_cap: Optional[float]
    _hub: Hub

    @property
    def symbol(self) -> str:
        return self._ticker.symbol

    def get_prices(
        self,
        start_date: datetime,
        end_date: datetime | None = None,
        *,
        period: Literal["5 mins", "1 hour", "1 day"] = "1 day",
        ensure: bool = False,
    ) -> pd.DataFrame:
        price_repo = self._hub.equity_prices_repo
        if not ensure:
            return price_repo.get_prices(equity=self, period=period, start_date=start_date, end_date=end_date)
        return price_repo.get_or_create_ensure(equity=self, period=period, start_date=start_date, end_date=end_date or datetime.now())
    
class EquitiesRepository:
    """
    Data-access layer for equities table.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        sector TEXT,
        industry TEXT,
        dividend_yield REAL,
        pe_ratio REAL,
        eps REAL,
        beta REAL,
        market_cap REAL,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def get_info(self, ticker: Ticker) -> Optional[Equity]:
        cur = self.connection.cursor()
        cur.execute(
            "SELECT equity_id, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap "
            "FROM equities WHERE ticker_id = ?",
            (ticker._id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return Equity(
            _id=row[0],
            _ticker=ticker,
            sector=row[1],
            industry=row[2],
            dividend_yield=row[3],
            pe_ratio=row[4],
            eps=row[5],
            beta=row[6],
            market_cap=row[7],
            _hub=self.hub,
        )


    # ---------- CREATE / UPSERT ----------

    def create(
        self,
        ticker_id: int,
        *,
        sector: str | None = None,
        industry: str | None = None,
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
    ) -> int:
        """
        Insert a new equity row for an existing ticker_id.
        Returns ticker_id (PK).
        """
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equities (ticker_id, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker_id, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap),
        )
        self.connection.commit()
        return ticker_id

    def get_or_create(
        self,
        ticker: Ticker,
        *,
        sector: str | None = None,
        industry: str | None = None,
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
    ) -> int:
        """
        Returns ticker_id if exists, else inserts a new row.
        """
        existing = self.get_info(ticker)
        if existing:
            return ticker._id
        return self.create(
            ticker._id,
            sector=sector,
            industry=industry,
            dividend_yield=dividend_yield,
            pe_ratio=pe_ratio,
            eps=eps,
            beta=beta,
            market_cap=market_cap,
        )
    
    def get_or_create_ensure(self, ticker: Ticker):
        """
        Returns ticker_id if exists, else inserts a new row using data fetched from the service.
        """
        
        existing = self.get_info(ticker)
        if existing:
            return ticker
        
        print("Tried to fetch equity info from service")
        equity = self.hub.service.fetch_equity(ticker.symbol, ticker.exchange_name, ticker.currency)
        self.create(
            ticker._id,
            sector = equity.sector,
            industry = equity.industry,
            dividend_yield = equity.dividend_yield,
            pe_ratio = equity.pe_ratio,
            eps = equity.eps,
            beta = equity.beta,
            market_cap = equity.market_cap
        )
        return self.get_info(ticker)
               
    def upsert(
        self,
        ticker: Ticker,
        *,
        sector: str | None = None,
        industry: str | None = None,
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
    ) -> int:
        """
        Insert if missing; otherwise update the provided (non-None) fields.
        Returns ticker_id.
        """
        # If your SQLite version supports it, this is the cleanest:
        # - insert default row if not exists
        # - then update only the non-None fields
        self.get_or_create(ticker)

        fields = []
        values = []

        if sector is not None:
            fields.append("sector = ?")
            values.append(sector)
        if industry is not None:
            fields.append("industry = ?")
            values.append(industry)
        if dividend_yield is not None:
            fields.append("dividend_yield = ?")
            values.append(dividend_yield)
        if pe_ratio is not None:
            fields.append("pe_ratio = ?")
            values.append(pe_ratio)
        if eps is not None:
            fields.append("eps = ?")
            values.append(eps)
        if beta is not None:
            fields.append("beta = ?")
            values.append(beta)
        if market_cap is not None:
            fields.append("market_cap = ?")
            values.append(market_cap)

        if not fields:
            return ticker._id  # nothing to update

        cur = self.connection.cursor()
        cur.execute(
            f"UPDATE equities SET {', '.join(fields)} WHERE ticker_id = ?",
            tuple(values) + (ticker._id,),
        )
        self.connection.commit()
        return ticker._id

    # ---------- UPDATE ----------

    def update(
        self,
        ticker: Ticker,
        *,
        sector: str | None = None,
        industry: str | None = None,
        dividend_yield: float | None = None,
        pe_ratio: float | None = None,
        eps: float | None = None,
        beta: float | None = None,
        market_cap: float | None = None,
    ) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """
        fields = []
        values = []

        if sector is not None:
            fields.append("sector = ?")
            values.append(sector)
        if industry is not None:
            fields.append("industry = ?")
            values.append(industry)
        if dividend_yield is not None:
            fields.append("dividend_yield = ?")
            values.append(dividend_yield)
        if pe_ratio is not None:
            fields.append("pe_ratio = ?")
            values.append(pe_ratio)
        if eps is not None:
            fields.append("eps = ?")
            values.append(eps)
        if beta is not None:
            fields.append("beta = ?")
            values.append(beta)
        if market_cap is not None:
            fields.append("market_cap = ?")
            values.append(market_cap)

        if not fields:
            return 0

        cur = self.connection.cursor()
        cur.execute(
            f"UPDATE equities SET {', '.join(fields)} WHERE ticker_id = ?",
            tuple(values) + (ticker._id,),
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker: Ticker) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equities WHERE ticker_id = ?", (ticker._id,))
        self.connection.commit()
        return cur.rowcount


# TODO: Implement Bonds Repository later once equities checks out
class BondsRepository:
    """
    Data-access layer for bonds-related tables.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        bond_type TEXT,
        maturity_date TEXT,
        coupon REAL,
        yield REAL,
        credit_rating TEXT,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")