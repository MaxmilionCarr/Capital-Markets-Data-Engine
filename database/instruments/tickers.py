from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple, Any
from dataclasses import dataclass
from functools import cached_property
import pandas as pd
from typing_extensions import Literal

@dataclass
class Ticker:
    _id: int
    symbol: str
    _market_id: int
    _exchange_id: int
    currency: str
    full_name: str
    description: str
    _source: str
    _connection: sql.Connection
    
    @cached_property
    def market_name(self) -> str:
        """Return the market name for this ticker."""
        from database.core.markets import MarketRepository
        repo = MarketRepository(self._connection)
        market = repo.get_info(exchange_id=self._exchange_id, market_id=self._market_id)
        return market.name if market else "Unknown"
    
    @cached_property
    def exchange_name(self) -> str:
        """Return the exchange name for this ticker."""
        from database.core.exchanges import ExchangeRepository
        repo = ExchangeRepository(self._connection)
        exchange = repo.get_info(exchange_id=self._exchange_id)
        return exchange.name if exchange else "Unknown"

    # Fetching Exchanges
    def get_exchange(self):
        """Return the exchange for this ticker."""
        from database.core.exchanges import ExchangeRepository
        repo = ExchangeRepository(self._connection)
        return repo.get_info(exchange_id=self._exchange_id)
    
    # Fetching Markets
    def get_market(self):
        """Return the market for this ticker."""
        from database.core.markets import MarketRepository
        repo = MarketRepository(self._connection)
        return repo.get_info(exchange_id=self._exchange_id, market_id=self._market_id)

    # Fetching Historical Prices
    def get_prices(self, start_date: str, end_date: str | None = None, period: Literal["5 Minutes", "1 Hour", "1 Day"] = "1 Day") -> pd.DataFrame:
        """Return historical prices for this ticker."""
        from database.technical_data.historical_prices import HistoricalPricesRepository
        repo = HistoricalPricesRepository(self._connection)
        return repo.get_info(self._id, period, start_date, end_date)
    
    # TODO: Implement further methods that help for data fetch without knowing exact dates

    # NEED BOND INFO LATER
    '''
    @cached_property
    def equity_info(self) -> Equity | None:
        """Return equity-specific info if this ticker is an equity."""
        if self.market_id != 1:
            print("Ticker is not an equity based on market_id.")
            return None
        repo = EquitiesRepository(self._connection)
        return repo.get_info(ticker_id=self.id, symbol=self.symbol)
    '''
    

# This is the primary table for all instruments
# TODO: Multiple of the same symbol can exist in the same exchange if they are different markets
class TickerRepository:
    """
    Data-access layer for the `tickers` table.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        market_id INTEGER NOT NULL,
        exchange_id INTEGER NOT NULL,
        currency TEXT NOT NULL,
        full_name TEXT,
        description TEXT,
        source TEXT NOT NULL,
        UNIQUE(symbol, exchange_id),
        FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
    # ---------- READ ----------

    def get_all(self) -> List[Ticker]:
        """Return all tickers as a list of Ticker objects."""
        cur = self.connection.cursor()
        cur.execute("SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers")
        rows = cur.fetchall()
        return [Ticker(*row, _connection=self.connection) for row in rows]

    # TODO: need to make it so that get_info is based around a symbols exchange and market too
    def get_info(self, exchange_id: int, market_id: int, *, symbol: str | None = None, ticker_id: int | None = None) -> Ticker | None:
        """
        Return a single row by primary key or None if not found.
        """
        if symbol is None and ticker_id is None:
            raise ValueError("Must provide symbol or ticker_id")
        cur = self.connection.cursor()
        if ticker_id is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE ticker_id = ? AND exchange_id = ? AND market_id = ?",
                    (ticker_id, exchange_id, market_id),
                )
                row = cur.fetchone()
                if row:
                    return Ticker(*row, _connection=self.connection)
            except sql.Error as e:
                print(f"Error fetching ticker by ticker_id: {e}")
                pass
            
        if symbol is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE symbol = ? AND exchange_id = ? AND market_id = ?",
                    (symbol, exchange_id, market_id),
                )
                row = cur.fetchone()
                if row:
                    return Ticker(*row, _connection=self.connection)
            except sql.Error as e:
                print(f"Error fetching ticker by symbol: {e}")
                pass

        return None
    
    def get_info_by_exchange(self, exchange_id: int, *, symbol: str | None = None, ticker_id: int | None = None) -> List[Ticker] | None:
        """
        Return rows by primary key and exchange or None if not found.
        """
        if symbol is None and ticker_id is None:
            raise ValueError("Must provide symbol or ticker_id")
        cur = self.connection.cursor()
        if ticker_id is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE ticker_id = ? AND exchange_id = ?",
                    (ticker_id, exchange_id),
                )
                rows = cur.fetchall()
                if rows:
                    return [Ticker(*row, _connection=self.connection) for row in rows]
            except sql.Error as e:
                print(f"Error fetching ticker by ticker_id: {e}")
                pass
            
        if symbol is not None:
            try:
                cur.execute(
                    f"SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE symbol = ? AND exchange_id = ?",
                    (symbol, exchange_id),
                )
                rows = cur.fetchall()
                if rows:
                    return [Ticker(*row, _connection=self.connection) for row in rows]
            except sql.Error as e:
                print(f"Error fetching ticker by symbol: {e}")
                pass

        return None

    
    def get_by_market(self, market_id: int) -> List[Ticker]:
        """Return all tickers for a given market_id."""
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE market_id = ?",
            (market_id,),
        )
        rows = cur.fetchall()
        return [Ticker(*row, _connection=self.connection) for row in rows]
    
    def get_by_exchange(self, exchange_id: int) -> List[Ticker]:
        """Return all tickers for a given exchange_id."""
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE exchange_id = ?",
            (exchange_id,),
        )
        rows = cur.fetchall()
        return [Ticker(*row, _connection=self.connection) for row in rows]
    
    def get_by_market_and_exchange(self, market_id: int, exchange_id: int) -> List[Ticker]:
        """Return all tickers for a given market_id and exchange_id."""
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, symbol, market_id, exchange_id, currency, full_name, description, source FROM tickers WHERE market_id = ? AND exchange_id = ?",
            (market_id, exchange_id),
        )
        rows = cur.fetchall()
        return [Ticker(*row, _connection=self.connection) for row in rows]
    
    # ---------- CREATE ----------

    def create(self, symbol: str, market_id: int, exchange_id: int, *, currency: str, full_name: str | None = None, description: str | None = None, source: str) -> int:
        """
        Insert a new ticker and return its ID.
        """
        cur = self.connection.cursor()
        cur.execute(
            f"INSERT INTO tickers (symbol, market_id, exchange_id, currency, full_name, description, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, market_id, exchange_id, currency, full_name, description, source),
        )
        self.connection.commit()

        return cur.lastrowid  


    def get_or_create(self, symbol: str, market_id: int, exchange_id: int, *, currency: str, full_name: str | None = None, description: str | None = None, source: str) -> int:
        """
        Return the ID of a row where unique_col == unique_val,
        or create it using defaults if it doesn't exist.
        """
        cur = self.connection.cursor()
        cur.execute(
            f"SELECT ticker_id FROM tickers WHERE symbol = ? AND market_id = ? AND exchange_id = ?",
            (symbol, market_id, exchange_id),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            f"INSERT INTO tickers (symbol, market_id, exchange_id, currency, full_name, description, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, market_id, exchange_id, currency, full_name or "", description or "", source),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------

    def update(self, ticker_id: str, *, symbol: str | None = None, market_id: int | None = None, exchange_id: int | None = None, currency: str | None = None, full_name: str | None = None, description: str | None = None, source: str | None = None) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """
        if not (symbol or market_id or exchange_id or currency or full_name or description or source):
            raise ValueError("Must provide at least one field to update")
        cur = self.connection.cursor()
        fields = []
        values = []
        if symbol is not None:
            fields.append("symbol = ?")
            values.append(symbol)
        if market_id is not None:
            fields.append("market_id = ?")
            values.append(market_id)
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
        cur.execute(
            f"UPDATE tickers SET {', '.join(fields)} WHERE ticker_id = ?",
            tuple(values),
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker_id: int | None = None, symbol: str | None = None) -> int:
        """
        Delete a row by primary key or symbol.
        Returns number of rows deleted.
        """
        if ticker_id is None and symbol is None:
            raise ValueError("Must provide ticker_id or symbol")
        cur = self.connection.cursor()
        if ticker_id is not None:
            cur.execute("DELETE FROM tickers WHERE ticker_id = ?", (ticker_id,))
        elif symbol is not None:
            cur.execute("DELETE FROM tickers WHERE symbol = ?", (symbol,))
        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL tickers.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM tickers")
        self.connection.commit()
        return cur.rowcount


# These tables provide definitions on the data availability for 
# Different market types
# TODO: NEEDS TO BE POPULATED ON TICKER CREATION BASED ON MARKET TYPE
@dataclass
class Equity:
    ticker_id: int
    symbol: str
    sector: str
    industry: str
    dividend_yield: float
    pe_ratio: float
    eps: float
    beta: float
    market_cap: float

class EquitiesRepository:
    """
    Data-access layer for equities-related tables.

    Schema:
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        sector TEXT,
        industry TEXT,
        dividend_yield REAL,
        pe_ratio REAL,
        eps REAL,
        beta REAL,
        market_cap REAL,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")
    
    # ---------- READ ----------

    def get_all(self) -> List[Equity]:
        """
        Return all rows as a list of Equity objects.
        """
        cur = self.connection.cursor()
        cur.execute(
            "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities"
        )
        rows = cur.fetchall()
        return [Equity(*row) for row in rows]

    def get_info(self, *, ticker_id: int | None = None, symbol: str | None = None) -> Optional[Equity]:
        """
        Return a single row by primary key or None if not found.
        """
        cur = self.connection.cursor()
        if ticker_id is not None:
            try:
                cur.execute(
                    "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities WHERE ticker_id = ?",
                    (ticker_id,),
                )
                row = cur.fetchone()
                return Equity(*row) if row else None
            except sql.Error as e:
                print(f"Error fetching equity by ticker_id: {e}")
                pass
        if symbol is not None:
            try:
                cur.execute(
                    "SELECT ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap FROM equities WHERE symbol = ?",
                    (symbol,),
                )
                row = cur.fetchone()
                return Equity(*row) if row else None
            except sql.Error as e:
                print(f"Error fetching equity by symbol: {e}")
                pass

        return None

    # ---------- CREATE ----------

    def create(self, ticker_id: int, symbol: str, *, sector: str | None = None, industry: str | None = None, dividend_yield: float | None = None, pe_ratio: float | None = None, eps: float | None = None, beta: float | None = None, market_cap: float | None = None) -> int:
        """
        Insert a new row and return its primary key.
        """
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equities (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap),
        )
        self.connection.commit()
        return cur.lastrowid

    def get_or_create(self, ticker_id: int, symbol: str, *, sector: str | None = None, industry: str | None = None, dividend_yield: float | None = None, pe_ratio: float | None = None, eps: float | None = None, beta: float | None = None, market_cap: float | None = None) -> int:
        """
        Return the ID of a row where unique_col == unique_val,
        or create it using defaults if it doesn't exist.
        """
        cur = self.connection.cursor()
        if (ticker_id is None and symbol is None):
            raise ValueError("Must provide ticker_id or symbol")
        
        if ticker_id is not None:
            info = self.get_info(ticker_id=ticker_id)
            if info:
                return info.ticker_id
        if symbol is not None:
            info = self.get_info(symbol=symbol)
            if info:
                return info.ticker_id

        if ticker_id is not None and symbol is not None:
            cur.execute(
                "INSERT INTO equities (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ticker_id, symbol, sector, industry, dividend_yield, pe_ratio, eps, beta, market_cap),
            )
            self.connection.commit()
            return cur.lastrowid
        
        raise ValueError("Must provide ticker_id and symbol to create a new equity")
    
    # ---------- UPDATE ----------

    def update(self, ticker_id: int, *, symbol: str | None = None, sector: str | None = None, industry: str | None = None, dividend_yield: float | None = None, pe_ratio: float | None = None, eps: float | None = None, beta: float | None = None, market_cap: float | None = None) -> int:
        """
        Update given columns for a row.
        Returns number of rows updated.
        """

        cur = self.connection.cursor()
        fields = []
        values = [] 
        if symbol is not None:
            fields.append("symbol = ?")
            values.append(symbol)
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
            
        cur.execute(
            f"UPDATE equities SET {', '.join(fields)} WHERE ticker_id = ?",
            tuple(values) + (ticker_id,),
        )

        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, ticker_id: int = None, symbol: str = None) -> int:
        """
        Delete a row by primary key OR by symbol.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        if ticker_id is not None:
            cur.execute("DELETE FROM equities WHERE ticker_id = ?", (ticker_id,))
        elif symbol is not None:
            cur.execute("DELETE FROM equities WHERE symbol = ?", (symbol,))
        else:
            raise ValueError("Must provide ticker_id or symbol")

        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL rows from this table.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equities")
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