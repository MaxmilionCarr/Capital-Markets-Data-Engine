from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple
from dataclasses import dataclass
from functools import cached_property
from enum import IntEnum
from typing_extensions import Literal

# TODO: NEED MORE IN FUTURE Options, Forex
MARKET_TYPES = {
    "COMMON": 1,
    "ETF": 2,
    "BND": 2,
    "TEST_MARKET": 999,
    "UPDATED_TEST_MARKET": 999
}
    
@dataclass
class Market:      
    """
    Data class representing a market.
    """

    # Data Fields
    _id: int
    _exchange_id: int
    name: str
    _connection: sql.Connection

    # Convenience Properties
    @cached_property
    def exchange_name(self) -> str:
        from .exchanges import ExchangeRepository
        repo = ExchangeRepository(self._connection)
        exchange = repo.get_info(exchange_id=self._exchange_id)
        return exchange.name if exchange else "Unknown"

    # Fetching Exchanges
    def exchange(self):
        """Return the exchange object for this market."""
        from .exchanges import ExchangeRepository
        repo = ExchangeRepository(self._connection)
        return repo.get_info(exchange_id=self._exchange_id)

    # Fetching Tickers
    def get_all_tickers(self):
        """Return all tickers for this market."""
        from database.instruments.tickers import TickerRepository
        repo = TickerRepository(self._connection)
        return repo.get_by_market_and_exchange(self._id, self._exchange_id)
    
    def get_ticker(self, ticker_symbol: str = None):
        """Return a specific ticker by symbol or name for this market."""
        from database.instruments.tickers import TickerRepository
        repo = TickerRepository(self._connection)
        return repo.get_info(exchange_id=self._exchange_id, market_id=self._id, symbol=ticker_symbol)

class MarketRepository:
    """
    Data-access layer for the `markets` table.

    Schema:
        market_id INTEGER PRIMARY KEY,
        exchange_id INTEGER NOT NULL,
        market_name TEXT NOT NULL
    
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON;")
    
    # ---------- READ ----------

    def get_all(self) -> List[Market]:
        """Return all markets as a list of Market objects."""
        cur = self.connection.cursor()
        cur.execute("SELECT market_id, exchange_id, market_name FROM markets")
        rows = cur.fetchall()
        return [Market(*row, _connection=self.connection) for row in rows]
    
    # TODO: Maybe make this able to fetch based on name too???
    def get_info(self, exchange_id: int, *, market_id: int | None = None, market_name: Optional[Literal["STK", "BND"]] | None = None) -> Market | None:
        """
        Get a single market by market_id and exchange_id.
        Returns None if not found.
        """

        if (exchange_id is None) or ((market_id is None) and (market_name is None)):
            raise ValueError("Provide at least exchange_id and one of market_id or market_name")
        if market_name is not None:
            try:
                market_type = MARKET_TYPES[market_name]
            except KeyError:
                raise ValueError("Invalid market_name")
        else:
            market_type = market_id

        cur = self.connection.cursor()
        try:
            cur.execute(
                "SELECT market_id, exchange_id, market_name FROM markets WHERE market_id = ? AND exchange_id = ?",
                (market_type, exchange_id),
            )
        except sql.Error as e:
            print(f"SQL error: {e}")
            return None
        row = cur.fetchone()
        return Market(*row, _connection=self.connection) if row else None

    def get_by_exchange(self, exchange_id: int) -> List[Market]:
        """
        Get all markets for a specific exchange by ID.
        """
        if exchange_id is None:
            raise ValueError("Provide exchange_id")

        cur = self.connection.cursor()
        cur.execute("SELECT market_id, exchange_id, market_name FROM markets WHERE exchange_id = ?", (exchange_id,))
        return [Market(*row, _connection=self.connection) for row in cur.fetchall()]

    # ---------- CREATE ----------

    def create(self, exchange_id: int, market_name: str) -> int:
        """Insert a new market and return its ID."""
        if exchange_id is None or market_name is None:
            raise ValueError("exchange_id and market_name must be provided")

        if market_name is not None:
            try:
                market_type = MARKET_TYPES[market_name]
            except KeyError:
                raise ValueError("Invalid market_name")

        cur = self.connection.cursor()
        try:
            cur.execute(
                "INSERT INTO markets (market_id, exchange_id, market_name) VALUES (?, ?, ?)",
                (market_type, exchange_id, market_name),
            )
            self.connection.commit()
            return cur.lastrowid
        except sql.Error as e:
            print(f"SQL error: {e}")
            cur.close()
            return None

    # TODO: Modify to return object instead of just ID?
    def get_or_create(self, exchange_id: int, market_name: str | None = None) -> int:
        """
        Return the ID of an existing market with this name,
        or create it if it doesn't exist.
        """
        if not market_name or not exchange_id:
            raise ValueError("market_name and exchange_id must be provided")

        cur = self.connection.cursor()
        
        if market_name is not None:
            try:
                market_type = MARKET_TYPES[market_name]
            except KeyError:
                raise ValueError("Invalid market_name")
            
            try:
                cur.execute(
                    "SELECT market_id, exchange_id FROM markets WHERE market_id = ? AND exchange_id = ?",
                    (market_type, exchange_id),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
            except sql.Error as e:
                print(f"SQL error: {e}")


        cur.execute(
            "INSERT INTO markets (market_id, exchange_id, market_name) VALUES (?, ?, ?)",
            (market_type, exchange_id, market_name),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------
    def update(self, exchange_id: int, *, market_id: int | None = None, market_name: str | None = None) -> int:
        """
        Update a market's name by id.
        Returns number of rows updated.
        """
        if (market_id is None) and (market_name is None) or (exchange_id is None):
            raise ValueError("Provide both market_name and exchange_id")

        cur = self.connection.cursor()

        if market_name is not None:
            try:
                market_id = MARKET_TYPES[market_name]
            except KeyError:
                raise ValueError("Invalid market_name")
            
        cur.execute(
            "UPDATE markets SET market_name = ? WHERE market_id = ? AND exchange_id = ?",
            (market_name, market_id, exchange_id),
        )

        self.connection.commit()
        return 1

    def delete(self, exchange_id: int, *, market_id: int | None = None, market_name: str | None = None) -> int:
        """
        Delete a market by id or name.
        Returns number of rows deleted.
        """
        if (market_id is None) and (market_name is None) or (exchange_id is None):
            raise ValueError("Provide both market_name and exchange_id")

        cur = self.connection.cursor()

        if market_name is not None:
            try:
                market_id = MARKET_TYPES[market_name]
            except KeyError:
                raise ValueError("Invalid market_name")
            
        cur.execute(
            "DELETE FROM markets WHERE market_id = ? AND exchange_id = ?",
            (market_id, exchange_id),
        )

        self.connection.commit()
        return 1

    def delete_all(self) -> int:
        """
        Delete ALL markets.
        Returns number of rows deleted.
        Be sure the caller confirms before calling this.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM markets")
        self.connection.commit()
        return 1