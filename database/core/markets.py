from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple
from dataclasses import dataclass
from functools import cached_property
from enum import IntEnum


# Cannot auto fetch this, the user will need to specify type
class MarketType(IntEnum):
    EQUITIES = 1
    BONDS = 2
    # NEED MORE IN FUTURE Options, Forex

# TODO: Wonder if I can reduce overhead through the creation of 1 database connection and caching for multiple query runs: FIXED???
@dataclass
class Market:

    # Data Fields
    id: int
    exchange_id: int
    name: str
    connection: sql.Connection

    # Ladder Up Properties
    @cached_property
    def exchange(self):
        """Return the exchange for this market."""
        from .exchanges import ExchangeRepository
        repo = ExchangeRepository(self.connection)
        return repo.get_info(self.exchange_id)

    # Ladder Down Properties
    @cached_property
    def tickers(self):
        """Return all tickers for this market."""
        from instruments.tickers import TickerRepository
        repo = TickerRepository(self.connection)
        return repo.get_by_market(self.id)

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
        return [Market(*row, connection=self.connection) for row in rows]
    
    # Maybe make this able to fetch based on name too???
    def get_info(self, market_id: int, exchange_id: int) -> Market | None:
        """
        Get a single market by market_id and exchange_id.
        Returns None if not found.
        """
        if (market_id is None) or (exchange_id is None):
            raise ValueError("Provide both market_id and exchange_id")
        try:
            market_type = MarketType(market_id)
        except ValueError:
            raise ValueError("Invalid market_id")
        cur = self.connection.cursor()
        try:
            cur.execute(
                "SELECT market_id, exchange_id, market_name FROM markets WHERE market_id = ? AND exchange_id = ?",
                (market_type.value, exchange_id),
            )
        except sql.Error as e:
            print(f"SQL error: {e}")
            return None
        row = cur.fetchone()
        return Market(*row, connection=self.connection) if row else None

    def get_by_exchange(self, exchange_id: int) -> List[Market]:
        """
        Get all markets for a specific exchange by ID.
        """
        if exchange_id is None:
            raise ValueError("Provide exchange_id")

        cur = self.connection.cursor()
        cur.execute("SELECT market_id, exchange_id, market_name FROM markets WHERE exchange_id = ?", (exchange_id,))
        return [Market(*row, connection=self.connection) for row in cur.fetchall()]

    # ---------- CREATE ----------

    def create(self, market_id: int, exchange_id: int) -> int:
        """Insert a new market and return its ID."""
        if market_id is None or exchange_id is None:
            raise ValueError("market_id and exchange_id must be provided")
        try:
            market_type = MarketType(market_id)
        except ValueError:
            raise ValueError("Invalid market_id")
        
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO markets (market_id, exchange_id, market_name) VALUES (?, ?, ?)",
            (market_type.value, exchange_id, market_type.name),
        )
        self.connection.commit()
        return cur.lastrowid

    def get_or_create(self, market_id: int, exchange_id: int) -> int:
        """
        Return the ID of an existing market with this name,
        or create it if it doesn't exist.
        """
        if not market_id or not exchange_id:
            raise ValueError("market_id and exchange_id must be provided")
        try:
            market_type = MarketType(market_id)
        except ValueError:
            raise ValueError("Invalid market_id")
        
        cur = self.connection.cursor()
        cur.execute(
            "SELECT market_id, exchange_id FROM markets WHERE market_id = ? AND exchange_id = ?",
            (market_id, exchange_id),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO markets (market_id, exchange_id, market_name) VALUES (?, ?, ?)",
            (market_type.value, exchange_id, market_type.name),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------

    def delete(self, market_id: int, exchange_id: int) -> int:
        """
        Delete a market by id or name.
        Returns number of rows deleted.
        """
        if (market_id is None) or (exchange_id is None):
            raise ValueError("Provide both market_id and exchange_id")

        cur = self.connection.cursor()
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