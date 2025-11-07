from __future__ import annotations
import sqlite3 as sql
from typing import Optional, List, Tuple
from dataclasses import dataclass
from functools import cached_property
from typing_extensions import Literal

@dataclass
class Exchange:
    """
    Data class representing an exchange.
    """
    
    # Data Fields
    _id: int
    name: str
    timezone: str
    _connection: sql.Connection

    # Fetch Markets
    def get_all_markets(self):
        """Return all markets for this exchange."""
        from .markets import MarketRepository
        repo = MarketRepository(self._connection)
        return repo.get_by_exchange(self._id)
    
    def get_market(self, market_name: Literal["STK", "BND"]):
        """Return a specific market by name for this exchange."""
        from .markets import MarketRepository
        repo = MarketRepository(self._connection)
        return repo.get_info(exchange_id=self._id, market_name=market_name)

    # Fetch Tickers
    def get_all_tickers(self):
        """Return all tickers for this exchange."""
        from instruments.tickers import TickerRepository
        repo = TickerRepository(self._connection)
        return repo.get_by_exchange(self._id)
    
    # TODO: need to make it so that get_ticker is given by market as well
    def get_ticker(self, ticker_symbol: str = None, *, market_name: Optional[Literal["STK", "BND"]] = None):
        """Return a specific ticker by symbol or ID for this exchange."""
        from instruments.tickers import TickerRepository
        repo = TickerRepository(self._connection)

        if market_name is None: #GRAB TICKERS WITH THE SAME SYMBOL, EXCHANGE, DIFFERENT MARKETS
            return repo.get_info_by_exchange(exchange_id=self._id, symbol=ticker_symbol)
        
        else:
            market = self.get_market(market_name=market_name)
            return market.get_ticker(ticker_symbol=ticker_symbol) if market else None

class ExchangeRepository:
    """
    Data-access layer for the `exchanges` table.

    Schema:
        exchange_id INTEGER PRIMARY KEY,
        exchange_name TEXT NOT NULL,
        timezone TEXT NOT NULL
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def get_all(self) -> List[Exchange]:
        """Return all exchanges as a list of exchange objects"""
        cur = self.connection.cursor()
        cur.execute("SELECT exchange_id, exchange_name, timezone FROM exchanges")
        rows = cur.fetchall()
        return [Exchange(*row, _connection=self.connection) for row in rows]
    
    # Suggestion, is None really the best default return type here??. 
    def get_info(self, *, exchange_id: int | None = None, exchange_name: str | None = None) -> Exchange | None:
        """Return a single exchange object or None if not found."""
        cur = self.connection.cursor()
        try:    
            cur.execute(
                "SELECT exchange_id, exchange_name, timezone FROM exchanges WHERE exchange_id = ? OR exchange_name = ?",
                (exchange_id, exchange_name),
            )
        except sql.Error as e:
            print(f"SQL error: {e}")
            return None
        row = cur.fetchone()
        return Exchange(*row, _connection=self.connection) if row else None
    

    # ---------- CREATE ----------

    def create(self, exchange_name: str, timezone: str) -> int:
        """Insert a new exchange and return its ID."""
        if not exchange_name or not timezone:
            raise ValueError("exchange_name and timezone must be provided")
        
        cur = self.connection.cursor()
        try:
            cur.execute(
                "INSERT INTO exchanges (exchange_name, timezone) VALUES (?, ?)",
                (exchange_name, timezone),
            )
            self.connection.commit()
            return cur.lastrowid
        except sql.Error as e:
            print(f"SQL error: {e}")
            cur.close()
        
    # FIXME: USE CREATE INSTEAD OF REWRITING LOGIC
    def get_or_create(self, exchange_name: str, *, timezone: Optional[str] = None) -> int:
        """
        Return the ID of an existing exchange with this name,
        or create it if it doesn't exist.
        """
        if not exchange_name:
            raise ValueError("exchange_name must be provided")
        cur = self.connection.cursor()
        cur.execute(
            "SELECT exchange_id FROM exchanges WHERE exchange_name = ?",
            (exchange_name,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        if timezone is None:
            raise ValueError("timezone must be provided when creating a new exchange")

        cur.execute(
            "INSERT INTO exchanges (exchange_name, timezone) VALUES (?, ?)",
            (exchange_name, timezone),
        )
        self.connection.commit()
        return cur.lastrowid

    # ---------- UPDATE ----------

    def update(
        self,
        exchange_id: int,
        *,
        exchange_name: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> int:
        """
        Update name and/or timezone for an exchange.
        Returns number of rows updated (0 if nothing matched).
        """
        if exchange_name is None and timezone is None:
            raise ValueError("Must provide at least one field to update")

        fields, values = [], []
        if exchange_name:
            fields.append("exchange_name = ?")
            values.append(exchange_name)
        if timezone:
            fields.append("timezone = ?")
            values.append(timezone)

        values.append(exchange_id)
        sql_query = f"UPDATE exchanges SET {', '.join(fields)} WHERE exchange_id = ?"
        cur = self.connection.cursor()
        cur.execute(sql_query, tuple(values))
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, *, exchange_id: Optional[int] = None, exchange_name: Optional[str] = None) -> int:
        """
        Delete an exchange by id or name.
        Returns number of rows deleted.
        """
        if (exchange_id is None) == (exchange_name is None):
            raise ValueError("Provide exactly one of exchange_id or exchange_name")

        cur = self.connection.cursor()
        if exchange_id is not None:
            cur.execute("DELETE FROM exchanges WHERE exchange_id = ?", (exchange_id,))
        else:
            cur.execute("DELETE FROM exchanges WHERE exchange_name = ?", (exchange_name,))

        self.connection.commit()
        return 1

    def delete_all(self) -> int:
        """
        Delete ALL exchanges.
        Returns number of rows deleted.
        Be sure the caller confirms before calling this.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM exchanges")
        self.connection.commit()
        return 1