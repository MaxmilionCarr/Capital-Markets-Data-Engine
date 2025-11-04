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
    id: int
    name: str
    timezone: str
    connection: sql.Connection

    # Ladder Down Properties
    @cached_property
    def all_markets(self):
        """Return all markets for this exchange."""
        from .markets import MarketRepository
        repo = MarketRepository(self.connection)
        return repo.get_by_exchange(self.id)
    
    @cached_property
    def get_market(self, market_id: Literal[1, 2]):
        """Return a specific market by name for this exchange."""
        from .markets import MarketRepository
        repo = MarketRepository(self.connection)
        return repo.get_info(market_id=market_id, exchange_id=self.id)

    @cached_property
    def tickers(self):
        """Return all tickers for this exchange."""
        from instruments.tickers import TickerRepository
        repo = TickerRepository(self.connection)
        return repo.get_by_exchange(self.id)
    
    @cached_property
    def get_ticker(self, *, ticker_symbol: str = None, ticker_id: Optional[int] = None):
        """Return a specific ticker by symbol or ID for this exchange."""
        from instruments.tickers import TickerRepository
        repo = TickerRepository(self.connection)
        return repo.get_info(ticker_symbol=ticker_symbol, ticker_id=ticker_id, exchange_id=self.id)

class ExchangeRepository:
    """
    Data-access layer for the `exchanges` table.

    Schema:
        exchange_id INTEGER PRIMARY KEY,
        exchange_name TEXT NOT NULL,
        timezone TEXT NOT NULL

    Function Returns:
        gets:
            get_all() -> List[Tuple[int, str, str]]
            get_at(exchange_id: int) -> Optional[Tuple[int, str, str]]
        creates:
            create(exchange_name: str, timezone: str) -> int
            get_or_create(exchange_name: str, *, timezone: Optional[str] = None) -> int
        updates:
            update(exchange_id: int, *, exchange_name: Optional[str] = None, timezone: Optional[str] = None) -> int
        deletes:
            delete(*, exchange_id: Optional[int] = None, exchange_name: Optional[str] = None) -> int
            delete_all() -> int
    """

    def __init__(self, connection: sql.Connection):
        self.connection = connection
        # Ensure foreign key constraints are enforced
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def get_all(self) -> List[Exchange]:
        """Return all exchanges as a list of (id, name, timezone)."""
        cur = self.connection.cursor()
        cur.execute("SELECT exchange_id, exchange_name, timezone FROM exchanges")
        rows = cur.fetchall()
        return [Exchange(*row, connection=self.connection) for row in rows]

    def get_info(self, *, exchange_id: int | None = None, exchange_name: str | None = None) -> Exchange | None:
        """Return a single exchange row or None if not found."""
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
        return Exchange(*row, connection=self.connection) if row else None
    

    # ---------- CREATE ----------

    def create(self, exchange_name: str, timezone: str) -> int:
        """Insert a new exchange and return its ID."""
        if not exchange_name or not timezone:
            raise ValueError("exchange_name and timezone must be provided")
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO exchanges (exchange_name, timezone) VALUES (?, ?)",
            (exchange_name, timezone),
        )
        self.connection.commit()
        return cur.lastrowid

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