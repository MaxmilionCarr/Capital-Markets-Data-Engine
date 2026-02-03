from __future__ import annotations

import sqlite3 as sql
from dataclasses import dataclass
from typing import Optional, List
from database.db import Hub

EXIT_SUCCESS = 1
EXIT_FAILURE = 0


@dataclass
class Exchange:
    """
    Data class representing an exchange.
    """
    _id: int
    name: str
    timezone: str

    # Normal RTH (exchange-local) time-of-day strings: 'HH:MM:SS'
    rth_open: str
    rth_close: str

    _hub: Hub

    def get_all_tickers(self):
        """Return all tickers for this exchange."""
        return self._hub.ticker_repo.get_by_exchange(self._id)

    def get_ticker(self, ticker_symbol: str = None, *, ensure: bool = False):
        """Return a specific ticker by symbol for this exchange."""
        if ensure:
            return self._hub.ticker_repo.get_or_create_ensure(ticker_symbol, exchange_name=self.name)
        return self._hub.ticker_repo.get_info(exchange_id=self._id, symbol=ticker_symbol)


class ExchangeRepository:
    """
    Data-access layer for the `exchanges` table.

    Expected Schema:
        exchange_id   INTEGER PRIMARY KEY,
        exchange_name TEXT NOT NULL UNIQUE,
        timezone      TEXT NOT NULL,
        rth_open      TEXT NOT NULL,   -- '09:30:00'
        rth_close     TEXT NOT NULL    -- '16:00:00'
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def get_all(self) -> List[Exchange]:
        """Return all exchanges as a list of Exchange objects."""
        cur = self.connection.cursor()
        cur.execute("SELECT exchange_id, exchange_name, timezone, rth_open, rth_close FROM exchanges")
        rows = cur.fetchall()
        return [Exchange(*row, _hub=self.hub) for row in rows]

    def get_info(self, *, exchange_id: int | None = None, exchange_name: str | None = None) -> Exchange:
        """Return a single Exchange object. Raises if not found."""
        if (exchange_id is None) == (exchange_name is None):
            raise ValueError("Provide exactly one of exchange_id or exchange_name")

        cur = self.connection.cursor()
        try:
            if exchange_id is not None:
                cur.execute(
                    "SELECT exchange_id, exchange_name, timezone, rth_open, rth_close "
                    "FROM exchanges WHERE exchange_id = ?",
                    (exchange_id,),
                )
            else:
                cur.execute(
                    "SELECT exchange_id, exchange_name, timezone, rth_open, rth_close "
                    "FROM exchanges WHERE exchange_name = ?",
                    (exchange_name,),
                )
        except sql.Error as e:
            print(f"SQL error: {e}")
            raise

        row = cur.fetchone()
        if row is None:
            raise sql.Error("No exchange found with the given identifier.")

        return Exchange(*row, _hub=self.hub)

    # ---------- CREATE ----------

    def create(
        self,
        exchange_name: str,
        timezone: str,
        *,
        rth_open: str = "09:30:00",
        rth_close: str = "16:00:00",
    ) -> int:
        """Insert a new exchange and return its ID."""
        if not exchange_name or not timezone:
            raise ValueError("exchange_name and timezone must be provided")

        cur = self.connection.cursor()
        try:
            cur.execute(
                "INSERT INTO exchanges (exchange_name, timezone, rth_open, rth_close) VALUES (?, ?, ?, ?)",
                (exchange_name, timezone, rth_open, rth_close),
            )
            self.connection.commit()
            return int(cur.lastrowid)
        except sql.Error as e:
            print(f"SQL error: {e}")
            raise

    def get_or_create(
        self,
        exchange_name: str,
        *,
        timezone: Optional[str] = None,
        rth_open: str = "09:30:00",
        rth_close: str = "16:00:00",
    ) -> int:
        """
        Return the ID of an existing exchange with this name,
        or create it if it doesn't exist.
        """
        if not exchange_name:
            raise ValueError("exchange_name must be provided")

        try:
            return self.get_info(exchange_name=exchange_name)._id
        except sql.Error:
            pass

        if timezone is None:
            raise ValueError("timezone must be provided when creating a new exchange")

        return self.create(exchange_name=exchange_name, timezone=timezone, rth_open=rth_open, rth_close=rth_close)

    # ---------- UPDATE ----------

    def update(
        self,
        exchange_id: int,
        *,
        exchange_name: Optional[str] = None,
        timezone: Optional[str] = None,
        rth_open: Optional[str] = None,
        rth_close: Optional[str] = None,
    ) -> int:
        """
        Update fields for an exchange.
        Returns number of rows updated (0 if nothing matched).
        """
        if exchange_name is None and timezone is None and rth_open is None and rth_close is None:
            raise ValueError("Must provide at least one field to update")

        fields, values = [], []
        if exchange_name is not None:
            fields.append("exchange_name = ?")
            values.append(exchange_name)
        if timezone is not None:
            fields.append("timezone = ?")
            values.append(timezone)
        if rth_open is not None:
            fields.append("rth_open = ?")
            values.append(rth_open)
        if rth_close is not None:
            fields.append("rth_close = ?")
            values.append(rth_close)

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
        return cur.rowcount

    def delete_all(self) -> int:
        """
        Delete ALL exchanges.
        Returns number of rows deleted.
        """
        cur = self.connection.cursor()
        cur.execute("DELETE FROM exchanges")
        self.connection.commit()
        return cur.rowcount
