from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3 as sql
from typing import Any, Literal, List

from datahub.db import Hub


# Statement "type" (what kind of statement)
STATEMENT_TYPES = Literal["income_statement", "balance_sheet", "cash_flow"]

# DB "period" (how frequently the statement is stored)
PERIODS = Literal["annual", "quarterly"]


@dataclass
class Statement:
    """
    Represents one row in the `statements` table.
    """
    _id: int
    _ticker_id: int
    type: str
    period: str
    fiscal_date: str
    statement: dict[str, Any]
    _hub: Hub


class StatementRepository:
    """
    Data-access layer for the `statements` table.

    Expected schema:

        id INTEGER PRIMARY KEY,
        ticker_id INTEGER NOT NULL REFERENCES tickers(ticker_id) ON DELETE CASCADE,
        type TEXT NOT NULL,
        period TEXT NOT NULL,
        fiscal_date TEXT NOT NULL,     -- ISO YYYY-MM-DD
        statement TEXT NOT NULL,       -- JSON
        UNIQUE(ticker_id, type, period, fiscal_date)
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ============================================================
    # READ
    # ============================================================

    def get_statements(
        self,
        ticker_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        count: int = 1,
    ) -> List[Statement]:
        """
        Returns the most recent `count` statements (newest -> oldest).
        """
        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT id, ticker_id, type, period, fiscal_date, statement
            FROM statements
            WHERE ticker_id = ? AND type = ? AND period = ?
            ORDER BY fiscal_date DESC
            LIMIT ?
            """,
            (ticker_id, statement_type, period, count),
        )
        rows = cur.fetchall()

        return [
            Statement(
                _id=r[0],
                _ticker_id=r[1],
                type=r[2],
                period=r[3],
                fiscal_date=r[4],
                statement=json.loads(r[5]) if r[5] else {},
                _hub=self.hub,
            )
            for r in rows
        ]

    # ============================================================
    # WRITE (UPSERT)
    # ============================================================

    def upsert_statement(
        self,
        ticker_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        fiscal_date: str,
        statement: dict[str, Any],
    ) -> None:
        """
        Insert or override statement.
        """
        cur = self.connection.cursor()
        cur.execute(
            """
            INSERT INTO statements (ticker_id, type, period, fiscal_date, statement)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ticker_id, type, period, fiscal_date)
            DO UPDATE SET statement = excluded.statement
            """,
            (ticker_id, statement_type, period, fiscal_date, json.dumps(statement)),
        )
        self.connection.commit()

    # ============================================================
    # ENSURE LOGIC
    # ============================================================

    def ensure_statements(
        self,
        ticker_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        count: int,
    ) -> List[Statement]:
        """
        Ensures the most recent `count` statements exist in DB.
        Missing ones are fetched and upserted (override allowed).
        """

        # 1) Get what we already have
        existing = self.get_statements(ticker_id, statement_type, period, count)
        existing_dates = {s.fiscal_date for s in existing}

        # If we already have enough, return them
        if len(existing) >= count:
            return existing

        print("Refetch requested")
        # 2) Fetch from service
        symbol = self.hub.ticker_repo.get_symbol_by_id(ticker_id)

        fetched = self.hub.fundamental_data_service.fetch_statement(
            symbol,
            statement_type,
            count,        # service expected to return most recent N
            period,
        ) or []

        # 3) Upsert everything fetched (override safe)
        for st in fetched[:count]:
            fiscal_date = st.get("date")
            if not fiscal_date:
                continue
            self.upsert_statement(
                ticker_id,
                statement_type,
                period,
                fiscal_date,
                st,
            )

        # 4) Return newest N from DB
        return self.get_statements(ticker_id, statement_type, period, count)

    # ============================================================
    # DELETE
    # ============================================================

    def delete_by_ticker(self, ticker_id: int) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM statements WHERE ticker_id = ?", (ticker_id,))
        self.connection.commit()
        return int(cur.rowcount)

    # ============================================================
    # DEBUG
    # ============================================================

    def debug_periods(self):
        cur = self.connection.cursor()
        cur.execute("SELECT period, COUNT(*) FROM statements GROUP BY period")
        return cur.fetchall()

    def debug_latest_rows(self, ticker_id: int, n: int = 5):
        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT fiscal_date, type, period
            FROM statements
            WHERE ticker_id = ?
            ORDER BY fiscal_date DESC
            LIMIT ?
            """,
            (ticker_id, n),
        )
        return cur.fetchall()
