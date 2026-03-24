from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3 as sql
from typing import Any, Literal, List

from database_connector.db import Hub


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
    _issuer_id: int
    type: str
    period: str
    fiscal_date: str
    provider_identifier: str
    statement: dict[str, Any]
    _hub: Hub


class StatementRepository:
    """
    Data-access layer for the `statements` table.

    Expected schema:

        id INTEGER PRIMARY KEY,
        issuer_id INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
        type TEXT NOT NULL,
        period TEXT NOT NULL,
        fiscal_date TEXT NOT NULL,     -- ISO YYYY-MM-DD
        statement TEXT NOT NULL,       -- JSON
        UNIQUE(issuer_id, type, period, fiscal_date)
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
        issuer_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        count: int = 1,
        provider_identifier: str | None = None,
    ) -> List[Statement]:
        """
        Returns the most recent `count` statements (newest -> oldest).
        """
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["fundamental"]

        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT id, issuer_id, type, period, fiscal_date, provider_identifier, statement
            FROM statements
            WHERE issuer_id = ? AND type = ? AND period = ? AND provider_identifier = ?
            ORDER BY fiscal_date DESC
            LIMIT ?
            """,
            (issuer_id, statement_type, period, provider_identifier, count),
        )
        rows = cur.fetchall()

        return [
            Statement(
                _id=r[0],
                _issuer_id=r[1],
                type=r[2],
                period=r[3],
                fiscal_date=r[4],
                provider_identifier=r[5],
                statement=json.loads(r[6]) if r[6] else {},
                _hub=self.hub,
            )
            for r in rows
        ]

    # ============================================================
    # WRITE (UPSERT)
    # ============================================================

    def upsert_statement(
        self,
        issuer_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        fiscal_date: str,
        statement: dict[str, Any],
        provider_identifier: str | None = None,
    ) -> None:
        """
        Insert or override statement.
        """
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["fundamental"]

        cur = self.connection.cursor()
        cur.execute(
            """
            INSERT INTO statements (issuer_id, type, period, fiscal_date, provider_identifier, statement)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(issuer_id, type, period, fiscal_date, provider_identifier)
            DO UPDATE SET statement = excluded.statement
            """,
            (
                issuer_id,
                statement_type,
                period,
                fiscal_date,
                provider_identifier,
                json.dumps(statement),
            ),
        )
        self.connection.commit()

    # ============================================================
    # ENSURE LOGIC
    # ============================================================

    def ensure_statements(
        self,
        issuer_id: int,
        statement_type: STATEMENT_TYPES,
        period: PERIODS,
        count: int,
        provider_identifier: str | None = None,
    ) -> List[Statement]:
        """
        Ensures the most recent `count` statements exist in DB.
        Missing ones are fetched and upserted (override allowed).
        """

        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["fundamental"]

        # 1) Get what we already have
        existing = self.get_statements(
            issuer_id,
            statement_type,
            period,
            count,
            provider_identifier=provider_identifier,
        )
        existing_dates = {s.fiscal_date for s in existing}

        # If we already have enough, return them
        if len(existing) >= count:
            return existing

        print("Refetch requested")
        # 2) Fetch from service
        issuer = self.hub.issuer_repo.get_info(issuer_id=issuer_id)
        equities = issuer.get_equities()
        for equity in equities:
            symbol = equity.symbol
            exchange_name = equity.exchange.name

            try:
                fetched = self.hub.fundamental_data_service.fetch_statement(
                    symbol,
                    statement_type,
                    count,        # service expected to return most recent N
                    period,
                ) or []
                break  # stop after first successful fetch
            except Exception as e:
                print(f"Error fetching {statement_type} for {symbol} on {exchange_name}: {e}")
                continue

        # 3) Upsert everything fetched (override safe)
        for st in fetched[:count]:
            fiscal_date = st.get("date")
            if not fiscal_date:
                continue
            self.upsert_statement(
                issuer_id,
                statement_type,
                period,
                fiscal_date,
                st,
                provider_identifier=provider_identifier,
            )

        # 4) Return newest N from DB
        return self.get_statements(
            issuer_id,
            statement_type,
            period,
            count,
            provider_identifier=provider_identifier,
        )

    # ============================================================
    # DELETE
    # ============================================================

    def delete_by_issuer(self, issuer_id: int) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM statements WHERE issuer_id = ?", (issuer_id,))
        self.connection.commit()
        return int(cur.rowcount)

    # ============================================================
    # DEBUG
    # ============================================================

    def debug_periods(self):
        cur = self.connection.cursor()
        cur.execute("SELECT period, COUNT(*) FROM statements GROUP BY period")
        return cur.fetchall()

    def debug_latest_rows(self, issuer_id: int, n: int = 5):
        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT fiscal_date, type, period
            FROM statements
            WHERE issuer_id = ?
            ORDER BY fiscal_date DESC
            LIMIT ?
            """,
            (issuer_id, n),
        )
        return cur.fetchall()
