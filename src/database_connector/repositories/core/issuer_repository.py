from __future__ import annotations

import sqlite3 as sql
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, List, TYPE_CHECKING

import pandas as pd
from typing_extensions import Literal

from database_connector.db import Hub
from database_connector.repositories.fundamental_data.statements_repository import Statement

if TYPE_CHECKING:
    from database_connector.repositories.securities.equities_repository import Equity

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
    main_symbol: str
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
    
    
    def get_equities(self) -> List["Equity"]:
        """Return all equities (listings) associated with this issuer."""
        return self._hub.equities_repo.get_by_issuer(self.issuer_id)


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

    # ---------- READ ----------

    def get_info(
        self,
        *,
        issuer_id: int | None = None,
        main_symbol: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
    ) -> Issuer:
        if issuer_id is None and main_symbol is None and cik is None and lei is None:
            raise ValueError("Provide issuer_id or main_symbol or cik or lei")

        cur = self.connection.cursor()

        if issuer_id is not None:
            cur.execute(
                "SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers WHERE issuer_id = ?",
                (issuer_id,),
            )
        elif main_symbol is not None:
            cur.execute(
                "SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers WHERE main_symbol = ?",
                (main_symbol,),
            )
        elif cik is not None:
            cur.execute(
                "SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers WHERE cik = ?",
                (cik,),
            )
        else:
            cur.execute(
                "SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers WHERE lei = ?",
                (lei,),
            )

        row = cur.fetchone()
        return None if not row else Issuer(
            issuer_id=row[0],
            main_symbol=row[1],
            full_name=row[2],
            cik=row[3],
            lei=row[4],
            _hub=self.hub,
        )

    def get_all(self) -> List[Issuer]:
        cur = self.connection.cursor()
        cur.execute("SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers")
        rows = cur.fetchall()
        return [
            Issuer(issuer_id=r[0], main_symbol=r[1], full_name=r[2], cik=r[3], lei=r[4], _hub=self.hub)
            for r in rows
        ]

    def get_by_equity_symbol(self, symbol: str) -> Issuer | None:
        """Find issuer by an existing equity symbol across any exchange."""
        cur = self.connection.cursor()
        cur.execute(
            "SELECT i.issuer_id, i.main_symbol, i.full_name, i.cik, i.lei "
            "FROM issuers i "
            "JOIN equities e ON e.issuer_id = i.issuer_id "
            "WHERE e.symbol = ? "
            "ORDER BY i.issuer_id ASC LIMIT 1",
            (symbol,),
        )
        row = cur.fetchone()
        return None if not row else Issuer(
            issuer_id=row[0],
            main_symbol=row[1],
            full_name=row[2],
            cik=row[3],
            lei=row[4],
            _hub=self.hub,
        )

    # ---------- CREATE / UPSERT ----------

    def create(
        self,
        *,
        main_symbol: str,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO issuers (main_symbol, full_name, cik, lei, provider_identifier) VALUES (?, ?, ?, ?, ?)",
            (main_symbol, full_name, cik, lei, provider_identifier),
        )
        self.connection.commit()
        return int(cur.lastrowid)

    def get_or_create(
        self,
        *,
        main_symbol: str | None = None,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        # Prefer deterministic identifiers
        if cik:
            existing = self.get_info(cik=cik)
            if existing:
                self.upsert(
                    existing.issuer_id,
                    full_name=full_name,
                    cik=cik,
                    lei=lei,
                    provider_identifier=provider_identifier,
                )
                return existing.issuer_id
        if lei:
            existing = self.get_info(lei=lei)
            if existing:
                self.upsert(
                    existing.issuer_id,
                    main_symbol=main_symbol,
                    full_name=full_name,
                    cik=cik,
                    lei=lei,
                    provider_identifier=provider_identifier,
                )
                return existing.issuer_id

        if main_symbol:
            existing = self.get_info(main_symbol=main_symbol)
            if existing:
                self.upsert(
                    existing.issuer_id,
                    main_symbol=main_symbol,
                    full_name=full_name,
                    cik=cik,
                    lei=lei,
                    provider_identifier=provider_identifier,
                )
                return existing.issuer_id

        # Fallback: name match (best-effort). Keep it conservative.
        if full_name:
            cur = self.connection.cursor()
            cur.execute(
                "SELECT issuer_id, main_symbol, full_name, cik, lei FROM issuers WHERE full_name = ?",
                (full_name,),
            )
            row = cur.fetchone()
            if row:
                self.upsert(
                    int(row[0]),
                    main_symbol=main_symbol,
                    full_name=full_name,
                    cik=cik,
                    lei=lei,
                    provider_identifier=provider_identifier,
                )
                return int(row[0])

        if not main_symbol:
            raise ValueError("main_symbol is required to create a new issuer")

        return self.create(
            main_symbol=main_symbol,
            full_name=full_name,
            cik=cik,
            lei=lei,
            provider_identifier=provider_identifier,
        )

    def upsert(
        self,
        issuer_id: int,
        *,
        main_symbol: str | None = None,
        full_name: str | None = None,
        cik: str | None = None,
        lei: str | None = None,
        provider_identifier: str | None = None,
    ) -> int:
        if provider_identifier is None:
            provider_identifier = self.hub.data_hub.provider_identifiers["basic_info"]

        if not (main_symbol or full_name or cik or lei or provider_identifier):
            return 0

        fields: list[str] = []
        values: list[object] = []

        if main_symbol is not None:
            fields.append("main_symbol = ?")
            values.append(main_symbol)
        if full_name is not None:
            fields.append("full_name = ?")
            values.append(full_name)
        if cik is not None:
            fields.append("cik = ?")
            values.append(cik)
        if lei is not None:
            fields.append("lei = ?")
            values.append(lei)
        if provider_identifier is not None:
            fields.append("provider_identifier = ?")
            values.append(provider_identifier)

        values.append(issuer_id)

        cur = self.connection.cursor()
        cur.execute(
            f"UPDATE issuers SET {', '.join(fields)} WHERE issuer_id = ?",
            tuple(values),
        )
        self.connection.commit()
        return cur.rowcount


