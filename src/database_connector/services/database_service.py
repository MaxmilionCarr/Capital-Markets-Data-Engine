from __future__ import annotations

import sqlite3 as sql
from contextlib import contextmanager
from typing import Iterable, Sequence

from database_connector.dialects import DatabaseDialect


class DatabaseService:
    def __init__(self, connection: sql.Connection, dialect: DatabaseDialect):
        self.connection = connection
        self.dialect = dialect
        self.dialect.configure_connection(self.connection)

    def execute(self, query: str, params: Sequence[object] = ()):
        cur = self.connection.cursor()
        cur.execute(query, tuple(params))
        return cur

    def executemany(self, query: str, rows: Iterable[Sequence[object]]):
        cur = self.connection.cursor()
        cur.executemany(query, rows)
        return cur

    def fetchone(self, query: str, params: Sequence[object] = ()):
        return self.execute(query, params).fetchone()

    def fetchall(self, query: str, params: Sequence[object] = ()):
        return self.execute(query, params).fetchall()

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self):
        cur = self.connection.cursor()
        cur.execute("BEGIN")
        try:
            yield
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def table_exists(self, table_name: str) -> bool:
        query = self.dialect.table_exists_sql()
        row = self.fetchone(query, (table_name,))
        return row is not None

    def build_insert_ignore(self, *, table: str, columns: Sequence[str]) -> str:
        return self.dialect.insert_ignore_sql(table=table, columns=columns)

    def build_upsert(
        self,
        *,
        table: str,
        columns: Sequence[str],
        conflict_columns: Sequence[str],
        update_columns: Sequence[str],
        coalesce_update_columns: Sequence[str] = (),
    ) -> str:
        return self.dialect.upsert_sql(
            table=table,
            columns=columns,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            coalesce_update_columns=coalesce_update_columns,
        )
