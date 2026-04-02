from __future__ import annotations

from typing import Sequence

from database_connector.dialects.base import DatabaseDialect


class SQLiteDialect(DatabaseDialect):
    name = "sqlite"

    def configure_connection(self, connection) -> None:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")

    def table_exists_sql(self) -> str:
        return "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?"

    def insert_ignore_sql(self, *, table: str, columns: Sequence[str]) -> str:
        column_list = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        return (
            f"INSERT OR IGNORE INTO {table} "
            f"({column_list}) VALUES ({placeholders})"
        )

    def upsert_sql(
        self,
        *,
        table: str,
        columns: Sequence[str],
        conflict_columns: Sequence[str],
        update_columns: Sequence[str],
        coalesce_update_columns: Sequence[str] = (),
    ) -> str:
        column_list = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        conflict_list = ", ".join(conflict_columns)

        coalesce_set = set(coalesce_update_columns)
        assignments: list[str] = []
        for column in update_columns:
            if column in coalesce_set:
                assignments.append(f"{column} = COALESCE(excluded.{column}, {table}.{column})")
            else:
                assignments.append(f"{column} = excluded.{column}")

        update_clause = ",\n                ".join(assignments)
        return (
            f"INSERT INTO {table} ({column_list})\n"
            f"            VALUES ({placeholders})\n"
            f"            ON CONFLICT({conflict_list}) DO UPDATE SET\n"
            f"                {update_clause}"
        )
