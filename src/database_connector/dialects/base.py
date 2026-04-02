from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence


class DatabaseDialect(ABC):
    name: str

    @abstractmethod
    def configure_connection(self, connection) -> None:
        pass

    @abstractmethod
    def table_exists_sql(self) -> str:
        pass

    @abstractmethod
    def insert_ignore_sql(self, *, table: str, columns: Sequence[str]) -> str:
        pass

    @abstractmethod
    def upsert_sql(
        self,
        *,
        table: str,
        columns: Sequence[str],
        conflict_columns: Sequence[str],
        update_columns: Sequence[str],
        coalesce_update_columns: Sequence[str] = (),
    ) -> str:
        pass
