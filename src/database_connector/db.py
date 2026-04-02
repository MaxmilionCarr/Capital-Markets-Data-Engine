from __future__ import annotations

import json
import logging
import os
import sqlite3 as sql
import importlib
from dataclasses import dataclass, field
from typing import List

from data_providers import DataHub, DataHubConfig
from database_connector.dialects import SQLiteDialect
from database_connector.services import DatabaseService, SchemaService

logger = logging.getLogger(__name__)

try:
    dotenv_module = importlib.import_module("dotenv")
    load_dotenv = getattr(dotenv_module, "load_dotenv", None)
    if callable(load_dotenv):
        load_dotenv()
except Exception:
    logger.debug("python-dotenv is unavailable; relying on process environment variables only.")

env_path = os.getenv("DATABASE_PATH")


def _resolve_sqlite_dialect(dialect: str) -> SQLiteDialect:
    normalized = dialect.strip().lower()
    if normalized != "sqlite":
        raise ValueError(f"Unsupported dialect '{dialect}'. Supported: sqlite")
    return SQLiteDialect()


class Hub:
    def __init__(self, database_service: DatabaseService, config: DataHubConfig):
        self.db_service = database_service
        self.conn = database_service.connection
        self.config = config
        self.data_hub = DataHub(config)
        self._register_active_provenance()

        self._market_data_service = None
        self._exchange_service = None
        self._pricing_data_service = None
        self._fundamental_data_service = None

        self._exchange_repo = None
        self._issuer_repo = None
        self._equities_repo = None
        self._equity_prices_repo = None

        self._statements_repo = None

    def _register_active_provenance(self) -> None:
        """
        Best-effort registration of active provider hashes.
        This is safe on existing databases that do not yet have the provenance table.
        """
        if not self.db_service.table_exists("provider_provenance"):
            return

        try:
            manifest = self.data_hub.provider_manifest
            identifiers = self.data_hub.provider_identifiers

            rows = (
                (identifiers["basic_info"], "basic_info", json.dumps(manifest["basic_info"])),
                (identifiers["pricing"], "pricing", json.dumps(manifest["pricing"])),
                (identifiers["fundamental"], "fundamental", json.dumps(manifest["fundamental"])),
                (
                    identifiers["all"],
                    "all",
                    json.dumps(manifest["basic_info"] + manifest["pricing"] + manifest["fundamental"]),
                ),
            )

            upsert_sql = self.db_service.build_upsert(
                table="provider_provenance",
                columns=("provider_identifier", "scope", "providers"),
                conflict_columns=("provider_identifier",),
                update_columns=("scope", "providers"),
            )

            self.db_service.executemany(upsert_sql, rows)
            self.db_service.commit()
        except sql.Error:
            logger.exception("Failed to register active provider provenance.")
    
    @property
    def market_data_service(self):
        if self._market_data_service is None:

            self._market_data_service = self.data_hub.basic_info

        return self._market_data_service

    @property
    def basic_info_service(self):
        return self.market_data_service

    @property
    def exchange_service(self):
        if self._exchange_service is None:
            self._exchange_service = self.data_hub.exchange
        return self._exchange_service

    @property
    def pricing_data_service(self):
        if self._pricing_data_service is None:

            self._pricing_data_service = self.data_hub.pricing

        return self._pricing_data_service
    
    @property
    def fundamental_data_service(self):
        if self._fundamental_data_service is None:

            self._fundamental_data_service = self.data_hub.fundamentals

        return self._fundamental_data_service

    @property
    def exchange_repo(self):
        from .repositories.core.exchange_repository import ExchangeRepository
        if self._exchange_repo is None:
            self._exchange_repo = ExchangeRepository(self.conn, hub=self)
        return self._exchange_repo
    
    @property
    def issuer_repo(self):
        from .repositories.core.issuer_repository import IssuerRepository
        if self._issuer_repo is None:
            self._issuer_repo = IssuerRepository(self.conn, hub=self)
        return self._issuer_repo

    @property
    def equities_repo(self):
        from .repositories.securities.equities_repository import EquitiesRepository
        if self._equities_repo is None:
            self._equities_repo = EquitiesRepository(self.conn, hub=self)
        return self._equities_repo
    
    @property
    def equity_prices_repo(self):
        from .repositories.technical_data.price_repository import EquityPricesRepository
        if self._equity_prices_repo is None:
            self._equity_prices_repo = EquityPricesRepository(self.conn, hub=self)
        return self._equity_prices_repo
    
    @property
    def statements_repo(self):
        from .repositories.fundamental_data.statements_repository import StatementRepository
        if self._statements_repo is None:
            self._statements_repo = StatementRepository(self.conn, hub=self)
        return self._statements_repo

@dataclass
class DB:
    db_path: str | None = env_path
    config: DataHubConfig = field(default_factory=DataHubConfig)
    dialect: str = "sqlite"

    _connection: sql.Connection = field(init=False)
    _database_service: DatabaseService = field(init=False)
    _hub: Hub = field(init=False)

    def __post_init__(self):
        if not self.db_path:
            raise ValueError("db_path must be provided explicitly or via DATABASE_PATH")

        self._connection = sql.connect(self.db_path)
        self._database_service = DatabaseService(self._connection, _resolve_sqlite_dialect(self.dialect))
        self._hub = Hub(self._database_service, self.config)

    def close(self):
        try:
            self._database_service.close()
        except Exception:
            pass
    
    def get_exchange_id(self, exchange_name: str) -> int | None:
        exchange = self._hub.exchange_repo.get_info(exchange_name = exchange_name)
        if exchange is not None:
            return exchange.exchange_id
        return None
    
    def get_exchange(self, exchange_name: str):
        exchange = self._hub.exchange_repo.get_info(exchange_name = exchange_name)
        if exchange is None:
            raise sql.Error(f"Exchange '{exchange_name}' not found")
        return exchange

    # Allow for a search without exchange name through a bulk insert
    from .repositories.securities.equities_repository import Equity
    def get_equity(self, symbol: str, exchange_name: str, *, ensure: bool = False) -> Equity | List[Equity] | None:
        exchange_name = exchange_name.strip()

        if not ensure:
            exchange_id = self.get_exchange_id(exchange_name)
            if not exchange_id:
                raise sql.Error(f"Exchange '{exchange_name}' not found")
            t = self._hub.equities_repo.get_by_exchange_symbol(exchange_id=exchange_id, symbol=symbol)
            if t is None:
                raise sql.Error(f"Equity '{symbol}' not found on exchange '{exchange_name}'")
            return t

        # ensure=True:
        return self._hub.equities_repo.get_or_create_ensure(symbol=symbol, exchange_name=exchange_name)

class DataBase:
    def __init__(self, db_path=env_path, dialect: str = "sqlite"):
        if not db_path:
            raise ValueError("db_path must be provided explicitly or via DATABASE_PATH")

        self.path = db_path
        self.connection = sql.connect(db_path)
        self.database_service = DatabaseService(self.connection, _resolve_sqlite_dialect(dialect))
        self.schema_service = SchemaService(self.database_service)

    def close(self):
        self.database_service.close()

    def get_custom(self, query, params=()):
        return self.database_service.fetchall(query, params)
    
    def create_db(self, *, keep_open: bool = False):
        self.schema_service.create_schema()
        if not keep_open:
            self.close()
    
    def delete_db(self):
        self.close()
        if input("Are you sure you want to delete the database? This action cannot be undone. (y/n): ").lower() == 'y':
            os.remove(self.path)
            logger.info("Database file removed.")
        else:
            logger.info("Database deletion cancelled.")
    
    def close_db(self):
        self.close()