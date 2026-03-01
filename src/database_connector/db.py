# TODO: need a better name for this top level access. This is where users will get all their data from
# FIXME: need to handle connection closing better in handling duplicate creations
from __future__ import annotations
import sqlite3 as sql
from typing import Any, List, Literal
import os
from dataclasses import dataclass, field
from functools import cached_property

from data_providers import FMPConfig, IBKRConfig, IBKRService, FMPService, DataHubConfig, DataHub
from data_providers.datahub import PriorityMarket
#from .data.services.IBKR_service import IBKRService

try:
    from dotenv import load_dotenv
except ImportError:
    raise ImportError("Please install python-dotenv to manage environment variables.")
try:
    load_dotenv()  # Load environment variables from a .env file
    env_path = os.getenv("DATABASE_PATH")
except Exception as e:
    print("Not using environment variables, please configure your .env file.")
        
class Hub:
    def __init__(self, connection: sql.Connection, config: DataHubConfig):
        self.conn = connection
        self.config = config
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.data_hub = DataHub(config)
        
        self._market_data_service = None
        self._fundamental_data_service = None

        self._exchange_repo = None
        self._ticker_repo = None
        self._equities_repo = None
        self._equity_prices_repo = None
        
        self._statements_repo = None
    
    @property
    def market_data_service(self):
        if self._market_data_service is None:

            self._market_data_service = self.data_hub.market

        return self._market_data_service
    
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
    def ticker_repo(self):
        from .repositories.instruments.ticker_repository import TickerRepository
        if self._ticker_repo is None:
            self._ticker_repo = TickerRepository(self.conn, hub=self)
        return self._ticker_repo

    @property
    def equities_repo(self):
        from .repositories.instruments.ticker_repository import EquitiesRepository
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
    db_path: str = env_path
    config: DataHubConfig = field(default_factory=DataHubConfig)

    _connection: sql.Connection = field(init=False)
    _hub: Hub = field(init=False)

    def __post_init__(self):
        self._connection = sql.connect(self.db_path)
        self._connection.execute("PRAGMA foreign_keys = ON")
        # strongly recommended for your workload:
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._hub = Hub(self._connection, self.config)

    def close(self):
        try:
            self._connection.close()
        except Exception:
            pass
    
    def get_exchange_id(self, exchange_name: str) -> int | None:
        exchange = self._hub.exchange_repo.get_info(exchange_name = exchange_name)
        if exchange is not None:
            return exchange._id
        return None
    
    def get_exchange(self, exchange_name: str):
        exchange = self._hub.exchange_repo.get_info(exchange_name = exchange_name)
        if exchange is None:
            raise sql.Error(f"Exchange '{exchange_name}' not found")
        return exchange

    # Allow for a search without exchange name through a bulk insert
    from .repositories.instruments.ticker_repository import Ticker  
    def get_ticker(self, symbol: str, exchange_name: str, *, ensure: bool = False) -> Ticker | List[Ticker]:
        exchange_name = exchange_name.strip()

        if not ensure:
            exchange_id = self.get_exchange_id(exchange_name)
            if not exchange_id:
                raise sql.Error(f"Exchange '{exchange_name}' not found")
            t = self._hub.ticker_repo.get_info(exchange_id=exchange_id, symbol=symbol)
            if t is None:
                raise sql.Error(f"Ticker '{symbol}' not found on exchange '{exchange_name}'")
            return t

        # ensure=True:
        return self._hub.ticker_repo.get_or_create_ensure(symbol=symbol, exchange_name=exchange_name)

# FIXME
class DataBase:
    # TODO: add flow down identifiers for exchange, market when creating tickers so don't have to ladder up
    def __init__(self, db_path=env_path):
        self.path = db_path
        self.connection = sql.connect(db_path)
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self):
        self.connection.close()

    def get_custom(self, query, params=()):
        cur = self.connection.cursor()
        cur.execute(query, params)
        return cur.fetchall()
    
    def create_db(self):
        
        con = self.connection
        cur = con.cursor()

        # --- Core Reference Tables ---
        cur.execute('''CREATE TABLE IF NOT EXISTS exchanges (
                        exchange_id INTEGER PRIMARY KEY,
                        exchange_name TEXT NOT NULL UNIQUE,
                        timezone TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        rth_open TEXT NOT NULL,
                        rth_close TEXT NOT NULL
                    )''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_exchanges_name ON exchanges (exchange_name)''')
        
        # --- Ticker Tables ---
        
        cur.execute('''CREATE TABLE IF NOT EXISTS underlyings (
                        underlying_id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL UNIQUE
                    );''')
        
        # Change this so that tickers doesn't need exchange, equities instead should
        cur.execute('''CREATE TABLE IF NOT EXISTS tickers (
                        ticker_id INTEGER PRIMARY KEY,
                        underlying_id INTEGER NOT NULL,
                        exchange_id INTEGER NOT NULL,

                        symbol TEXT NOT NULL,
                        full_name TEXT,
                        
                        source TEXT NOT NULL,

                        UNIQUE(symbol, exchange_id),
                        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
                        FOREIGN KEY (underlying_id) REFERENCES underlyings(underlying_id) ON DELETE CASCADE
                    );''')
        
        # --- Security Type ---
        
        # -- Equity Table --
        cur.execute('''CREATE TABLE IF NOT EXISTS equities (
                        equity_id INTEGER PRIMARY KEY,
                        ticker_id INTEGER NOT NULL,

                        sector TEXT,
                        industry TEXT,
                        dividend_yield REAL,
                        pe_ratio REAL,
                        eps REAL,
                        beta REAL,
                        market_cap REAL,
                        
                        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
                    );''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS equity_intraday_coverage (
                        equity_id INTEGER NOT NULL,
                        date      DATE NOT NULL,
                        period    TEXT NOT NULL,          -- '1 hour', '5 mins'
                        status    TEXT NOT NULL,          -- ok | closed | partial | missing | error
                        provider  TEXT NOT NULL,
                        rows      INTEGER NOT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (equity_id, date, period, provider)
                    );''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_daily (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_hourly (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_five_minute (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')
        
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_daily ON equity_prices_daily (equity_id, datetime)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_hourly ON equity_prices_hourly (equity_id, datetime)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_five_minute ON equity_prices_five_minute (equity_id, datetime)''')
        
        # TODO: 
        
        # -- Statement Tables --

        cur.execute('''CREATE TABLE IF NOT EXISTS statements (
                    id INTEGER PRIMARY KEY,
                    ticker_id INTEGER NOT NULL REFERENCES tickers(ticker_id) ON DELETE CASCADE,
                    type TEXT NOT NULL,  -- 'income_statement', 'balance_sheet', 'cash_flow'
                    period TEXT NOT NULL,  -- 'annual' or 'quarterly'
                    fiscal_date DATETIME NOT NULL,
                    statement JSON NOT NULL,
                    UNIQUE(ticker_id, type, period, fiscal_date)
                    )''')

        


        # --- Prices Table ---   
        
        
        # --- Check --- 
        res = cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print("Tables in database:")
        for row in res:
            print(row[0])
        con.commit()
        con.close()
        print("Database created successfully.")
    
    def delete_db(self):
        self.connection.close()
        if input("Are you sure you want to delete the database? This action cannot be undone. (y/n): ").lower() == 'y':
            os.remove(self.path)
            print("Database file removed.")
        else:
            print("Database deletion cancelled.")
    
    def close_db(self):
        self.connection.close()

    # Figure out a persistent schema migration
    '''
    def update_schema(self, table, updated_schema):
        con = self.connection
        cur = con.cursor()
        # Drop the existing table
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        # Create the new table with the updated schema
        cur.execute(updated_schema)
        con.commit()
        con.close()
    '''