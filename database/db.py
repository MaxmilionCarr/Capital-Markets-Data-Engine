# TODO: need a better name for this top level access. This is where users will get all their data from
# FIXME: need to handle connection closing better in handling duplicate creations
from __future__ import annotations
import sqlite3 as sql
from .core.exchanges import ExchangeRepository
from .core.markets import MarketRepository
from .instruments.tickers import TickerRepository, EquitiesRepository
from .technical_data.historical_prices import HistoricalPricesRepository
import os
from dataclasses import dataclass
from functools import cached_property

try:
    from dotenv import load_dotenv
except ImportError:
    raise ImportError("Please install python-dotenv to manage environment variables.")
try:
    load_dotenv()  # Load environment variables from a .env file
    env_path = os.getenv("DATABASE_PATH")
except Exception as e:
    print("Not using environment variables, please configure your .env file.")

@dataclass
class Data:
    db_path: str

    def get_exchange(self, exchange_name: str):
        db = DataBase(self.db_path)
        return db.exchange_repo.get_info(exchange_name=exchange_name)
    
    def get_market(self, market_name: str, exchange_name: str):
        db = DataBase(self.db_path)
        exchange_id = db.exchange_repo.get_info(exchange_name=exchange_name)._id
        return db.market_repo.get_info(exchange_id=exchange_id, market_name=market_name)
    
    def get_ticker(self, symbol: str, exchange_name: str, market_name: str):
        db = DataBase(self.db_path)
        exchange = db.exchange_repo.get_info(exchange_name=exchange_name)
        if not exchange:
            return None
        market = db.market_repo.get_info(exchange_id=exchange._id, market_name=market_name)
        if not market:
            return None
        return market.get_ticker(ticker_symbol=symbol)

class DataBase:
    # TODO: add flow down identifiers for exchange, market when creating tickers so don't have to ladder up
    def __init__(self, db_path=env_path):
        self.path = db_path
        self.connection = sql.connect(db_path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.exchange_repo = ExchangeRepository(self.connection)
        self.market_repo = MarketRepository(self.connection)
        self.ticker_repo = TickerRepository(self.connection)
        self.equity_repo = EquitiesRepository(self.connection)
        self.historical_price_repo = HistoricalPricesRepository(self.connection)

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
                        timezone TEXT NOT NULL
                    )''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_exchanges_name ON exchanges (exchange_name)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS markets (
                        market_id INTEGER NOT NULL,
                        exchange_id INTEGER NOT NULL,
                        market_name TEXT NOT NULL,
                        PRIMARY KEY (market_id, exchange_id),
                        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
                    )''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_markets_name ON markets (market_name)''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS tickers (
                        ticker_id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        market_id INTEGER NOT NULL,
                        exchange_id INTEGER NOT NULL,
                        currency TEXT NOT NULL,
                        full_name TEXT,
                        description TEXT,
                        source TEXT NOT NULL,
                        UNIQUE(symbol, exchange_id),
                        FOREIGN KEY (market_id, exchange_id) REFERENCES markets(market_id, exchange_id) ON DELETE CASCADE
                    )''')
        
        # --- Market Specific Tables ---

        cur.execute('''CREATE TABLE IF NOT EXISTS equities (
                        ticker_id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        sector TEXT,
                        industry TEXT,
                        dividend_yield REAL,
                        pe_ratio REAL,
                        eps REAL,
                        beta REAL,
                        market_cap REAL,
                        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
                    )''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS bonds (
                        ticker_id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        maturity_date DATE,
                        coupon_rate REAL,
                        yield_to_maturity REAL,
                        credit_rating TEXT,
                        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
                    )''')

        # --- Equities Tables ---   
        cur.execute('''CREATE TABLE IF NOT EXISTS historical_prices (
                        ticker_id INTEGER NOT NULL REFERENCES tickers(ticker_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (ticker_id, datetime)
                    )''')
        
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_ticker_time ON historical_prices (ticker_id, datetime)''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS option_chains (
                        option_id INTEGER PRIMARY KEY,
                        ticker_id INTEGER NOT NULL REFERENCES equity_data(ticker_id) ON DELETE CASCADE,
                        symbol TEXT NOT NULL,
                        creation_date DATE,
                        expiration_date DATE NOT NULL,
                        strike_price REAL NOT NULL,
                        option_type TEXT NOT NULL,
                        UNIQUE(ticker_id, expiration_date, strike_price, option_type)
                    )''')
        
        cur.execute('''CREATE TABLE IF NOT EXISTS option_prices (
                        option_id INTEGER NOT NULL REFERENCES option_chains(option_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        bid REAL,
                        ask REAL,
                        last_price REAL NOT NULL,
                        volume INTEGER,
                        open_interest INTEGER,
                        PRIMARY KEY (option_id, datetime)
                    )''')
        
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_option_prices_id_time ON option_prices (option_id, datetime)''')

        # --- Bonds Tables ---
        # TO BE ADDED

        # --- Check --- 
        res = cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print("Tables in database:")
        for row in res:
            print(row[0])
        con.commit()
        con.close()
    
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