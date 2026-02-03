from database.db import DB
from .exchanges import create_test_exchange
import sqlite3 as sql
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
csv_path = os.getenv("CSV_CONTRACT")
database = DB(db_path=test_env_path)
TICKER = database._hub.ticker_repo
EXCHANGE = database._hub.exchange_repo
use_cols = ['contract.localSymbol', 'stockType', 'contract.primaryExchange', 'contract.currency', 'longName']

# NEED (Symbol, Market ID, exchange ID, Currency, Full name, Description, Source)

def create_test_ticker():
    ticker = TICKER
    exch = EXCHANGE
    print("Creating test ticker...")
    try:
        ticker.create("TEST", exch.get_info(exchange_name="TEST_EXCHANGE")._id, currency="USD", full_name="Test Ticker", source="IB")
        print("Created ticker: TEST_TICKER")
    except sql.IntegrityError:
        print("Ticker already exists: TEST_TICKER")

def create_tickers():
    ticker = TICKER
    exch = EXCHANGE
    print("Creating tickers...")
    try:
        df = pd.read_csv(csv_path)
        tickers = df[use_cols]
        tickers.columns = ['symbol', 'market_name', 'exchange_name', 'currency', 'full_name']
        for _, row in tickers.iterrows():
            try:
                ticker.create(row['symbol'], exch.get_info(exchange_name=row['exchange_name'])._id,
                              currency=row['currency'], full_name=row['full_name'], source="IB")
                print(f"Created ticker: {row['symbol']}")
            except sql.IntegrityError:
                print(f"Ticker already exists: {row['symbol']}")
    except Exception as e:
        print(f"Error: {e}")
    
    create_test_ticker()


def create_duplicate_record():
    ticker = TICKER
    exch = EXCHANGE
    print("Creating duplicate ticker for testing...")
    try:
        df = pd.read_csv(csv_path)
        tickers = df[use_cols]
        tickers.columns = ['symbol', 'market_name', 'exchange_name', 'currency', 'full_name']
        first_row = tickers.drop_duplicates().iloc[0]
        try:
            ticker.create(first_row['symbol'], exch.get_info(exchange_name=first_row['exchange_name'])._id, 
                          currency=first_row['currency'], full_name=first_row['full_name'], source="IB")
            print(f"Created ticker: {first_row['symbol']}")
        except sql.IntegrityError:
            print("Ticker already exists: DUPLICATE_TICKER")
    except Exception as e:
        print(f"Error: {e}")

def fetch_all_tickers():
    ticker = TICKER
    print("Fetching all tickers...")
    try:
        all_tickers = ticker.get_all()
        for ticker in all_tickers:
            print(f"Ticker ID: {ticker._id}, Symbol: {ticker.symbol}, Exchange ID: {ticker._exchange_id}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_single_ticker(symbol, market_name, exchange_name=None):
    ticker = TICKER
    exch = EXCHANGE
    print(f"Fetching ticker: {symbol}...")
    try:
        ticker_info = ticker.get_info(exch.get_info(exchange_name=exchange_name)._id, symbol=symbol)
        if ticker_info:
            print(f"Ticker: {ticker_info.symbol}, Long Name: {ticker_info.full_name}, Currency: {ticker_info.currency}, Exchange ID: {ticker_info._exchange_id}")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")

def update_ticker(symbol, prev_long_name, new_long_name, exchange_name, market_name):
    ticker = TICKER
    exch = EXCHANGE
    print(f"Updating ticker: {symbol} from {prev_long_name} to {new_long_name}...")
    try:
        ticker_info = ticker.get_info(exch.get_info(exchange_name=exchange_name)._id, symbol=symbol)
        if ticker_info:
            ticker.update(ticker_info._id, full_name=new_long_name)
            print("Ticker updated successfully.")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")

def delete_ticker(exchange, market_name, symbol):
    ticker = TICKER
    exch = EXCHANGE
    print(f"Deleting ticker: {symbol}...")
    try:
        ticker_info = ticker.get_info(exch.get_info(exchange_name=exchange)._id, symbol=symbol)
        if ticker_info:
            ticker.delete(ticker_id=ticker_info._id)
            print("Ticker deleted successfully.")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")

def ticker_tests():
    print("TICKER TEST SUITE")
    create_test_exchange()
    create_tickers()
    create_duplicate_record()
    fetch_all_tickers()
    fetch_single_ticker("AAPL", "COMMON", "NASDAQ")
    update_ticker("TEST", "Test Ticker", "Updated Test Ticker", "TEST_EXCHANGE", "TEST_MARKET")
    delete_ticker("TEST_EXCHANGE", "TEST_MARKET", "TEST")