from database.db import DataBase
from .exchanges import create_test_exchange
from .markets import create_test_market, fetch_exchange_id
import sqlite3 as sql
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
csv_path = os.getenv("CSV_CONTRACT")
DB = DataBase(db_path=test_env_path)
TICKER = DB.ticker_repo
use_cols = ['contract.localSymbol', 'stockType', 'contract.primaryExchange', 'contract.currency', 'longName']

# NEED (Symbol, Market ID, exchange ID, Currency, Full name, Description, Source)

def create_test_ticker():
    ticker = TICKER
    print("Creating test ticker...")
    try:
        ticker.create("TEST", fetch_market_id("TEST_MARKET", "TEST_EXCHANGE"), fetch_exchange_id("TEST_EXCHANGE"), currency="USD", full_name="Test Ticker", description="TEST TICKER", source="IB")
        print("Created ticker: TEST_TICKER")
    except sql.IntegrityError:
        print("Ticker already exists: TEST_TICKER")

def fetch_market_id(market_name, exchange_name, path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo
    exchange_id = fetch_exchange_id(exchange_name, path)
    market = market_repo.get_info(exchange_id=exchange_id, market_name=market_name)
    if market:
        return market._id
    else:
        raise ValueError(f"Market '{market_name}' not found.")

def create_tickers():
    ticker = TICKER
    print("Creating tickers...")
    try:
        df = pd.read_csv(csv_path)
        tickers = df[use_cols]
        tickers.columns = ['symbol', 'market_name', 'exchange_name', 'currency', 'full_name']
        for _, row in tickers.iterrows():
            try:
                ticker.create(row['symbol'], fetch_market_id(row['market_name'], row['exchange_name']), fetch_exchange_id(row['exchange_name']),
                              currency=row['currency'], full_name=row['full_name'], description=None, source="IB")
                print(f"Created ticker: {row['symbol']}")
            except sql.IntegrityError:
                print(f"Ticker already exists: {row['symbol']}")
    except Exception as e:
        print(f"Error: {e}")
    
    create_test_ticker()


def create_duplicate_record():
    ticker = TICKER
    print("Creating duplicate ticker for testing...")
    try:
        df = pd.read_csv(csv_path)
        tickers = df[use_cols]
        tickers.columns = ['symbol', 'market_name', 'exchange_name', 'currency', 'full_name']
        first_row = tickers.drop_duplicates().iloc[0]
        try:
            ticker.create(first_row['symbol'], fetch_market_id(first_row['market_name'], first_row['exchange_name']), fetch_exchange_id(first_row['exchange_name']),
                          currency=first_row['currency'], full_name=first_row['full_name'], description=None, source="IB")
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
            print(f"Ticker ID: {ticker._id}, Symbol: {ticker.symbol}, Market ID: {ticker._market_id}, Exchange ID: {ticker._exchange_id}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_single_ticker(symbol, market_name, exchange_name=None):
    ticker = TICKER
    print(f"Fetching ticker: {symbol}...")
    try:
        ticker_info = ticker.get_info(fetch_exchange_id(exchange_name), fetch_market_id(market_name, exchange_name), symbol=symbol)
        if ticker_info:
            print(f"Ticker: {ticker_info.symbol}, Long Name: {ticker_info.full_name}, Currency: {ticker_info.currency}, Exchange ID: {ticker_info._exchange_id}")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")

def update_ticker(symbol, prev_long_name, new_long_name, exchange_name, market_name):
    ticker = TICKER
    print(f"Updating ticker: {symbol} from {prev_long_name} to {new_long_name}...")
    try:
        ticker_info = ticker.get_info(fetch_exchange_id(exchange_name), fetch_market_id(market_name, exchange_name), symbol=symbol)
        if ticker_info:
            ticker.update(ticker_info._id, full_name=new_long_name)
            print("Ticker updated successfully.")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")

def delete_ticker(exchange, market_name, symbol):
    ticker = TICKER
    print(f"Deleting ticker: {symbol}...")
    try:
        ticker_info = ticker.get_info(fetch_exchange_id(exchange), fetch_market_id(market_name, exchange), symbol=symbol)
        if ticker_info:
            ticker.delete(ticker_id=ticker_info._id)
            print("Ticker deleted successfully.")
        else:
            print("Ticker not found.")
    except Exception as e:
        print(f"Error: {e}")


        
if __name__ == "__main__":
    create_test_exchange()
    create_test_market()
    create_tickers()
    create_duplicate_record()
    fetch_all_tickers()
    fetch_single_ticker("AAPL", "COMMON", "NASDAQ")
    update_ticker("TEST", "Test Ticker", "Updated Test Ticker", "TEST_EXCHANGE", "TEST_MARKET")
    delete_ticker("TEST_EXCHANGE", "TEST_MARKET", "TEST")