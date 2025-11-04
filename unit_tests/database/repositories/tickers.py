from database.db import DataBase
from .markets import create_test_exchange
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

# --- Ticker Tests ---
def fetch_exchange_id(exchange_name, path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo
    exchange = exch_repo.get_info(exchange_name=exchange_name)
    if exchange:
        return exchange.id
    else:
        raise ValueError(f"Exchange '{exchange_name}' not found.")

def create_test_market(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo
    try:
        market_repo.create(1, fetch_exchange_id("TEST_EXCHANGE"))
        print("Test market created.")
    except sql.IntegrityError:
        print("Test market already exists.")
    db.close_db()

def test_ticker_creation(path = test_env_path):
    db = DataBase(path)
    ticker_repo = db.ticker_repo

    print("Creating ticker...")
    try:
        ticker_repo.create("TEST_TICKER", 1, fetch_exchange_id("TEST_EXCHANGE"), currency="USD", full_name="Test Ticker", description="This is a test ticker.", source="manual")
        print("Ticker created successfully.")
        return True
    except sql.IntegrityError:
        print("Ticker already exists.")
        db.close_db()
        return False

def test_ticker_retrieval(path = test_env_path):
    db = DataBase(path)
    ticker_repo = db.ticker_repo

    print("Retrieving ticker...")
    ticker = ticker_repo.get_info(symbol="TEST_TICKER")
    if ticker:
        print(f"Ticker found: ID={ticker.id}, Symbol={ticker.symbol}, Currency={ticker.currency}, Full Name={ticker.full_name}, Description={ticker.description}, Source={ticker.source}")
        return True
    else:
        print("Ticker not found.")
        return False

# Tests all links to other repos here
def test_ticker_links(path = test_env_path):
    db = DataBase(path)
    ticker_repo = db.ticker_repo

    print("Retrieving ticker links...")
    ticker = ticker_repo.get_info(symbol="TEST_TICKER")
    # TODO: Add ticker repo as well
    if ticker:
        print(f"Ticker found: ID={ticker.id}, Symbol={ticker.symbol}, Market={ticker.market}, Exchange={ticker.exchange}")
        return True
    else:
        print("Ticker not found.")
        return False
    
def test_ticker_update(path = test_env_path):
    db = DataBase(path)
    ticker_repo = db.ticker_repo

    print("Updating ticker...")
    ticker = ticker_repo.get_info(symbol="TEST_TICKER")
    if ticker:
        ticker_repo.update(ticker.id, full_name="UPDATED_TEST_TICKER")
        updated_ticker = ticker_repo.get_info(ticker_id=ticker.id)
        print(f"Updated Ticker: ID={updated_ticker.id}, Full Name={updated_ticker.full_name}")
        return True
    else:
        print("Ticker not found for update.")
        return False

def test_ticker_deletion(path = test_env_path):
    db = DataBase(path)
    ticker_repo = db.ticker_repo

    print("Deleting ticker...")
    try:
        ticker_repo.delete(symbol="TEST_TICKER")
        print("Ticker deleted successfully.")
    except Exception as e:
        print("Failed to delete ticker:", e)
        return False
    

    # Confirm deletion
    time.sleep(1)  # Ensure DB has processed deletion
    ticker = ticker_repo.get_info(symbol="TEST_TICKER")
    if ticker is None:
        print("Ticker successfully deleted.")
        return True
    else:
        print("Failed to delete ticker.")
        return False

def ticker_tests():
    print("TICKER TESTS")
    create_test_exchange()
    create_test_market()
    check = True
    if not test_ticker_creation():
        print("Ticker creation test failed.")
        check = False
    if not test_ticker_retrieval():
        print("Ticker retrieval test failed.")
        check = False
    if not test_ticker_update():
        print("Ticker update test failed.")
        check = False
    if not test_ticker_links():
        print("Ticker links test failed.")
        check = False
    if not test_ticker_deletion():
        print("Ticker deletion test failed.")
        check = False
    if check:
        print("All ticker tests passed.")
    else:
        print("Some ticker tests failed.")
