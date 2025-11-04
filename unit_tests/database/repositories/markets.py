from database.db import DataBase
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

# --- Market Tests ---
def fetch_exchange_id(exchange_name, path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo
    exchange = exch_repo.get_info(exchange_name=exchange_name)
    if exchange:
        return exchange.id
    else:
        raise ValueError(f"Exchange '{exchange_name}' not found.")
    
def create_test_exchange(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo
    try:
        exch_repo.create("TEST_EXCHANGE", "UTC")
        print("Test exchange created.")
    except sql.IntegrityError:
        print("Test exchange already exists.")
    db.close_db()

def test_market_creation(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo

    print("Creating market...")
    try:
        market_repo.create(1, fetch_exchange_id("TEST_EXCHANGE"))
        print("Market created successfully.")
        return True
    except sql.IntegrityError:
        print("Market already exists.")
        return False

def test_market_retrieval(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo

    print("Retrieving market...")
    market = market_repo.get_info(market_id=1, exchange_id=fetch_exchange_id("TEST_EXCHANGE"))
    if market:
        print(f"Market found: ID={market.id}, Name={market.name}")
        return True
    else:
        print("Market not found.")
        return False

# Tests all links to other repos here
def test_market_links(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo

    print("Retrieving market links...")
    market = market_repo.get_info(market_id=1, exchange_id=fetch_exchange_id("TEST_EXCHANGE"))
    # TODO: Add ticker repo as well
    if market:
        exchange = market.exchange
        print(f"Exchange for market {market.name}: ID={exchange.id}, Name={exchange.name}")
        return True
    else:
        print("Market not found.")
        return False
'''
def test_market_update(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo

    print("Updating market...")
    market = market_repo.get_info(market_id=1, exchange_id="TEST_EXCHANGE")
    if market:
        market_repo.update(market.id, name="UPDATED_TEST_MARKET")
        updated_market = market_repo.get_info(market.id, exchange_id="TEST_EXCHANGE")
        print(f"Updated Market: ID={updated_market.id}, Name={updated_market.name}")
        return True
    else:
        print("Market not found for update.")
        return False
'''

def test_market_deletion(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo

    print("Deleting market...")
    try:
        market_repo.delete(market_id=1, exchange_id=fetch_exchange_id("TEST_EXCHANGE"))
        print("Market deleted successfully.")
    except Exception as e:
        print("Failed to delete market:", e)
        return False
    

    # Confirm deletion
    time.sleep(1)  # Ensure DB has processed deletion
    market = market_repo.get_info(market_id=1, exchange_id=fetch_exchange_id("TEST_EXCHANGE"))
    if market is None:
        print("Market successfully deleted.")
        return True
    else:
        print("Failed to delete market.")
        return False

def market_tests():
    print("MARKET TESTS")

    create_test_exchange()
    check = True
    if not test_market_creation():
        print("Market creation test failed.")
        check = False
    if not test_market_retrieval():
        print("Market retrieval test failed.")
        check = False
    if not test_market_links():
        print("Market links test failed.")
        check = False
    if not test_market_deletion():
        print("Market deletion test failed.")
        check = False
    if check:
        print("All market tests passed.")
    else:
        print("Some market tests failed.")
