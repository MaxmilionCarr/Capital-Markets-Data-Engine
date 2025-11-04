from database.db import DataBase
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
  
# --- Exchange Tests ---
def test_exchange_creation(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo

    print("Creating exchange...")
    try:
        exch_repo.create("TEST_EXCHANGE", "UTC")
        print("Exchange created successfully.")
        return True
    except sql.IntegrityError:
        print("Exchange already exists.")
        return False

def test_exchange_retrieval(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo

    print("Retrieving exchange...")
    exchange = exch_repo.get_info(exchange_name="TEST_EXCHANGE")
    if exchange and exchange.id and exchange.name and exchange.timezone:
        print(f"Exchange found: ID={exchange.id}, Name={exchange.name}, Timezone={exchange.timezone}")
        return True
    else:
        print("Exchange not found.")
        return False

def test_exchange_links(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo
    # TODO: Add ticker_repo
    print("Retrieving exchange links...")
    exchange = exch_repo.get_info(exchange_name="TEST_EXCHANGE")
    if exchange:
        markets = exchange.markets
        print(f"Markets for exchange {exchange.name}:")
        for market in markets:
            print(f"- Market ID={market.market_id}, Name={market.market_name}")
        return True
    else:
        print("Exchange not found.")
        return False

def test_exchange_update(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo

    print("Updating exchange timezone...")
    exchange = exch_repo.get_info(exchange_name="TEST_EXCHANGE")
    if exchange:
        exch_repo.update(exchange.id, timezone="America/New_York")
        updated_exchange = exch_repo.get_info(exchange.id)
        print(f"Updated Timezone: {updated_exchange.timezone}")
        return True
    else:
        print("Exchange not found for update.")
        return False

def test_exchange_tickers(path = test_env_path):
    print("TODO: Implement ticker retrieval test.")

def test_exchange_deletion(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo

    print("Deleting exchange...")
    try:
        exch_repo.delete(exchange_name="TEST_EXCHANGE")
        print("Exchange deleted successfully.")
    except Exception as e:
        print("Failed to delete exchange:", e)
        return False
    

    # Confirm deletion
    time.sleep(1)  # Ensure DB has processed deletion
    exchange = exch_repo.get_info(exchange_name="TEST_EXCHANGE")
    if exchange is None:
        print("Exchange successfully deleted.")
        return True
    else:
        print("Failed to delete exchange.")
        return False

def exchange_tests():
    print("EXCHANGE TESTS")
    db = DataBase(test_env_path)
    exch_repo = db.exchange_repo
    exchanges = exch_repo.get_all()
    for exch in exchanges:
        print(f"Pre-existing exchange: ID={exch.id}, Name={exch.name}, Timezone={exch.timezone}")
    check = True
    if not test_exchange_creation():
        print("Exchange creation test failed.")
        check = False
    if not test_exchange_retrieval():
        print("Exchange retrieval test failed.")
        check = False
    if not test_exchange_update():
        print("Exchange update test failed.")
        check = False
    if not test_exchange_links():
        print("Exchange links test failed.")
        check = False
    if not test_exchange_deletion():
        print("Exchange deletion test failed.")
        check = False
    if check:
        print("All exchange tests passed.")
    else:
        print("Some exchange tests failed.")
