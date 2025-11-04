from database.db import DataBase
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

checklist = {
    "Database Creation" : 0,
    "Exchange Population" : 0,
    "Market Population" : 0,
    "Database Deletion" : 0,
}
  
# Pre Populate DB
def populate_exchanges(path = test_env_path):
    print("Populating exchanges...")
    db = DataBase(path)
    exch_repo = db.exchange_repo

    exchanges = [
        ("NASDAQ", "America/New_York"),
        ("LSE", "Europe/London"),
        ("JPX", "Asia/Tokyo"),
        ("SSE", "Asia/Shanghai"),
    ]

    for name, timezone in exchanges:
        try:
            exch_repo.create(name, timezone)
            print(f"Created exchange: {name}")
            checklist["Exchange Population"] = 1
        except sql.IntegrityError:
            print(f"Exchange {name} already exists.")
            checklist["Exchange Population"] = 0

def populate_markets(path = test_env_path):
    print("Populating markets...")
    db = DataBase(path)
    market_repo = db.market_repo
    exch_repo = db.exchange_repo

    # Fetch exchange IDs
    nasdaq_id = exch_repo.get_or_create("NASDAQ", timezone="America/New_York")

    markets = [
        (1, nasdaq_id),  # STOCKS on NASDAQ
    ]

    for market_id, exchange_id in markets:
        try:
            market_repo.create(market_id, exchange_id)
            print(f"Created market ID {market_id} for exchange ID {exchange_id}")
            checklist["Market Population"] = 1
        except sql.IntegrityError:
            print(f"Market ID {market_id} for exchange ID {exchange_id} already exists.")
            checklist["Market Population"] = 0

def test_database_create(path = test_env_path):
    print("Creating database...")
    try:
        db = DataBase(path)
        db.create_db()
    except Exception as e:
        print("Failed to create database:", e)
        checklist["Database Creation"] = 0
        return -1
    checklist["Database Creation"] = 1
    return 1

def test_database_delete(path = test_env_path):
    print("Deleting database...")
    try:
        db = DataBase(path)
        db.delete_db()
        time.sleep(2)
    except Exception as e:
        print("Failed to delete database:", e)
        checklist["Database Deletion"] = 0
        return
    checklist["Database Deletion"] = 1

def basic_tests():
    print("CORE DATABASE TESTS")
    run = test_database_delete()
    if run == -1:
        return
    test_database_create()
    populate_exchanges()
    populate_markets()
    for key, value in checklist.items():
        status = "Passed" if value == 1 else "Failed"
        print(f"{key}: {status}")
    print("Database setup and population complete.")