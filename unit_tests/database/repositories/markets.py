from database.db import DataBase
import sqlite3 as sql
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
csv_path = os.getenv("CSV_CONTRACT")
DB = DataBase(db_path=test_env_path)
MARKET = DB.market_repo

# NEED (MARKET_NAME, EXCHANGE_ID)

def fetch_exchange_id(exchange_name, path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo
    exchange = exch_repo.get_info(exchange_name=exchange_name)
    if exchange:
        return exchange._id
    else:
        raise ValueError(f"Exchange '{exchange_name}' not found.")

def create_markets():
    market = MARKET
    print("Creating markets...")
    try:
        df = pd.read_csv(csv_path)
        markets = df[['stockType', 'contract.primaryExchange']]
        markets.columns = ['market_name', 'exchange_name']
        markets = pd.concat([markets, pd.DataFrame({'market_name': ['TEST_MARKET'], 'exchange_name': ['TEST_EXCHANGE']})])
        print(markets)
        for _, row in markets.drop_duplicates().iterrows():
            try:
                market.create(fetch_exchange_id(row['exchange_name']), row['market_name'])
                print(f"Created market: {row['market_name']}")
            except sql.IntegrityError:
                print(f"Market already exists: {row['market_name']}")
    except Exception as e:
        print(f"Error: {e}")


def create_duplicate_record():
    market = MARKET
    print("Creating duplicate market for testing...")
    try:
        df = pd.read_csv(csv_path)
        markets = df[['stockType', 'contract.primaryExchange']]
        markets.columns = ['market_name', 'exchange_name']
        first_row = markets.drop_duplicates().iloc[0]
        try:
            market.create(fetch_exchange_id(first_row['exchange_name']), first_row['market_name'])
            print(f"Created market: {first_row['market_name']}")
        except sql.IntegrityError:
            print("Market already exists: DUPLICATE_MARKET")
    except Exception as e:
        print(f"Error: {e}")

def fetch_all_markets():
    market = MARKET
    print("Fetching all markets...")
    try:
        all_markets = market.get_all()
        for market in all_markets:
            print(f"Market ID: {market._id}, Name: {market.name}, Exchange ID: {market.exchange_id}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_single_market(market_name):
    market = MARKET
    print(f"Fetching market: {market_name}...")
    try:
        market = market.get_info(market_name=market_name)
        if market:
            print(f"Market ID: {market._id}, Name: {market.name}, Exchange ID: {market.exchange_id}")
        else:
            print("Market not found.")
    except Exception as e:
        print(f"Error: {e}")

def update_market(market_name, new_exchange_id):
    market = MARKET
    print(f"Updating market: {market_name} to exchange ID {new_exchange_id}...")
    try:
        market_info = market.get_info(market_name=market_name)
        if market_info:
            market.update(exchange_id=market_info._exchange_id, market_id=market_info._id, market_name="UPDATED_MARKET")
            print("Market updated successfully.")
        else:
            print("Market not found.")
    except Exception as e:
        print(f"Error: {e}")

def delete_market(market_name):
    market = MARKET
    print(f"Deleting market: {market_name}...")
    try:
        market = market.get_info(market_name=market_name)
        if market:
            market.delete(market_id=market._id)
            print("Market deleted successfully.")
        else:
            print("Market not found.")
    except Exception as e:
        print(f"Error: {e}")


        
if __name__ == "__main__":
    create_markets()
    create_duplicate_record()
    fetch_all_markets()
    fetch_single_market("NASDAQ")
    update_market("TEST_MARKET", "TEST_EXCHANGE")
    delete_market("TEST_MARKET")