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
EXCHANGE = DB.exchange_repo

# NEED (EXCHANGE_NAME, EXCHANGE_TIMEZONE)

def create_exchanges():
    exch = EXCHANGE
    print("Creating exchanges...")
    try:
        df = pd.read_csv(csv_path)
        exchanges = df[['contract.primaryExchange', 'timeZoneId']]
        exchanges.columns = ['exchange_name', 'exchange_timezone']
        exchanges = pd.concat([exchanges, pd.DataFrame({'exchange_name': ['TEST_EXCHANGE'], 'exchange_timezone': ['TEST_TIMEZONE']})])
        for _, row in exchanges.drop_duplicates().iterrows():
            try:
                exch.create(row['exchange_name'], row['exchange_timezone'])
                print(f"Created exchange: {row['exchange_name']}")
            except sql.IntegrityError:
                print(f"Exchange already exists: {row['exchange_name']}")
    except Exception as e:
        print(f"Error: {e}")


def create_duplicate_record():
    exch = EXCHANGE
    print("Creating duplicate exchange for testing...")
    try:
        df = pd.read_csv(csv_path)
        exchanges = df[['contract.primaryExchange', 'timeZoneId']]
        exchanges.columns = ['exchange_name', 'exchange_timezone']
        first_row = exchanges.drop_duplicates().iloc[0]
        try:
            exch.create(first_row['exchange_name'], first_row['exchange_timezone'])
            print(f"Created exchange: {first_row['exchange_name']}")
        except sql.IntegrityError:
            print("Exchange already exists: DUPLICATE_EXCHANGE")
    except Exception as e:
        print(f"Error: {e}")

def fetch_all_exchanges():
    exch = EXCHANGE
    print("Fetching all exchanges...")
    try:
        all_exchanges = exch.get_all()
        for exchange in all_exchanges:
            print(f"Exchange ID: {exchange._id}, Name: {exchange.name}, Timezone: {exchange.timezone}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_single_exchange(exchange_name):
    exch = EXCHANGE
    print(f"Fetching exchange: {exchange_name}...")
    try:
        exchange = exch.get_info(exchange_name=exchange_name)
        if exchange:
            print(f"Exchange ID: {exchange._id}, Name: {exchange.name}, Timezone: {exchange.timezone}")
        else:
            print("Exchange not found.")
    except Exception as e:
        print(f"Error: {e}")

def update_exchange(exchange_name, new_timezone):
    exch = EXCHANGE
    print(f"Updating exchange: {exchange_name} to timezone {new_timezone}...")
    try:
        exchange = exch.get_info(exchange_name=exchange_name)
        if exchange:
            exch.update(exchange_id=exchange._id, timezone=new_timezone)
            print("Exchange updated successfully.")
        else:
            print("Exchange not found.")
    except Exception as e:
        print(f"Error: {e}")

def delete_exchange(exchange_name):
    exch = EXCHANGE
    print(f"Deleting exchange: {exchange_name}...")
    try:
        exchange = exch.get_info(exchange_name=exchange_name)
        if exchange:
            exch.delete(exchange_id=exchange._id)
            print("Exchange deleted successfully.")
        else:
            print("Exchange not found.")
    except Exception as e:
        print(f"Error: {e}")


        
if __name__ == "__main__":
    create_exchanges()
    create_duplicate_record()
    fetch_all_exchanges()
    fetch_single_exchange("NASDAQ")
    update_exchange("TEST_EXCHANGE", "TEST_TIMEZONE")
    delete_exchange("TEST_EXCHANGE")