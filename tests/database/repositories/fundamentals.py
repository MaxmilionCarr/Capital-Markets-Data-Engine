from database.db import DB
import sqlite3 as sql
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime
import matplotlib.pyplot as plt



load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
csv_path = os.getenv("CSV_PRICES")
db = DB(db_path=test_env_path)

# NEED ('ticker_id', 'datetime', 'open', 'high', 'low', 'close', 'volume')

def create_prices():
    TICKER = db.get_ticker("AAPL", "NASDAQ", ensure=True)
    exch_id = TICKER._exchange_id
    equity_prices = db._hub.equity_prices_repo
    print("Creating historical prices...")
    try:
        df = pd.read_csv(csv_path)
        prices = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
        prices.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        prices['datetime'] = pd.to_datetime(prices['datetime'], utc=False)
        prices['datetime'] = prices['datetime'].dt.tz_localize(None)
        prices['datetime'] = prices['datetime'].astype(str)
        for _, row in prices.iterrows():
            try:
                equity_prices.create(ticker_id=TICKER._id,
                                         datetime=row['datetime'],
                                         close=row['close'],
                                         open=row['open'],
                                         high=row['high'],
                                         low=row['low'],
                                         volume=row['volume'])
                print(f"Created historical prices for ticker: {TICKER.symbol}, {TICKER._id} at {row['datetime']}")
            except sql.IntegrityError:
                print(f"Historical price already exists for ticker: {TICKER.symbol} at {row['datetime']}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_all_prices():
    TICKER = db.get_ticker("AAPL", "NASDAQ")
    exch_id = TICKER._exchange_id
    equity_prices = db._hub.equity_prices_repo
    print("Fetching historical prices...")
    try:
        print(TICKER._id)
        prices = equity_prices.get_prices(ticker_id=TICKER._id, period='5 Minutes', start_date=datetime(2026,1,3))
        print(prices)
        return prices
    except Exception as e:
        print(f"Error: {e}")

def delete_all_prices():
    TICKER = db.get_ticker("AAPL", "NASDAQ")
    exch_id = TICKER._exchange_id
    equity_prices = db._hub.equity_prices_repo
    print("Deleting historical prices...")
    try:
        equity_prices.delete(ticker_id=TICKER._id)
    except Exception as e:
        print(f"Error: {e}")

def create_time_series(df: pd.DataFrame):
    print("Creating time series historical prices...")

    plt.plot(df['date'], df['close'], label='Close Prices of AAPL')
    plt.ylabel('Price')
    plt.title('Historical Prices')
    plt.legend()
    plt.show()
    
def historical_price_tests():
    print("HISTORICAL PRICE TEST SUITE")
    create_prices()
    df = fetch_all_prices()
    create_time_series(df)
    delete_all_prices()