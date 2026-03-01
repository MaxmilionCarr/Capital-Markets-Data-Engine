from data_providers import *
from database_connector import DB
from dotenv import load_dotenv
import os
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

def time_request_check(ticker_symbol="AAPL", exchange_name="NASDAQ", config=None, start_date=None, end_date=datetime(2026, 2, 20),):
    
    start = datetime.now()


    db = DB(db_path = test_env_path, config=config)
    
    ticker = db.get_ticker(ticker_symbol, exchange_name, ensure=True)
    print(ticker)
    
    exchange = ticker.get_exchange()
    print(exchange)
    
    equity = ticker.get_equity(ensure=True)
    print(equity)

    prices = equity.get_prices(start_date=start_date, end_date=end_date, period="5 mins", ensure=True)
    
    end = datetime.now()
    
    return end - start, prices

def plot_prices(prices):
    plt.figure(figsize=(12, 6))
    plt.plot(prices.index, prices['close'], label='Close Price')
    plt.title('Equity Prices Over Time')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()

def check_similarity(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    # Check if the two DataFrames are equal
    return df1.equals(df2)

def get_config(limit, count):

    pacer = _HistPacer(
        max_10min=limit
    )

    config = DataHubConfig(
        market_services=[IBKRService(IBKRConfig(
            client_id=count,
            pacer=pacer
            ))],
        fundamental_services=[FMPService(FMPConfig(api_key=os.getenv("API_KEY")))]
    )

    return config

if __name__ == "__main__":
    start_date = datetime(2020, 1, 1)
    
    # One year test
    print("----- 5 Year Test (100 Requests per 10 Minutes) -----")
    config = get_config(limit=100, count=1)
    test1, prices_test1 = time_request_check(
        ticker_symbol="AMZN",
        exchange_name="NASDAQ",
        config=config,
        start_date=start_date,
    )
    print("Prices")
    print(prices_test1)
    print("Duration")
    print(test1)
    '''
    print("----- 5 Year Test ( Requests per 10 Minutes) -----")
    config = get_config(limit=80, count=2)

    test2, prices_test2 = time_request_check(
        ticker_symbol="AAPL",
        exchange_name="NASDAQ",
        config=config,
        start_date=start_date,
        end_date=datetime(2025, 7, 1, 16, 0, 0)
    )
    
    print(test2)
    '''
        
    print("----- SUMMARY OF DURATIONS -----")
    print(f"Duration for 5 year with 100 requests per 10 minutes: {test1}")
    #print(f"Duration for 5 year with 80 requests per 10 minutes: {test2}")
