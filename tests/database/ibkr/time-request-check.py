from data_providers import *
from database_connector import DB
from dotenv import load_dotenv
import os
from datetime import datetime
import matplotlib.pyplot as plt

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
config = DataHubConfig(
    market_services=[IBKRService(IBKRConfig())],
    fundamental_services=[FMPService(FMPConfig(api_key=os.getenv("API_KEY")))]
)

load_dotenv()

def time_request_check(ticker_symbol="AAPL", exchange_name="NASDAQ", start_date=None, end_date=None):
    
    start = datetime.now()


    db = DB(db_path = test_env_path, config=config)
    
    ticker = db.get_ticker(ticker_symbol, exchange_name, ensure=True)
    print(ticker)
    
    exchange = ticker.get_exchange()
    print(exchange)
    
    equity = ticker.get_equity(ensure=True)
    print(equity)

    prices = equity.get_prices(start_date=start_date, end_date=end_date, period="1 hour", ensure=True)
    
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

if __name__ == "__main__":
    start_date = datetime(2024, 7, 1)
    
    # One day test
    print("----- ONE DAY TEST -----")
    duration_day, prices_day = time_request_check(
        ticker_symbol="SHOP",
        exchange_name="NASDAQ",
        start_date=start_date,
        end_date=datetime(2024, 7, 2, 16, 0, 0)
    )
    print("----- Prices -----")
    print(prices_day)
    
    # One week test
    print("----- ONE WEEK TEST -----")
    duration_week, prices_week = time_request_check(
        ticker_symbol="META",
        exchange_name="NASDAQ",
        start_date=start_date,
        end_date=datetime(2024, 7, 8, 16, 0, 0)
    )
    print("----- Prices -----")
    print(prices_week)
    
    # One month test
    print("----- ONE MONTH TEST -----")
    duration_month, prices_month = time_request_check(
        ticker_symbol="ORCL",
        exchange_name="NASDAQ",
        start_date=start_date,
        end_date=datetime(2024, 7, 31, 16, 0, 0)
    )
    print("----- Prices -----")
    print(prices_month)

    
    # One year test
    print("----- ONE YEAR TEST -----")
    duration_year, prices_year = time_request_check(
        ticker_symbol="AAPL",
        exchange_name="NASDAQ",
        start_date=start_date,
        end_date=datetime(2025, 7, 1, 16, 0, 0)
    )
    print("----- Prices -----")
    open("prices_year_9_day.csv", "w").write(prices_year.to_csv())
    print(prices_year)
    
    print("----- SUMMARY OF DURATIONS -----")

    print(f"Duration for 1 day: {duration_day}")
    print(f"Duration for 1 week: {duration_week}")
    print(f"Duration for 1 month: {duration_month}")

    print(f"Duration for 1 year: {duration_year}")
    
    
    