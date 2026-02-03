from database.db import DB, Config
from database.data.providers.IBKR_provider import IBKRConfig
from dotenv import load_dotenv
import os
from datetime import datetime
import matplotlib.pyplot as plt

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

load_dotenv()

def time_request_check():
    start = datetime.now()
    
    config = Config(
        provider = "IBKR",
        provider_config = IBKRConfig(),
    )
    
    db = DB(db_path = test_env_path, _config = config)
    db._hub.service
    
    ticker = db.get_ticker("AMZN", "NASDAQ", ensure=True)
    print(ticker)
    
    exchange = ticker.get_exchange()
    print(exchange)
    
    equity = ticker.get_equity(ensure=True)
    print(equity)

    prices = equity.get_prices(start_date=datetime(2025, 12, 5), period="5 mins", ensure=True)
    print("----- FINAL PRICES -----")
    print(prices)
    
    end = datetime.now()
    print(f"Time taken: {end - start}")
    return prices

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
    '''
    print("COLD REQUEST")
    time_request_check()
    '''
    
    print("\nWARM REQUEST")
    prices = time_request_check()
    plot_prices(prices)
    