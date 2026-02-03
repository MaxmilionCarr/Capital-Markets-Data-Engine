from database.db import DB, Config
from database.data.providers.IBKR_provider import IBKRConfig
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

load_dotenv()


def main():
    config = Config(
        provider = "IBKR",
        provider_config = IBKRConfig(),
    )
    
    db = DB(db_path = test_env_path, _config = config)
    db._hub.service
    
    ticker = db.get_ticker("AAPL", "NASDAQ", ensure=True)
    print(ticker)
    
    equity = ticker.get_equity(ensure=True)
    print(equity)

    prices = equity.get_prices(start_date=datetime(2026, 1, 29), period="1 day", ensure=True)
    print("----- FINAL PRICES -----")
    print(prices)

    
if __name__ == "__main__":
    main()
    
    