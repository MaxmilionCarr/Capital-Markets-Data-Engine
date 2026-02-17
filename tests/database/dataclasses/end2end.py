from datetime import datetime
from database.db import Data
from database.core.exchanges import Exchange
from database.core.markets import Market
from database.instruments.tickers import Ticker
import os
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

DB = Data(db_path=test_env_path)

# Exchange Test
def exchange_test():
    exchange = DB.get_exchange("NASDAQ")
    assert exchange.name == "NASDAQ"
    for market in exchange.get_all_markets():
        print(market.name)
    for ticker in exchange.get_all_tickers():
        print(ticker.symbol, ticker._id)
    
# Market Test
def market_test(exchange: Exchange):
    market = exchange.get_market("COMMON")
    assert market.name == "COMMON"
    for ticker in market.get_all_tickers(): # Should be all the tickers in the NASDAQ COMMON Market
        print(ticker.symbol)

def ticker_test(market: Market):
    ticker = market.get_ticker("AAPL")
    print(ticker._id)
    assert ticker.symbol == "AAPL"
    print(ticker.get_prices(start_date=datetime(2022,1,3), end_date=None, period='5 Minutes'))

def end2end_test():
    exchange_test()
    market_test(DB.get_exchange("NASDAQ"))
    ticker_test(DB.get_exchange("NASDAQ").get_market("COMMON"))

if __name__ == "__main__":
    end2end_test()
