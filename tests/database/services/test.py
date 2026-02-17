from datahub.db import DB
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import *

load_dotenv()
db_path = os.getenv("TESTING_DATABASE_PATH")

def test_fundamentals():
    db = DB(db_path=db_path)

    ticker = db.get_ticker("AAPL", "NASDAQ", ensure=True)
    print("Ticker info:")
    print(ticker)

    # Test fetching fundamental functionality
    income_statement = ticker.get_statements("income_statement", period="annual", look_back=4, ensure=True)
    df = pd.DataFrame([s.statement for s in income_statement])
    print("\nIncome Statement (4 Years):")
    print(df)

    print("One Income Statement")
    print(df.iloc[0])

    balance_sheet = ticker.get_statements("balance_sheet", period="annual", look_back=4, ensure=True)
    df = pd.DataFrame([s.statement for s in balance_sheet])
    print("\nBalance Sheet (4 Years):")
    print(df)

    print("One Balance Sheet")
    print(df.iloc[0])
    
    cash_flow_statement = ticker.get_statements("cash_flow", period="annual", look_back=4, ensure=True)
    df = pd.DataFrame([s.statement for s in cash_flow_statement])
    print("\nCash Flow (4 Years):")
    print(df)

    print("One Cash Flow")
    print(df.iloc[0])

def test_market_data():
    db = DB(db_path=db_path)

    ticker = db.get_ticker("AAPL", "NASDAQ", ensure=True)
    print("Ticker info:")
    print(ticker)

    equity = ticker.get_equity(ensure=True)
    print("\nEquity info:")
    print(equity)

    prices = equity.get_prices(datetime(2023, 1, 1), datetime(2026, 2, 1), ensure=True)
    print("\nPrices:")
    print(prices)

if __name__ == "__main__":
    test_fundamentals()
    test_market_data()
