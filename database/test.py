from database.db import DB
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
db_path = os.getenv("TESTING_DATABASE_PATH")

def test_fundamentals():
    db = DB(db_path=db_path)

    ticker = db.get_ticker("AAPL", "NASDAQ", ensure=True)
    print("Ticker info:")
    print(ticker)

    # Test fetching fundamental functionality
    income_statement = ticker.get_statements("cash_flow", period="annual", look_back=4, ensure=True)
    df = pd.DataFrame([s.statement for s in income_statement])
    print("\nBalance Sheet (4 Years):")
    print(df)

    print("One Balance Sheet")
    print(df.iloc[0])

if __name__ == "__main__":
    test_fundamentals()
