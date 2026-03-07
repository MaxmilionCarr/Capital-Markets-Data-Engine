from datetime import datetime

from rpds import List
from data_providers import  IBKRConfig, IBKRService, DataHubConfig, DataHub, FMPConfig, FMPService
from database_connector import DB, DataBase
import pandas as pd
import csv
import os

api_key = os.getenv("FMP_API_KEY")
db_path = "./tests/database/repositories/testing.db"

def create_test_database():
    database = DataBase(db_path)
    database.create_db()

def main(symbols = None, exchange_name = None):
    ibkr_cfg = IBKRConfig(
        host="127.0.0.1",
        port=60000,      # paper / gateway port
        client_id=1,
    )

    fmp_cfg = FMPConfig(
        api_key=api_key,
    )



    datahub_cfg = DataHubConfig(
        market_services=(FMPService(fmp_cfg), IBKRService(ibkr_cfg)),
        fundamental_services=(FMPService(fmp_cfg),)
    )

    db = DB(db_path, datahub_cfg)

    print("Testing equity fetch with enrichment...")
    print("Fetching AAPL...")
    db.get_equity(
        symbol="AAPL",
        exchange_name="NASDAQ",
        ensure=True
    )

    print("\n")

    print("Fetching GOOGL...")
    GOOGL_equity = db.get_equity(
        symbol="GOOGL",
        exchange_name="NASDAQ",
        ensure=True
    )
    print("\n")

    first_issuer = GOOGL_equity.issuer

    print("Fetching GOOG...")
    GOOG_equity = db.get_equity(
        symbol="GOOG",
        exchange_name="NASDAQ",
        ensure=True
    )
    print("\n")

    second_issuer = GOOG_equity.issuer

    print("Issuer Check, should be the same")
    print(f"First issuer: {first_issuer}")
    print(f"Second issuer: {second_issuer}")
    print("\n")

    print("Fetching income statement...")
    income_statement = first_issuer.get_statements(
        statement_type="income_statement",
        period="annual",
        look_back=5,
        ensure=True
    )
    print("\n")

    print("Alphabet Equities:")
    for eq in first_issuer.get_equities():
        print(eq)

def multi_test(symbols: list[str]):
    ibkr_cfg = IBKRConfig(
        host="127.0.0.1",
        port=60000,      # paper / gateway port
        client_id=1,
    )

    fmp_cfg = FMPConfig(
        api_key=api_key,
    )



    datahub_cfg = DataHubConfig(
        market_services=(FMPService(fmp_cfg), IBKRService(ibkr_cfg)),
        fundamental_services=(FMPService(fmp_cfg),)
    )

    db = DB(db_path, datahub_cfg)

    for symbol in symbols:
        db.get_equity(
            symbol=symbol,
            exchange_name="NASDAQ",
            ensure=True
        )
    exchange = db.get_exchange("NASDAQ")

    print("Equities")
    equities = exchange.get_all_equities()
    for equity in equities:
        print(equity)
    print("\n")

    print("Issuers")
    for equity in equities:
        print(equity.issuer)

    



if __name__ == "__main__":
    if not os.path.exists(db_path):
        create_test_database()
    symbols = ["AAPL", "GOOGL", "GOOG", "MSFT", "AMZN", "SHOP"]
    multi_test(symbols)

    