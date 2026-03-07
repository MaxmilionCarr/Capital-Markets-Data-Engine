from datetime import datetime
from data_providers import  IBKRConfig, IBKRService, DataHubConfig, DataHub, FMPConfig, FMPService
from database_connector import DB, DataBase
import pandas as pd
import csv
import os

api_key = os.getenv("FMP_API_KEY")
db_path = "./testing.db"
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

    db.get_equity(
        symbol="AAPL",
        exchange_name="NASDAQ",
        ensure=True
    )

    GOOGL_equity = db.get_equity(
        symbol="GOOGL",
        exchange_name="NASDAQ",
        ensure=True
    )

    first_issuer = GOOGL_equity.issuer

    GOOG_equity = db.get_equity(
        symbol="GOOG",
        exchange_name="NASDAQ",
        ensure=True
    )

    second_issuer = GOOG_equity.issuer

    print(f"First issuer: {first_issuer}")
    print(f"Second issuer: {second_issuer}")

    income_statement = first_issuer.get_statements(
        statement_type="income_statement",
        period="annual",
        look_back=5,
        ensure=True
    )

    print(f"Income statement for {first_issuer}:")
    print(income_statement[0])

    print("Alphabet Equities:")
    for eq in first_issuer.get_equities():
        print(eq)



if __name__ == "__main__":
    create_test_database()
    main()

    