from datetime import datetime
from database.data.providers.IBKR_provider import IBKRProvider, IBKRConfig
from database.data.providers.FMP_provider import FMPProvider
from database.data.providers.base import Provider, TickerInfo
import pandas as pd
import csv

def main(symbols = None, exchange_name = None):
    cfg = IBKRConfig(
        host="127.0.0.1",
        port=55000,      # paper / gateway port
        client_id=1,
    )

    provider = IBKRProvider(cfg)

    print("Connecting to IBKR...")
    provider.connect()
    print("Connected ✅\n")
    

    df = provider.get_equity_prices("AAPL", "NASDAQ", start_date=datetime(2026, 1, 29), bar_size="1 day")
    print(df)
    
def test_bonds(CUSID):
    cfg = IBKRConfig(
        host="127.0.0.1",
        port=55000,      # paper / gateway port
        client_id=1,
    )

    provider = IBKRProvider(cfg)
    
    print("Connecting to IBKR...")
    provider.connect()
    print("Connected ✅\n")
    

    info = provider.get_bond(CUSID)
    print("Returned object type:", type(info))
    assert isinstance(info, TickerInfo)

    print("\n--- TickerInfo fields ---")
    print("symbol     :", info.symbol)
    print("exchange   :", info.exchange)
    print("currency   :", info.currency)
    print("long_name  :", info.long_name)
    print("sec_type   :", info.sec_type)
    print("industry   :", info.industry)
    print("timezone   :", info.timezone)
    print("provider   :", info.provider)

    print("Returned object type:", type(info))
    assert isinstance(info, TickerInfo)
    
    df = provider.get_bond_prices(CUSID, start_date="2026-01-23")
    print(df.head())
    print("\nDataFrame columns:", df.columns.tolist())

    expected_cols = {"datetime", "open", "high", "low", "close", "volume"}
    assert expected_cols.issubset(df.columns)

    print("\nHistorical prices check passed ✅")

    print("TickerInfo checks passed ✅\n")

    provider.disconnect()
    print("\nDisconnected cleanly ✅")

def test_fundamentals(symbol, exchange_name, provider: FMPProvider):
    print("Testing fundamentals retrieval...")
    df = provider.get_income_statement(symbol, prev_years=3, period="annual")
    print(df.head())
    print("\nDataFrame columns:", df.columns.tolist())

    print("\nFundamentals retrieval check passed ✅")
    open("fundamentals_test_output.csv", "w", newline="").write(df.to_csv(index=False))
    
    

if __name__ == "__main__":
    '''
    cfg = IBKRConfig(
        host="127.0.0.1",
        port=55000,      # paper / gateway port
        client_id=1,
    )

    provider = IBKRProvider(cfg)
    '''
    
    test_fundamentals("AAPL", "NASDAQ", FMPProvider(api_key="P3u8bDHisw87do2yElISKK9UPWQ3qPCJ"))
    