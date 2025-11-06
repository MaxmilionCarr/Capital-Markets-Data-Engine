from database.core import exchanges, markets
from database.instruments import tickers
from database.technical_data import historical_prices
import sqlite3 as sql

from ib_insync import *

import time

def test_connection():
    ib = IB()
    try:
        ib.connect("127.0.0.1", 7497, clientId=1)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    else:
        print("✅ Connection successful")
        return ib

if __name__ == "__main__":
    '''
    retry = 0
    while retry < 5:
        if ib := test_connection():
            break
        retry += 1
        print(f"Retrying... ({retry}/5)")
        time.sleep(2)
    '''

    ''' 
    sql_connection = sql.connect('database/marketdata.db')
    exch_repo = exchanges.ExchangeRepository(sql_connection)
    exch_repo.create("NYSE", "America/New_York")
    exch_repo.create("NASDAQ", "America/New_York")
    exch_repo.delete(exchange_id=2)

    exid = exch_repo.get_or_create("NYSE")
    info = exch_repo.get_info(exid)
    print(info)

    print(exch_repo.get_all())
    '''

# TODO: Idea to fetch market data is when a user calls from a object, e.g. ticker.get_prices(), 
# It will check if data exists using fetch function, if sql error then ib sync will fetch.
