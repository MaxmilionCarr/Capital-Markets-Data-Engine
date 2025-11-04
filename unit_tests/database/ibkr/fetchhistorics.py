import asyncio
from datetime import datetime, timedelta
import pandas as pd
from ib_async import IB
from ib_async.contract import Stock
import nest_asyncio
nest_asyncio.apply()

file_path = "C:\\Users\\maxmi\\OneDrive - The University of Melbourne\\Desktop\\TradingProject\\unit_tests\\database\\"

async def fetch_range(symbol, start_date, end_date):
    ib = IB()
    try:
        await ib.connectAsync("127.0.0.1", 55000, clientId=1, timeout=10)

        contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(contract)

        df_list = []
        current_end = end_date

        while current_end > start_date:
            bars = await ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime=current_end.strftime("%Y%m%d %H:%M:%S"),
                durationStr="2 D",
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=True
            )

            if not bars:
                break

            chunk_df = pd.DataFrame([{
                "datetime": b.date,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume
            } for b in bars])

            df_list.append(chunk_df)
            current_end -= timedelta(days=2)

        all_df = (
            pd.concat(df_list)
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        return all_df

    finally:
    # Always disconnect and close loop even on failure
        if ib.isConnected():
            ib.disconnect()  # not awaited


if __name__ == "__main__":
    symbol = "AAPL"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 10)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(fetch_range(symbol, start_date, end_date))
        df.to_csv(file_path + "sample_historics.csv", index=False)
        print(df)
    except Exception as e:
        print(e)
    finally:
        if not loop.is_closed():
            loop.close()
