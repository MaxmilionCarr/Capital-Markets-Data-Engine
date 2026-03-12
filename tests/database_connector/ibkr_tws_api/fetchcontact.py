import asyncio
from ib_async import IB
from ib_async.contract import Stock, Index
import pandas as pd
import nest_asyncio
from datetime import datetime

nest_asyncio.apply()

# --- CONFIG ---
OUT_PATH = r"C:\\Users\\maxmi\\Desktop\\Financial-Data-API-Integration-Tool\\tests\\database\\ibkr"

# Instead of guessing, the user explicitly specifies the market type
# You can extend this dictionary to include Futures, Options, Bonds later.
SYMBOLS = [
    {"symbol": "AAPL", "type": "Stock"},
    {"symbol": "NVDA", "type": "Stock"},
    {"symbol": "SHOP", "type": "Stock"},
    {"symbol": "SPY", "type": "Stock"},
    {"symbol": "GOOGL", "type": "Stock"},
    {"symbol": "GOOG", "type": "Stock"},
]


# --- FACTORY FUNCTION ---
def make_contract(symbol: str, market_type: str):
    """Return the appropriate IBKR contract type based on the market type."""
    market_type = market_type.lower()
    if market_type == "stock":
        return Stock(symbol, "SMART", "USD")
    elif market_type == "index":
        return Index(symbol if symbol != "S&P500" else "SPX", "CBOE", "USD")
    else:
        raise ValueError(f"Unsupported market type: {market_type}")


# --- FETCH CONTRACT DETAILS ---
async def fetch_raw(symbol: str, market_type: str):
    ib = IB()
    try:
        await ib.connectAsync("127.0.0.1", 55000, clientId=2, timeout=10)

        contract = make_contract(symbol, market_type)

        try:
            # Timeout ensures we never hang on a symbol
            details = await asyncio.wait_for(ib.reqContractDetailsAsync(contract), timeout=15)
        except asyncio.TimeoutError:
            print(f"⚠️ Timeout while fetching {symbol}")
            return []

        if not details:
            print(f"❌ No contract details found for {symbol}")
            return []

        # Collect every available field (unprocessed)
        records = []
        for d in details:
            data = {}
            # ContractDetails fields
            for attr in dir(d):
                if not attr.startswith("_"):
                    try:
                        val = getattr(d, attr)
                        if not callable(val):
                            data[attr] = val
                    except Exception:
                        continue

            # Nested Contract fields
            if hasattr(d, "contract"):
                c = d.contract
                for attr in dir(c):
                    if not attr.startswith("_"):
                        try:
                            val = getattr(c, attr)
                            if not callable(val):
                                data[f"contract.{attr}"] = val
                        except Exception:
                            continue

            data["input_symbol"] = symbol
            data["market_type"] = market_type
            records.append(data)

        return records

    finally:
        if ib.isConnected():
            ib.disconnect()


# --- MAIN FUNCTION ---
async def main():
    all_records = []

    for entry in SYMBOLS:
        symbol, market_type = entry["symbol"], entry["type"]
        print(f"⏳ Fetching {symbol} ({market_type}) ...")

        try:
            recs = await fetch_raw(symbol, market_type)
            if recs:
                print(f"✅ {symbol}: {len(recs)} contract(s) found")
                all_records.extend(recs)
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")

    if not all_records:
        print("No data retrieved.")
        return

    df = pd.DataFrame(all_records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUT_PATH + f"raw_contract_dump_{timestamp}.csv"

    df.to_csv(out_file, index=False)
    print(f"\n💾 Full unprocessed data saved to: {out_file}")
    print(f"Total records: {len(df)} | Columns: {len(df.columns)}")


# --- EXECUTION ---
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        if not loop.is_closed():
            loop.close()
