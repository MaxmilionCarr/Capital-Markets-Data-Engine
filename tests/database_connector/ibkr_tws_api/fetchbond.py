from ib_async import IB, Bond, Contract
from pprint import pprint


HOST = "127.0.0.1"
PORT = 60000      # TWS paper default; use 4001 for IB Gateway paper if needed
CLIENT_ID = 1


def safe_get(obj, field):
    return getattr(obj, field, None)


def contract_to_dict(contract):
    # ib_async dataclasses expose .dict()
    try:
        return contract.dict()
    except Exception:
        return {k: getattr(contract, k) for k in dir(contract) if not k.startswith("_")}


def details_to_summary(cd):
    c = cd.contract

    return {
        # Core contract identity
        "conId": safe_get(c, "conId"),
        "symbol": safe_get(c, "symbol"),
        "localSymbol": safe_get(c, "localSymbol"),
        "secType": safe_get(c, "secType"),
        "exchange": safe_get(c, "exchange"),
        "primaryExchange": safe_get(c, "primaryExchange"),
        "currency": safe_get(c, "currency"),
        "tradingClass": safe_get(c, "tradingClass"),
        "secIdType": safe_get(c, "secIdType"),
        "secId": safe_get(c, "secId"),
        "issuerId": safe_get(c, "issuerId"),

        # Generic contract details
        "marketName": safe_get(cd, "marketName"),
        "longName": safe_get(cd, "longName"),
        "minTick": safe_get(cd, "minTick"),
        "validExchanges": safe_get(cd, "validExchanges"),
        "orderTypes": safe_get(cd, "orderTypes"),
        "timeZoneId": safe_get(cd, "timeZoneId"),
        "tradingHours": safe_get(cd, "tradingHours"),
        "liquidHours": safe_get(cd, "liquidHours"),
        "marketRuleIds": safe_get(cd, "marketRuleIds"),

        # Bond-specific metadata
        "cusip": safe_get(cd, "cusip"),
        "ratings": safe_get(cd, "ratings"),
        "descAppend": safe_get(cd, "descAppend"),
        "bondType": safe_get(cd, "bondType"),
        "couponType": safe_get(cd, "couponType"),
        "callable": safe_get(cd, "callable"),
        "putable": safe_get(cd, "putable"),
        "coupon": safe_get(cd, "coupon"),
        "convertible": safe_get(cd, "convertible"),
        "maturity": safe_get(cd, "maturity"),
        "issueDate": safe_get(cd, "issueDate"),
        "nextOptionDate": safe_get(cd, "nextOptionDate"),
        "nextOptionType": safe_get(cd, "nextOptionType"),
        "nextOptionPartial": safe_get(cd, "nextOptionPartial"),
        "notes": safe_get(cd, "notes"),
    }


def main():
    ib = IB()

    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
        print(f"Connected: {ib.isConnected()}")

        # Broad Apple bond search.
        # This is intentionally loose so we can see what IBKR returns.
        # You can tighten exchange/currency if needed.

        query = Bond(symbol="AAPL", exchange="SMART", currency="USD")

        details = ib.reqContractDetails(query)
        print(f"Found {len(details)} bond contract(s) for query.\n")

        if not details:
            print("No bonds returned.")
            print("Try one of these fallbacks:")
            print("  1) Remove exchange='SMART'")
            print("  2) Use exchange='ANY'")
            print("  3) Query by secIdType/secId (e.g. ISIN or CUSIP) if you know one")
            return

        for i, cd in enumerate(details, start=1):
            print("=" * 100)
            print(f"BOND #{i}")
            pprint(details_to_summary(cd), sort_dicts=False)

            print("\nFULL CONTRACT:")
            pprint(contract_to_dict(cd.contract), sort_dicts=False)

            print("\nFULL CONTRACT DETAILS:")
            try:
                pprint(cd.dict(), sort_dicts=False)
            except Exception:
                pprint(
                    {k: getattr(cd, k) for k in dir(cd) if not k.startswith("_")},
                    sort_dicts=False
                )
            print()

    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    main()