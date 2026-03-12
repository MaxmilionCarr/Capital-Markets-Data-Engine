from ib_async import IB, Stock
from datetime import datetime, timedelta


HOST = "127.0.0.1"
PORT = 60000          # TWS paper default; use 4001 for IB Gateway paper if needed
CLIENT_ID = 1


def main():
    ib = IB()

    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
        print(f"Connected: {ib.isConnected()}")

        # Resolve AAPL stock contract first
        contract = Stock("AAPL", "SMART", "USD")
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise RuntimeError("Could not qualify AAPL contract.")
        contract = qualified[0]
        print(f"Qualified contract conId={contract.conId}")

        # 1) Check which API news providers are available to your account
        providers = ib.reqNewsProviders()
        print("\nNEWS PROVIDERS")
        print("-" * 80)

        if not providers:
            print("No API news providers returned.")
            print("You may need API-specific news subscriptions in IBKR Account Management.")
            return

        provider_codes = []
        for p in providers:
            code = getattr(p, "code", None) or getattr(p, "providerCode", None)
            name = getattr(p, "name", None) or getattr(p, "providerName", None)
            provider_codes.append(code)
            print(f"{code}: {name}")

        provider_codes = [p for p in provider_codes if p]
        if not provider_codes:
            print("No usable provider codes returned.")
            return

        # Provider codes must be joined with '+'
        provider_code_str = "+".join(provider_codes)

        # 2) Fetch historical headlines
        end = datetime.utcnow()
        start = end - timedelta(days=7)

        headlines = ib.reqHistoricalNews(
            contract.conId,
            provider_code_str,
            start.strftime("%Y-%m-%d %H:%M:%S.0"),
            end.strftime("%Y-%m-%d %H:%M:%S.0"),
            20,     # max number of headlines
            []
        )

        print("\nHEADLINES")
        print("-" * 80)

        if not headlines:
            print("No headlines returned.")
            print("Possible reasons:")
            print("  - no API news subscription")
            print("  - no recent cached headlines for AAPL")
            print("  - wrong provider codes")
            return

        # 3) Print headlines and fetch article text if possible
        for i, h in enumerate(headlines, start=1):
            time_str = getattr(h, "time", "")
            provider_code = getattr(h, "providerCode", "")
            article_id = getattr(h, "articleId", "")
            headline = getattr(h, "headline", "")

            print(f"\n[{i}] {time_str} | {provider_code} | articleId={article_id}")
            print(f"Headline: {headline}")

            # Pull article body
            try:
                article = ib.reqNewsArticle(provider_code, article_id, [])
                article_type = getattr(article, "articleType", None)
                article_text = getattr(article, "articleText", None)

                print("ARTICLE")
                print("-" * 40)
                print(f"articleType: {article_type}")
                if article_text:
                    print(article_text[:2000])   # truncate for readability
                else:
                    print("No articleText returned.")
            except Exception as e:
                print(f"Could not fetch article body: {e}")

    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    main()