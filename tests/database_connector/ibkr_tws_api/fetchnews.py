from ib_async import IB, Stock
from datetime import datetime, timedelta
import json
from pathlib import Path


HOST = "127.0.0.1"
PORT = 60000
CLIENT_ID = 1

OUTPUT_FILE = "aapl_headlines.json"


def safe_get(obj, attr, default=None):
    return getattr(obj, attr, default)


def main():
    ib = IB()
    all_headlines = []

    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
        print(f"Connected: {ib.isConnected()}")

        contract = Stock("AAPL", "SMART", "USD")
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            raise RuntimeError("Could not qualify AAPL contract.")

        contract = qualified[0]
        print(f"Qualified contract conId={contract.conId}")

        providers = ib.reqNewsProviders()
        print("\nNEWS PROVIDERS")
        print("-" * 80)

        if not providers:
            print("No API news providers returned.")
            print("You may need API-specific news subscriptions in IBKR Account Management.")
            return

        provider_codes = []
        for p in providers:
            code = safe_get(p, "code") or safe_get(p, "providerCode")
            name = safe_get(p, "name") or safe_get(p, "providerName")
            if code:
                provider_codes.append(code)
            print(f"{code}: {name}")

        if not provider_codes:
            print("No usable provider codes returned.")
            return

        provider_code_str = "+".join(provider_codes)

        end = datetime.utcnow()
        start = end - timedelta(days=7)

        headlines = ib.reqHistoricalNews(
            contract.conId,
            provider_code_str,
            start.strftime("%Y-%m-%d %H:%M:%S.0"),
            end.strftime("%Y-%m-%d %H:%M:%S.0"),
            300,
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

        for i, h in enumerate(headlines, start=1):
            time_str = safe_get(h, "time", "")
            provider_code = safe_get(h, "providerCode", "")
            article_id = safe_get(h, "articleId", "")
            headline = safe_get(h, "headline", "")

            print(f"\n[{i}] {time_str} | {provider_code} | articleId={article_id}")
            print(f"Headline: {headline}")

            article_type = None
            article_text = None
            article_error = None

            try:
                article = ib.reqNewsArticle(provider_code, article_id, [])
                article_type = safe_get(article, "articleType")
                article_text = safe_get(article, "articleText")

                print("ARTICLE")
                print("-" * 40)
                print(f"articleType: {article_type}")
                if article_text:
                    print(article_text[:2000])
                else:
                    print("No articleText returned.")

            except Exception as e:
                article_error = str(e)
                print(f"Could not fetch article body: {e}")

            all_headlines.append({
                "symbol": contract.symbol,
                "conId": contract.conId,
                "time": time_str,
                "provider_code": provider_code,
                "article_id": article_id,
                "headline": headline,
                "article_type": article_type,
                "article_text": article_text,
                "article_error": article_error,
            })

        output = {
            "symbol": contract.symbol,
            "conId": contract.conId,
            "providers_used": provider_codes,
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "headline_count": len(all_headlines),
            "headlines": all_headlines,
        }

        out_path = Path(OUTPUT_FILE)
        out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

        print("\nSaved headlines to:", out_path.resolve())

    finally:
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    main()