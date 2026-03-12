from data_providers import DataHub, DataHubConfig, FMPService, FMPConfig, IBKRService, IBKRConfig
import os
from dotenv import load_dotenv

load_dotenv()

def test_datahub():
    fmp_service = FMPService(FMPConfig(
        api_key = os.getenv("FMP_API_KEY")
    ))

    ibkr_service = IBKRService(IBKRConfig(
        port = 60000
    ))

    config = DataHubConfig(
        market_services = [ibkr_service, fmp_service],
        fundamental_services = [fmp_service]
    )

    hub = DataHub(config)

    print("Fetching Statements")
    response = hub.fundamentals.fetch_statement("AAPL", "income_statement", 1, "annual")
    print(response)

    response = hub.market.fetch_issuer_enriched("AAPL", "NASDAQ")
    print(response)

if __name__ == "__main__":
    test_datahub()