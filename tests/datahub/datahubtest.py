from data_providers import DataHub, DataHubConfig, FMPService, FMPConfig, IBKRService, IBKRConfig
import os
from dotenv import load_dotenv

def test_datahub():
    fundamental_service = FMPService(FMPConfig(
        api_key = os.getenv("API_KEY")
    ))

    market_service = IBKRService(IBKRConfig())

    config = DataHubConfig(
        market_services = [market_service],
        fundamental_services = [fundamental_service]
    )

    hub = DataHub(config)

    response = hub.fundamentals.fetch_statement("AAPL", "income_statement", 1, "annual")
    print(response)

    market_service = hub.market

    response = market_service.fetch_equity("AAPL", "NASDAQ")
    print(response)

if __name__ == "__main__":
    test_datahub()