import ib_async
from data_providers import IBKRConfig

config = IBKRConfig()

def main():
    ib = ib_async.IB()



    ib.connect(
        host=config.host,
        port=config.port,
        clientId=config.client_id,
        timeout=config.timeout
    )

    news_providers = ib.reqNewsProviders()
    print(news_providers)

    news = ib.reqHistoricalNews