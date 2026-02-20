from .repositories.database import database_tests
'''
from .repositories.exchanges import exchange_tests
from .repositories.tickers import ticker_tests
from .repositories.historicalprice import historical_price_tests
'''
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run database unit tests.")
    parser.add_argument(
        "--test",
        type=str,
        choices=["basic", "exchanges", "tickers", "historical_price", "all"],
        default="all",
        help="Specify which tests to run: 'basic', 'exchanges', 'tickers', 'historical_price', or 'all'. Default is 'all'.",
    )
    args = parser.parse_args()

    if args.test == "basic":
        database_tests()
    '''
    elif args.test == "exchanges":
        exchange_tests()
    elif args.test == "tickers":
        ticker_tests()
    elif args.test == "historical_price":
        historical_price_tests()
    '''
    if args.test == "all":
        database_tests()
        '''
        exchange_tests()
        ticker_tests()
        '''
        """
        historical_price_tests()
        """

if __name__ == "__main__":
    main()