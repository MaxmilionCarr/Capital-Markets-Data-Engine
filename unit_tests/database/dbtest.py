from .repositories.database import basic_tests
from .repositories.exchanges import exchange_tests
from .repositories.markets import market_tests
from .repositories.tickers import ticker_tests
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run database unit tests.")
    parser.add_argument(
        "--test",
        type=str,
        choices=["basic", "exchanges", "markets", "tickers", "all"],
        default="all",
        help="Specify which tests to run: 'basic', 'exchanges', 'markets', 'tickers', or 'all'. Default is 'all'.",
    )
    args = parser.parse_args()

    if args.test == "basic":
        basic_tests()
    elif args.test == "exchanges":
        exchange_tests()
    elif args.test == "markets":
        market_tests()
    elif args.test == "tickers":
        ticker_tests()
    elif args.test == "all":
        basic_tests()
        exchange_tests()
        market_tests()
        ticker_tests()

if __name__ == "__main__":
    main()