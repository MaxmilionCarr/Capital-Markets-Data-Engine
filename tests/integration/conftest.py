from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from dotenv import load_dotenv

from data_providers import DataHubConfig
from data_providers.clients import FMPConfig, IBKRConfig
from data_providers.services.fundamental_data.FMP_service import FMPService
from data_providers.services.market_data.IBKR_service import IBKRService
from database_connector import DataBase, DB


# Load environment variables from .env file at the project root
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

SEED_DB_PATH = ROOT / "tests" / "test_data" / "testing.sqlite"


def _ensure_seed_database() -> Path:
    if SEED_DB_PATH.exists():
        return SEED_DB_PATH

    SEED_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    creator = DataBase(str(SEED_DB_PATH))
    creator.create_db()
    return SEED_DB_PATH


@pytest.fixture(scope="session")
def seed_sqlite_database_path() -> Path:
    return _ensure_seed_database()


@pytest.fixture()
def temp_sqlite_database_path(tmp_path: Path, seed_sqlite_database_path: Path) -> Path:
    # Default behavior keeps integration tests isolated by writing into a temp copy.
    # Set PERSIST_TEST_DB=1 to write directly into tests/test_data/testing.sqlite for inspection.
    persist = os.getenv("PERSIST_TEST_DB", "0").strip().lower() in {"1", "true", "yes", "on"}
    if persist:
        return seed_sqlite_database_path

    target = tmp_path / "testing.sqlite"
    shutil.copy2(seed_sqlite_database_path, target)
    return target


@pytest.fixture(scope="session")
def fmp_api_key() -> str:
    value = os.getenv("FMP_API_KEY")
    if not value:
        pytest.skip("FMP_API_KEY is not set")
    return value


@pytest.fixture(scope="session")
def fmp_service(fmp_api_key: str) -> FMPService:
    return FMPService(FMPConfig(api_key=fmp_api_key))


@pytest.fixture()
def live_db(temp_sqlite_database_path: Path, fmp_service: FMPService) -> DB:
    ibkr_service = IBKRService(IBKRConfig())
    cfg = DataHubConfig(
        basic_info_services=(fmp_service, ibkr_service),
        exchange_services=(ibkr_service,),
        fundamental_services=(fmp_service,),
        pricing_services=(ibkr_service,),
    )
    return DB(db_path=str(temp_sqlite_database_path), config=cfg)