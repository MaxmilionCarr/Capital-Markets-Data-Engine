from __future__ import annotations

import os
import tempfile
from datetime import datetime

import pandas as pd

from data_providers import DataHub, DataHubConfig
from data_providers.clients.base import EquityInfo, IssuerInfo, Provider
from database_connector import DB, DataBase


class MockBasicFMP:
	name = "mock_basic_fmp"

	def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
		return IssuerInfo(
			provider=Provider.FMP,
			symbol=symbol,
			exchange=exchange_name or "NASDAQ",
			currency="USD",
			full_name="Acme Corp",
			sec_type=None,
			timezone=None,
			rth_open=None,
			rth_close=None,
			cik="0000123456",
			lei=None,
		)

	def fetch_equity(
		self,
		symbol: str,
		exchange_name: str | None = None,
		currency: str | None = None,
	) -> EquityInfo:
		return EquityInfo(
			provider=Provider.FMP,
			symbol=symbol,
			full_name=f"{symbol} Common Stock",
			sector="Technology",
			industry="Software",
			dividend_yield=0.01,
			pe_ratio=22.0,
			eps=4.5,
			beta=1.1,
			market_cap=100_000_000,
			cik="0000123456",
			lei=None,
		)


class MockBasicIBKR:
	name = "mock_basic_ibkr"

	def fetch_issuer(self, symbol: str, exchange_name: str | None = None) -> IssuerInfo:
		return IssuerInfo(
			provider=Provider.IBKR,
			symbol=symbol,
			exchange=exchange_name or "NASDAQ",
			currency="USD",
			full_name="Acme Corp",
			sec_type="STK",
			timezone="America/New_York",
			rth_open="09:30:00",
			rth_close="16:00:00",
			cik=None,
			lei="5493001KJTIIGC8Y1R12",
		)

	def fetch_equity(
		self,
		symbol: str,
		exchange_name: str | None = None,
		currency: str | None = None,
	) -> EquityInfo:
		# IBKR has priority for full_name in EquityBuilder.
		return EquityInfo(
			provider=Provider.IBKR,
			symbol=symbol,
			full_name=f"{symbol} (IBKR Name)",
			sector=None,
			industry="Software Infrastructure",
			dividend_yield=None,
			pe_ratio=None,
			eps=None,
			beta=None,
			market_cap=None,
			cik=None,
			lei="5493001KJTIIGC8Y1R12",
		)


class MockPricingIBKR:
	name = "mock_pricing_ibkr"

	def fetch_equity_prices(
		self,
		symbol: str,
		exchange_name: str,
		start_date: datetime,
		end_date: datetime,
		bar_size: str,
		rth_open,
		rth_close,
	) -> pd.DataFrame:
		if bar_size != "1 day":
			raise ValueError("This mock only supports 1 day bars for this test")

		return pd.DataFrame(
			[
				{
					"datetime": datetime(2024, 1, 2, 0, 0, 0),
					"open": 100.0,
					"high": 110.0,
					"low": 95.0,
					"close": 108.0,
					"volume": 1_000_000,
				}
			]
		)


class MockFundamentalFMP:
	name = "mock_fundamental_fmp"

	def fetch_statement(self, symbol: str, statement_type: str, prev_years: int, period: str):
		# Keep output in the same format as live service adapters (list[dict]).
		return [{"date": "2024-12-31", "statement_type": statement_type, "revenue": 1234567}]


def _assert_scoped_hash_config() -> None:
	cfg = DataHubConfig(
		basic_info_services=(MockBasicFMP(), MockBasicIBKR()),
		pricing_services=(MockPricingIBKR(),),
		fundamental_services=(MockFundamentalFMP(),),
	)
	hub = DataHub(cfg)

	assert "basic_info" in hub.provider_identifiers
	assert "pricing" in hub.provider_identifiers
	assert "fundamental" in hub.provider_identifiers
	assert "all" in hub.provider_identifiers

	# Hashes are scoped and deterministic; pricing must be isolated from enrichment scope.
	assert hub.provider_identifiers["basic_info"] != hub.provider_identifiers["pricing"]

	# Backward compatibility alias should exist.
	assert hub.provider_identifiers["market"] == hub.provider_identifiers["basic_info"]

	# Pricing config must remain single-provider.
	try:
		DataHubConfig(
			basic_info_services=(MockBasicFMP(),),
			pricing_services=(MockPricingIBKR(), MockPricingIBKR()),
			fundamental_services=(MockFundamentalFMP(),),
		)
	except ValueError:
		pass
	else:
		raise AssertionError("Expected ValueError when pricing_services has more than one provider")


def _assert_db_provenance_end_to_end() -> None:
	fd, temp_path = tempfile.mkstemp(prefix="provider_hash_test_", suffix=".db")
	os.close(fd)

	try:
		creator = DataBase(temp_path)
		creator.create_db()

		cfg = DataHubConfig(
			basic_info_services=(MockBasicFMP(), MockBasicIBKR()),
			pricing_services=(MockPricingIBKR(),),
			fundamental_services=(MockFundamentalFMP(),),
		)
		db = DB(db_path=temp_path, config=cfg)

		basic_hash = db._hub.data_hub.provider_identifiers["basic_info"]
		pricing_hash = db._hub.data_hub.provider_identifiers["pricing"]
		fundamental_hash = db._hub.data_hub.provider_identifiers["fundamental"]

		equity = db.get_equity("AAPL", "NASDAQ", ensure=True)

		# Prices: ensure path should use the pricing-specific hash.
		_ = equity.get_prices(
			start_date=datetime(2024, 1, 2),
			end_date=datetime(2024, 1, 3, 16, 0, 0),
			period="1 day",
			ensure=True,
		)

		# Fundamentals: ensure path should upsert rows under fundamental hash.
		_ = equity.get_statements(
			statement_type="income_statement",
			period="annual",
			look_back=1,
			ensure=True,
		)

		cur = db._connection.cursor()

		# Exchange + issuer + equity should all be stamped with basic_info hash.
		cur.execute("SELECT provider_identifier FROM exchanges WHERE exchange_name = ?", ("NASDAQ",))
		assert cur.fetchone()[0] == basic_hash

		cur.execute("SELECT provider_identifier FROM issuers WHERE issuer_id = ?", (equity.issuer_id,))
		assert cur.fetchone()[0] == basic_hash

		cur.execute(
			"SELECT provider_identifier FROM equities WHERE equity_id = ?",
			(equity.equity_id,),
		)
		assert cur.fetchone()[0] == basic_hash

		# Pricing coverage should be stamped with pricing hash.
		cur.execute(
			"""
			SELECT provider
			FROM equity_intraday_coverage
			WHERE equity_id = ? AND period = ?
			LIMIT 1
			""",
			(equity.equity_id, "1 day"),
		)
		row = cur.fetchone()
		assert row is not None
		assert row[0] == pricing_hash

		# Statements should be stamped with fundamental hash.
		cur.execute(
			"""
			SELECT provider_identifier
			FROM statements
			WHERE issuer_id = ? AND type = ? AND period = ?
			LIMIT 1
			""",
			(equity.issuer_id, "income_statement", "annual"),
		)
		row = cur.fetchone()
		assert row is not None
		assert row[0] == fundamental_hash

		# Provider provenance table should contain scoped hashes.
		cur.execute("SELECT scope, provider_identifier FROM provider_provenance")
		print("Provider Provenance Table:")
		for scope, provider_identifier in cur.fetchall():
			print(f"  {scope}: {provider_identifier}")
		scopes = {scope: provider_identifier for scope, provider_identifier in cur.fetchall()}
		assert scopes["basic_info"] == basic_hash
		assert scopes["pricing"] == pricing_hash
		assert scopes["fundamental"] == fundamental_hash
	
		db.close()

	finally:
		if os.path.exists(temp_path):
			os.remove(temp_path)


def run_provider_hash_test() -> None:
	_assert_scoped_hash_config()
	_assert_db_provenance_end_to_end()
	print("provider_hash.py checks passed")


if __name__ == "__main__":
	run_provider_hash_test()
