from __future__ import annotations

from database_connector.services.database_service import DatabaseService


class SchemaService:
    def __init__(self, database_service: DatabaseService):
        self.db = database_service

    def create_schema(self) -> None:
        cur = self.db.connection.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS provider_provenance (
                        provider_identifier TEXT PRIMARY KEY,
                        scope TEXT NOT NULL,
                        providers TEXT NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS exchanges (
                        exchange_id INTEGER PRIMARY KEY,
                        exchange_name TEXT NOT NULL UNIQUE,
                        timezone TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        rth_open TEXT NOT NULL,
                        rth_close TEXT NOT NULL,
                        provider_identifier TEXT NOT NULL
                    );''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_exchanges_name ON exchanges (exchange_name)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS issuers (
                        issuer_id INTEGER PRIMARY KEY,
                        full_name TEXT,
                        cik TEXT UNIQUE,
                        lei TEXT UNIQUE,
                        provider_identifier TEXT NOT NULL
                    );''')

        cur.execute('''CREATE INDEX IF NOT EXISTS idx_issuers_cik ON issuers(cik);''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_issuers_lei ON issuers(lei);''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equities (
                        equity_id INTEGER PRIMARY KEY,
                        issuer_id INTEGER NOT NULL,
                        exchange_id INTEGER NOT NULL,

                        symbol TEXT NOT NULL,
                        full_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        provider_identifier TEXT NOT NULL,

                        FOREIGN KEY (issuer_id) REFERENCES issuers(issuer_id) ON DELETE CASCADE,
                        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id) ON DELETE CASCADE
                    );''')

        cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS uq_equities_exchange_symbol
                        ON equities(exchange_id, symbol);
                    ''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_intraday_coverage (
                        equity_id INTEGER NOT NULL,
                        date      DATE NOT NULL,
                        period    TEXT NOT NULL,
                        status    TEXT NOT NULL,
                        provider  TEXT NOT NULL,
                        rows      INTEGER NOT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (equity_id, date, period, provider)
                    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_daily_provider (
                        equity_id INTEGER NOT NULL,
                        provider_identifier TEXT NOT NULL,
                        last_updated DATETIME NOT NULL
                    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_daily (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_hourly_provider (
                        equity_id INTEGER NOT NULL,
                        provider_identifier TEXT NOT NULL,
                        last_updated DATETIME NOT NULL
                    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_hourly (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_five_minute_provider (
                        equity_id INTEGER NOT NULL,
                        provider_identifier TEXT NOT NULL,
                        last_updated DATETIME NOT NULL
                    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS equity_prices_five_minute (
                        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
                        datetime DATETIME NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        PRIMARY KEY (equity_id, datetime)
                    )''')

        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_daily ON equity_prices_daily (equity_id, datetime)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_hourly ON equity_prices_hourly (equity_id, datetime)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_prices_equity_time_five_minute ON equity_prices_five_minute (equity_id, datetime)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS statements (
                    id INTEGER PRIMARY KEY,
                    issuer_id INTEGER NOT NULL REFERENCES issuers(issuer_id) ON DELETE CASCADE,
                    type TEXT NOT NULL,
                    period TEXT NOT NULL,
                    fiscal_date DATETIME NOT NULL,
                    provider_identifier TEXT NOT NULL,
                    statement JSON NOT NULL,
                    UNIQUE(issuer_id, type, period, fiscal_date, provider_identifier)
                    )''')

        self.db.commit()
