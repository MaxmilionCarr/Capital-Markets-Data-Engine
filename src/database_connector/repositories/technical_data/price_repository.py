from __future__ import annotations

import sqlite3 as sql
from datetime import datetime, timedelta, time, date
import logging
from typing import Literal

import pandas as pd

from database_connector.db import Hub
from database_connector.repositories.securities.equities_repository import Equity

logger = logging.getLogger(__name__)


# -------------------------------------------------
# Constants
# -------------------------------------------------

periods = {"5 mins", "1 hour", "1 day"}
COVERED_STATUSES = {"ok", "partial", "closed"}


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _parse_hms(s: str) -> time:
    h, m, sec = map(int, s.split(":"))
    return time(h, m, sec)


def _period_step(period: str) -> timedelta:
    if period == "1 day":
        return timedelta(days=1)
    if period == "1 hour":
        return timedelta(hours=1)
    if period == "5 mins":
        return timedelta(minutes=5)
    raise ValueError("Invalid period")


def _day_is_weekday(d: date) -> bool:
    return d.weekday() < 5


def _get_exchange_rth(equity: Equity) -> tuple[time, time]:
    """
    Pull RTH bounds from your Exchange model (via equity.exchange / exchange_id).
    Fallback only if Exchange is missing/blank.
    """
    ex = getattr(equity, "exchange", None)
    if callable(ex):
        ex = ex()

    if ex is None:
        # fallback: try via hub if your Equity has _hub + exchange_id
        hub = equity._hub
        ex_id = getattr(equity, "exchange_id", None)
        if hub is not None and ex_id is not None:
            ex = hub.exchange_repo.get_info(exchange_id=ex_id)

    if ex and getattr(ex, "rth_open", None) and getattr(ex, "rth_close", None):
        return _parse_hms(ex.rth_open), _parse_hms(ex.rth_close)

    return time(9, 30), time(16, 0)


def _exchange_name_from_equity(equity: Equity) -> str:
    ex = getattr(equity, "exchange", None)
    if callable(ex):
        ex = ex()
    if ex is None:
        hub = equity._hub
        ex_id = getattr(equity, "exchange_id", None)
        if hub is not None and ex_id is not None:
            ex = hub.exchange_repo.get_info(exchange_id=ex_id)
    if not ex:
        return "SMART"
    return getattr(ex, "name", None) or getattr(ex, "exchange_name", None) or "SMART"


def _sql_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return value


# -------------------------------------------------
# Repository
# -------------------------------------------------

class EquityPricesRepository:

    # =================================================
    # INIT
    # =================================================

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub

    # =================================================
    # READ
    # =================================================

    def _fetch_daily(self, equity: Equity, start, end):
        cur = self.connection.cursor()
        start = _sql_value(start)
        end = _sql_value(end) if end else None

        if end:
            cur.execute(
                """
                SELECT date(datetime) AS date,
                       open, high, low, close, volume
                FROM equity_prices_daily
                WHERE equity_id = ?
                  AND date(datetime) BETWEEN ? AND ?
                ORDER BY date
                """,
                (equity.equity_id, start, end),
            )
        else:
            cur.execute(
                """
                SELECT date(datetime) AS date,
                       open, high, low, close, volume
                FROM equity_prices_daily
                WHERE equity_id = ?
                  AND date(datetime) >= ?
                ORDER BY date
                """,
                (equity.equity_id, start),
            )

        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])

    def _fetch_intraday_raw(self, equity: Equity, period, start, end):
        table = "equity_prices_hourly" if period == "1 hour" else "equity_prices_five_minute"
        cur = self.connection.cursor()
        start = _sql_value(start)
        end = _sql_value(end) if end else None

        if end:
            cur.execute(
                f"""
                SELECT datetime AS date,
                       open, high, low, close, volume
                FROM {table}
                WHERE equity_id = ?
                  AND datetime BETWEEN ? AND ?
                ORDER BY datetime
                """,
                (equity.equity_id, start, end),
            )
        else:
            cur.execute(
                f"""
                SELECT datetime AS date,
                       open, high, low, close, volume
                FROM {table}
                WHERE equity_id = ?
                  AND datetime >= ?
                ORDER BY datetime
                """,
                (equity.equity_id, start),
            )

        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])

    def get_prices(self, equity, period, start, end=None):
        if period == "1 day":
            return self._fetch_daily(equity, start, end)
        return self._fetch_intraday_raw(equity, period, start, end)

    # =================================================
    # COVERAGE
    # =================================================

    def _get_coverage_status(self, equity: Equity, d: date, period: str, provider: str) -> str | None:
        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT status
            FROM equity_intraday_coverage
            WHERE equity_id = ?
              AND date = ?
              AND period = ?
              AND provider = ?
            """,
            (equity.equity_id, d, period, provider),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _is_day_covered(self, equity: Equity, d: date, period: str, provider: str) -> bool:
        return self._get_coverage_status(equity, d, period, provider) in COVERED_STATUSES

    # =================================================
    # COVERAGE UPDATE
    # =================================================

    def update_intraday_coverage(
        self,
        *,
        equity: Equity,
        df: pd.DataFrame | None,
        period: str,
        provider: str,
        start: datetime,
        end: datetime,
    ) -> None:
        cur = self.connection.cursor()

        if df is None:
            df = pd.DataFrame()

        df = df.copy()

        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["day"] = df["datetime"].dt.date
            grouped = df.groupby("day")
        else:
            grouped = {}

        if period == "1 hour":
            expected = 6
            ok_ratio = 0.80
        elif period == "5 mins":
            expected = 78
            ok_ratio = 0.97
        elif period == "1 day":
            expected = 1
            ok_ratio = 1.0
        else:
            return

        cur_day = start.date()
        last = end.date()

        while cur_day <= last:
            if _day_is_weekday(cur_day):
                if cur_day in grouped:
                    rows = len(grouped.get_group(cur_day))
                    status = "ok" if rows >= expected * ok_ratio else "partial"
                else:
                    rows = 0
                    status = "closed"

                cur.execute(
                    """
                    INSERT INTO equity_intraday_coverage
                        (equity_id, date, period, status, provider, rows)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(equity_id, date, period, provider)
                    DO UPDATE SET
                        status = excluded.status,
                        rows = excluded.rows,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (equity.equity_id, _sql_value(cur_day), period, status, provider, rows),
                )

            cur_day += timedelta(days=1)

        self.connection.commit()

    def update_intraday_coverage_failed(
        self,
        *,
        equity: Equity,
        period: str,
        provider: str,
        start: datetime,
        end: datetime,
    ) -> None:
        cur = self.connection.cursor()

        cur_day = start.date()
        last = end.date()

        while cur_day <= last:
            if _day_is_weekday(cur_day):
                rows = 0
                status = "failed"

                cur.execute(
                    """
                    INSERT INTO equity_intraday_coverage
                        (equity_id, date, period, status, provider, rows)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(equity_id, date, period, provider)
                    DO UPDATE SET
                        status = excluded.status,
                        rows = excluded.rows,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (equity.equity_id, _sql_value(cur_day), period, status, provider, rows),
                )

            cur_day += timedelta(days=1)

        self.connection.commit()

    # =================================================
    # INSERT
    # =================================================

    def _insert_all(self, equity: Equity, period: str, df: pd.DataFrame):
        if df is None or df.empty:
            return

        table = {
            "1 day": "equity_prices_daily",
            "1 hour": "equity_prices_hourly",
            "5 mins": "equity_prices_five_minute",
        }[period]

        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)

        if period == "1 day":
            df["datetime"] = df["datetime"].dt.normalize()

        rows = [
            (
                equity.equity_id,
                _sql_value(r.datetime.to_pydatetime()),
                r.open,
                r.high,
                r.low,
                r.close,
                int(r.volume),
            )
            for r in df.itertuples(index=False)
        ]

        insert_sql = self.hub.db_service.build_insert_ignore(
            table=table,
            columns=("equity_id", "datetime", "open", "high", "low", "close", "volume"),
        )

        with self.hub.db_service.transaction():
            self.hub.db_service.executemany(insert_sql, rows)

    # =================================================
    # ENSURE
    # =================================================

    # NEED CURRENCY HERE TODO
    # TODO: When a ticker returns error/timesout, assume IPO/missing data. 
    def get_or_create_ensure(
        self,
        equity: Equity,
        period: Literal["5 mins", "1 hour", "1 day"],
        start_date: datetime,
        end_date: datetime | None = None,
        *,
        provider: str | None = None,
    ) -> pd.DataFrame:
        if provider is None:
            provider = self.hub.data_hub.provider_identifiers["pricing"]

        if end_date is None:
            end_date = datetime.now()

        if end_date <= start_date:
            raise ValueError("Invalid date range")

        rth_open, rth_close = _get_exchange_rth(equity)
        exchange_name = _exchange_name_from_equity(equity)

        existing = self.get_prices(equity, period, start_date, end_date)

        # -------- First fill --------
        if existing.empty:
            logger.info(
                "No existing %s data for %s between %s and %s; fetching.",
                period,
                equity.symbol,
                start_date,
                end_date,
            )
            try:
                df = self.hub.pricing_data_service.fetch_equity_prices(
                    equity.symbol,
                    exchange_name,
                    start_date,
                    end_date,
                    bar_size=period,
                    rth_open=rth_open,
                    rth_close=rth_close,
                )
            except Exception as e:
                logger.warning(
                    "Error fetching prices for %s from %s to %s: %s",
                    equity.symbol,
                    start_date,
                    end_date,
                    e,
                )
                self.update_intraday_coverage_failed(
                    equity=equity,
                    period=period,
                    provider=provider,
                    start=start_date,
                    end=end_date,
                )
                return self.get_prices(equity, period, start_date, end_date)
            else:
                self._insert_all(equity, period, df)
                self.update_intraday_coverage(
                    equity=equity,
                    df=df,
                    period=period,
                    provider=provider,
                    start=start_date,
                    end=end_date,
                )
                return self.get_prices(equity, period, start_date, end_date)

        # -------- Find missing weekdays (but don’t refetch if coverage says closed/partial/ok) --------
        existing["dt"] = pd.to_datetime(existing["date"])
        have_days = set(existing["dt"].dt.date)

        missing: list[date] = []
        cur = start_date.date()
        last = end_date.date()

        while cur <= last:
            if _day_is_weekday(cur):
                if cur not in have_days:
                    if not self._is_day_covered(equity, cur, period, provider):
                        missing.append(cur)
            cur += timedelta(days=1)

        if not missing:
            return existing.drop(columns=["dt"])

        missing = sorted(missing)

        # -------- Group windows, bridging weekends so you don’t split Fri->Mon --------
        windows: list[tuple[datetime, datetime]] = []

        run_s = missing[0]
        prev = missing[0]

        def _same_run(prev_d: date, cur_d: date) -> bool:
            gap = (cur_d - prev_d).days
            if gap == 1:
                return True
            if gap <= 3:  # bridge weekend Fri->Mon
                return True
            return False

        for d in missing[1:]:
            if _same_run(prev, d):
                prev = d
                continue

            windows.append(
                (datetime.combine(run_s, rth_open), datetime.combine(prev, rth_close))
            )
            run_s = prev = d

        windows.append(
            (datetime.combine(run_s, rth_open), datetime.combine(prev, rth_close))
        )

        # -------- Fetch those bigger windows --------
        for s, e in windows:
            logger.info("Fetching missing window for %s %s from %s to %s", equity.symbol, period, s, e)
            try:
                df = self.hub.pricing_data_service.fetch_equity_prices(
                    equity.symbol,
                    exchange_name,
                    start_date=s,
                    end_date=e,
                    bar_size=period,
                    rth_open=rth_open,
                    rth_close=rth_close,
                )
            except Exception as err:
                logger.warning("Error fetching prices for %s from %s to %s: %s", equity.symbol, s, e, err)
                self.update_intraday_coverage_failed(
                    equity=equity,
                    period=period,
                    provider=provider,
                    start=s,
                    end=e,
                )
                continue
            else:
                self._insert_all(equity, period, df)
                self.update_intraday_coverage(
                    equity=equity,
                    df=df,
                    period=period,
                    provider=provider,
                    start=s,
                    end=e,
                )

        return self.get_prices(equity, period, start_date, end_date)