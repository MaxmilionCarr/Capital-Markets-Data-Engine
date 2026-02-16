from __future__ import annotations

import sqlite3 as sql
from datetime import datetime, timedelta, time
from typing import Literal

import pandas as pd

from database.db import Hub
from database.repositories.instruments.ticker_repository import Equity

periods = {"5 mins", "1 hour", "1 day"}


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

class EquityPricesRepository:
    """
    Period-specific tables:
      - equity_prices_daily
      - equity_prices_hourly
      - equity_prices_five_minute

    Each table schema:
        equity_id INTEGER NOT NULL REFERENCES equities(equity_id) ON DELETE CASCADE,
        datetime DATETIME NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL NOT NULL,
        volume INTEGER,
        PRIMARY KEY (equity_id, datetime)
    """

    def __init__(self, connection: sql.Connection, hub: Hub):
        self.connection = connection
        self.hub = hub
        self.connection.execute("PRAGMA foreign_keys = ON")

    # ---------- READ ----------

    def _fetch_daily(self, equity: Equity, start_date: datetime, end_date: datetime | None = None) -> pd.DataFrame:
        """
        NOTE: you said you will store daily as date-only. This query treats stored
        values as dates using SQLite date(datetime).
        Returns column name 'date' for compatibility with your ensure logic.
        """
        cur = self.connection.cursor()
        print("Writing into equity_prices_daily")
        if end_date:
            cur.execute(
                """
                SELECT equity_id,
                       date(datetime) AS date,
                       open, high, low, close, volume
                FROM equity_prices_daily
                WHERE equity_id = ? AND date(datetime) BETWEEN ? AND ?
                ORDER BY date
                """,
                (equity._id, start_date, end_date),
            )
        else:
            cur.execute(
                """
                SELECT equity_id,
                       date(datetime) AS date,
                       open, high, low, close, volume
                FROM equity_prices_daily
                WHERE equity_id = ? AND date(datetime) >= ?
                ORDER BY date
                """,
                (equity._id, start_date),
            )
        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

    def _fetch_intraday_raw(
        self,
        equity: Equity,
        period: Literal["1 hour", "5 mins"],
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """
        You are already storing aligned bars; fetch raw range.
        Returns column name 'date' for compatibility with ensure logic.
        """
        cur = self.connection.cursor()
        table = "equity_prices_hourly" if period == "1 hour" else "equity_prices_five_minute"
        print("Writing into", table)
        if end_date:
            cur.execute(
                f"""
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM {table}
                WHERE equity_id = ? AND datetime BETWEEN ? AND ?
                ORDER BY datetime
                """,
                (equity._id, start_date, end_date),
            )
        else:
            cur.execute(
                f"""
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM {table}
                WHERE equity_id = ? AND datetime >= ?
                ORDER BY datetime
                """,
                (equity._id, start_date),
            )

        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

    def get_prices(
        self,
        equity: Equity,
        period: Literal["5 mins", "1 hour", "1 day"],
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        if period not in periods:
            raise ValueError("Period required")

        match period:
            case "1 day":
                prices = self._fetch_daily(equity, start_date, end_date)
            case "1 hour" | "5 mins":
                prices = self._fetch_intraday_raw(equity, period, start_date, end_date)
            case _:
                raise ValueError("Invalid period")

        return prices.drop(columns=["equity_id"]) if "equity_id" in prices.columns else prices

    # ---------- CREATE ----------

    def create(
        self,
        equity: Equity,
        period: str,
        datetime: datetime,
        close: float,
        *,
        open: float,
        high: float,
        low: float,
        volume: int,
    ) -> None:
        cur = self.connection.cursor()
        if period == "1 day":
            table = "equity_prices_daily"
        elif period == "1 hour":
            table = "equity_prices_hourly"
        elif period == "5 mins":
            table = "equity_prices_five_minute"
        else:
            raise ValueError("Invalid period")

        cur.execute(
            f"INSERT INTO {table} (equity_id, datetime, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (equity._id, datetime, open, high, low, close, volume),
        )
        self.connection.commit()

    # ---------- ENSURE ----------

    def get_or_create_ensure(
        self,
        equity: Equity,
        period: Literal["5 mins", "1 hour", "1 day"],
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """
        PRESERVES your original behavior:
          - compute patch windows from gaps
          - merge windows
          - intraday: clip to RTH per day then stitch to reduce calls
          - insert everything (PK dedupes)

        FIXES:
          - daily gap detection ignores weekends (no Sat->Mon patch spam)
          - daily uses date-based gap checks, not timedelta on naive datetimes
        """
        if end_date is None:
            end_date = datetime.now()

        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        def _period_delta(p: str) -> pd.Timedelta:
            match p:
                case "1 day":
                    return pd.Timedelta(days=1)
                case "1 hour":
                    return pd.Timedelta(hours=1)
                case "5 mins":
                    return pd.Timedelta(minutes=5)
                case _:
                    raise ValueError("Invalid period")

        def _to_naive(dt_series: pd.Series) -> pd.Series:
            return pd.to_datetime(dt_series, utc=False).dt.tz_localize(None)

        def _insert_all(df: pd.DataFrame) -> None:
            if df is None or df.empty:
                return

            table_by_period = {
                "1 day": "equity_prices_daily",
                "1 hour": "equity_prices_hourly",
                "5 mins": "equity_prices_five_minute",
            }
            table = table_by_period.get(period)
            if table is None:
                raise ValueError("Invalid period")

            # normalize datetimes
            df = df.copy()
            df["datetime"] = pd.to_datetime(df["datetime"], utc=False).dt.tz_localize(None)

            # daily: normalize to midnight so you truly store one row per date
            if period == "1 day":
                df["datetime"] = df["datetime"].dt.normalize()

            # Keep only needed cols and coerce types
            df = df[["datetime", "open", "high", "low", "close", "volume"]]
            df["volume"] = df["volume"].fillna(0).astype(int)

            # Convert to python-native tuples (fast)
            rows = [
                (equity._id,
                 r.datetime.to_pydatetime(),
                 float(r.open) if pd.notna(r.open) else None,
                 float(r.high) if pd.notna(r.high) else None,
                 float(r.low) if pd.notna(r.low) else None,
                 float(r.close),
                 int(r.volume))
                for r in df.itertuples(index=False)
            ]

            cur = self.connection.cursor()

            # Single transaction, bulk insert, ignore duplicates via PK
            cur.execute("BEGIN")
            try:
                cur.executemany(
                    f"""
                    INSERT OR IGNORE INTO {table}
                    (equity_id, datetime, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise


        def _fetch_and_insert(fetch_start: datetime, fetch_end: datetime) -> None:
            ex = equity._ticker.get_exchange()
            rth_open = _parse_hms(ex.rth_open) if ex and ex.rth_open else None
            rth_close = _parse_hms(ex.rth_close) if ex and ex.rth_close else None

            df = self.hub.market_data_service.fetch_equity_prices(
                equity._ticker.symbol,
                equity._ticker.get_exchange().name,
                start_date=fetch_start,
                end_date=fetch_end,
                bar_size=period,
                rth_open=rth_open,
                rth_close=rth_close,
            )
            _insert_all(df)

        def _clip_window_to_rth_per_day(
            s: datetime,
            e: datetime,
            rth_open: time,
            rth_close: time,
        ) -> list[tuple[datetime, datetime]]:
            if e <= s:
                return []

            out: list[tuple[datetime, datetime]] = []
            day = s.replace(hour=0, minute=0, second=0, microsecond=0)
            last = e.replace(hour=0, minute=0, second=0, microsecond=0)

            while day <= last:
                if day.weekday() >= 5:
                    day += timedelta(days=1)
                    continue

                d_open = day.replace(hour=rth_open.hour, minute=rth_open.minute, second=0, microsecond=0)
                d_close = day.replace(hour=rth_close.hour, minute=rth_close.minute, second=0, microsecond=0)

                ss = max(s, d_open)
                ee = min(e, d_close)

                if ss < ee:
                    out.append((ss, ee))

                day += timedelta(days=1)

            return out

        def _stitch_slices(
            slices: list[tuple[datetime, datetime]],
            *,
            rth_open: time,
            rth_close: time,
        ) -> list[tuple[datetime, datetime]]:
            if not slices:
                return []

            slices = sorted(slices, key=lambda x: x[0])
            out: list[tuple[datetime, datetime]] = []
            cur_s, cur_e = slices[0]

            for s, e in slices[1:]:
                if cur_e.time() == rth_close and s.time() == rth_open:
                    cur_e = e
                else:
                    out.append((cur_s, cur_e))
                    cur_s, cur_e = s, e

            out.append((cur_s, cur_e))
            return out

        def _business_days_between(a: datetime, b: datetime) -> int:
            """
            Count Mon-Fri days strictly between a and b (exclusive endpoints).
            Used only for daily period to avoid weekend patching.
            """
            if b <= a:
                return 0
            a0 = a.replace(hour=0, minute=0, second=0, microsecond=0)
            b0 = b.replace(hour=0, minute=0, second=0, microsecond=0)
            day = a0 + timedelta(days=1)
            cnt = 0
            while day < b0:
                if day.weekday() < 5:
                    cnt += 1
                day += timedelta(days=1)
            return cnt

        # --- 1) Read what we already have ---
        existing = self.get_prices(equity, period, start_date, end_date)

        if existing is None or existing.empty:
            _fetch_and_insert(start_date, end_date)
            return self.get_prices(equity, period, start_date, end_date)

        period_delta = _period_delta(period)

        fetch_windows: list[tuple[datetime, datetime]] = []

        # --- 2) Build fetch windows from gaps (DAILY vs INTRADAY) ---
        if period == "1 day":
            # existing["date"] are 'YYYY-MM-DD' strings
            have = pd.to_datetime(existing["date"], errors="coerce").dt.date
            have = set(d for d in have if pd.notna(d))

            def _expected_dates_daily_exclusive(s: datetime, e: datetime) -> list[datetime.date]:
                """
                Expected DAILY dates in [s, e) (end-exclusive).
                If e is at 00:00, we exclude that calendar date entirely.
                Weekend-aware only (Mon-Fri).
                """
                s_d = s.date()

                # end-exclusive: subtract 1 microsecond so 00:00 end becomes previous date
                e_adj = e - timedelta(microseconds=1)
                e_d = e_adj.date()

                out: list[datetime.date] = []
                cur = datetime(s_d.year, s_d.month, s_d.day)
                end_ = datetime(e_d.year, e_d.month, e_d.day)

                while cur <= end_:
                    if cur.weekday() < 5:
                        out.append(cur.date())
                    cur += timedelta(days=1)

                return out


            expected = _expected_dates_daily_exclusive(start_date, end_date)
            missing = sorted(d for d in expected if d not in have)

            if not missing:
                return existing

            # Group missing *dates* into contiguous date runs
            runs: list[tuple[datetime, datetime]] = []
            run_s = missing[0]
            prev = missing[0]

            for d in missing[1:]:
                if (d - prev).days == 1:
                    prev = d
                    continue

                # close run [run_s..prev] as [run_s 00:00, (prev+1) 00:00)
                runs.append(
                    (
                        datetime(run_s.year, run_s.month, run_s.day),
                        datetime(prev.year, prev.month, prev.day) + timedelta(days=1),
                    )
                )
                run_s = prev = d

            runs.append(
                (
                    datetime(run_s.year, run_s.month, run_s.day),
                    datetime(prev.year, prev.month, prev.day) + timedelta(days=1),
                )
            )

            fetch_windows.extend(runs)


        else:
            existing_dt = _to_naive(existing["date"])
            existing_dt = pd.Series(sorted(existing_dt.unique()))
            if existing_dt.empty:
                _fetch_and_insert(start_date, end_date)
                return self.get_prices(equity, period, start_date, end_date)

            # edges
            first = existing_dt.iloc[0].to_pydatetime()
            if first - start_date >= period_delta:
                fetch_windows.append((start_date, first))

            last = existing_dt.iloc[-1].to_pydatetime()
            if end_date - last >= period_delta:
                fetch_windows.append((last, end_date))

            # interior gaps
            for i in range(len(existing_dt) - 1):
                a = existing_dt.iloc[i].to_pydatetime()
                b = existing_dt.iloc[i + 1].to_pydatetime()
                if (b - a) >= (period_delta * 2):
                    fetch_windows.append((a + period_delta, b))

        if not fetch_windows:
            return existing

        # --- 3) Merge overlapping/adjacent windows to reduce IBKR calls ---
        fetch_windows.sort(key=lambda x: x[0])
        merged: list[tuple[datetime, datetime]] = []
        for s, e in fetch_windows:
            if not merged:
                merged.append((s, e))
                continue
            ps, pe = merged[-1]
            if s <= pe + period_delta:
                merged[-1] = (ps, max(pe, e))
            else:
                merged.append((s, e))

        # --- 4) Fetch + insert (preserve clip+stitch for intraday) ---
        ex = equity._ticker.get_exchange()
        rth_open = _parse_hms(ex.rth_open) if ex and getattr(ex, "rth_open", None) else time(9, 30)
        rth_close = _parse_hms(ex.rth_close) if ex and getattr(ex, "rth_close", None) else time(16, 0)

        for s, e in merged:
            if e <= s:
                continue

            if period == "1 day":
                print("PATCH FETCH:", s, e)
                _fetch_and_insert(s, e)
                continue

            slices = _clip_window_to_rth_per_day(s, e, rth_open, rth_close)
            if not slices:
                continue

            stitched = _stitch_slices(slices, rth_open=rth_open, rth_close=rth_close)
            for ss, ee in stitched:
                print("STITCHED FETCH:", ss, ee)
                _fetch_and_insert(ss, ee)

        return self.get_prices(equity, period, start_date, end_date)

    # ---------- UPDATE ----------

    def update(
        self,
        equity: Equity,
        period: str,
        datetime: datetime,
        *,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ) -> int:
        cur = self.connection.cursor()
        if period == "1 day":
            table = "equity_prices_daily"
        elif period == "1 hour":
            table = "equity_prices_hourly"
        elif period == "5 mins":
            table = "equity_prices_five_minute"
        else:
            raise ValueError("Invalid period")

        cur.execute(
            f"""
            UPDATE {table}
            SET open = ?, high = ?, low = ?, close = ?, volume = ?
            WHERE equity_id = ? AND datetime = ?
            """,
            (open, high, low, close, volume, equity._id, datetime),
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete_equity(self, equity_id: int, period: str) -> int:
        cur = self.connection.cursor()
        if period == "1 day":
            table = "equity_prices_daily"
        elif period == "1 hour":
            table = "equity_prices_hourly"
        elif period == "5 mins":
            table = "equity_prices_five_minute"
        else:
            raise ValueError("Invalid period")

        cur.execute(f"DELETE FROM {table} WHERE equity_id = ?", (equity_id,))
        self.connection.commit()
        return cur.rowcount

    def delete_range(self, equity_id: int, period: str, start_date: datetime, end_date: datetime) -> int:
        cur = self.connection.cursor()
        if period == "1 day":
            table = "equity_prices_daily"
        elif period == "1 hour":
            table = "equity_prices_hourly"
        elif period == "5 mins":
            table = "equity_prices_five_minute"
        else:
            raise ValueError("Invalid period")

        cur.execute(
            f"DELETE FROM {table} WHERE equity_id = ? AND datetime BETWEEN ? AND ?",
            (equity_id, start_date, end_date),
        )
        self.connection.commit()
        return cur.rowcount

    def delete_all(self, period: str) -> int:
        cur = self.connection.cursor()
        if period == "1 day":
            table = "equity_prices_daily"
        elif period == "1 hour":
            table = "equity_prices_hourly"
        elif period == "5 mins":
            table = "equity_prices_five_minute"
        else:
            raise ValueError("Invalid period")

        cur.execute(f"DELETE FROM {table}")
        self.connection.commit()
        return cur.rowcount
