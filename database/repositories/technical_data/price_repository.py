from __future__ import annotations

import sqlite3 as sql
from datetime import datetime, timedelta, time
from typing import Optional, Literal

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


def _expected_grid_for_day(day: datetime, period: str, rth_open: time, rth_close: time) -> list[datetime]:
    """
    Returns expected bar timestamps for a given day (exchange-local).
    For 1 hour: open=09:30 => 09:30,10:30,...,15:30
    For 5 mins: open=09:30 => 09:30,09:35,...,15:55
    """
    if period == "1 day":
        return []

    day0 = day.replace(hour=0, minute=0, second=0, microsecond=0)
    start = day0.replace(hour=rth_open.hour, minute=rth_open.minute, second=0, microsecond=0)
    end = day0.replace(hour=rth_close.hour, minute=rth_close.minute, second=0, microsecond=0)

    step = _period_step(period)

    out: list[datetime] = []
    t = start
    while t < end:
        out.append(t)
        t += step
    return out


class EquityPricesRepository:
    """
    Data-access layer for the `equity_prices` table.

    Schema:
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

    def get_all(self) -> pd.DataFrame:
        cur = self.connection.cursor()
        cur.execute("SELECT * FROM equity_prices")
        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description]).drop(columns=["equity_id"])

    def _fetch_daily(self, equity: Equity, start_date: datetime, end_date: datetime | None = None) -> pd.DataFrame:
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                """
                SELECT equity_id,
                       date(datetime) AS date,
                       FIRST_VALUE(open) OVER (PARTITION BY date(datetime) ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY date(datetime) ORDER BY datetime
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM equity_prices
                WHERE equity_id = ? AND datetime BETWEEN ? AND ?
                GROUP BY date
                ORDER BY date
                """,
                (equity._id, start_date, end_date),
            )
        else:
            cur.execute(
                """
                SELECT equity_id,
                       date(datetime) AS date,
                       FIRST_VALUE(open) OVER (PARTITION BY date(datetime) ORDER BY datetime) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       LAST_VALUE(close) OVER (PARTITION BY date(datetime) ORDER BY datetime
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                       SUM(volume) AS volume
                FROM equity_prices
                WHERE equity_id = ? AND datetime >= ?
                GROUP BY date
                ORDER BY date
                """,
                (equity._id, start_date),
            )
        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

    def _fetch_hourly(self, equity: Equity, start_date: datetime, end_date: datetime | None = None) -> pd.DataFrame:
        """
        Return stored hourly bars that match the exchange anchor minute (e.g. 09:30 => minute=30).
        """
        cur = self.connection.cursor()
        ex = equity._ticker.get_exchange()
        anchor_minute = int(ex.rth_open.split(":")[1])

        if end_date:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ?
                  AND datetime BETWEEN ? AND ?
                  AND CAST(strftime('%M', datetime) AS INTEGER) = ?
                  AND CAST(strftime('%S', datetime) AS INTEGER) = 0
                ORDER BY datetime
                """,
                (equity._id, start_date, end_date, anchor_minute),
            )
        else:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ?
                  AND datetime >= ?
                  AND CAST(strftime('%M', datetime) AS INTEGER) = ?
                  AND CAST(strftime('%S', datetime) AS INTEGER) = 0
                ORDER BY datetime
                """,
                (equity._id, start_date, anchor_minute),
            )

        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

    def _fetch_five_minute(self, equity: Equity, start_date: datetime, end_date: datetime | None = None) -> pd.DataFrame:
        """
        If you are storing IBKR 5-min bars directly, just filter to aligned minutes.
        Anchor uses exchange open minute (e.g. 09:30 => minute%5==0 is fine, but we also enforce seconds=0).
        """
        cur = self.connection.cursor()

        if end_date:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ?
                  AND datetime BETWEEN ? AND ?
                  AND (CAST(strftime('%M', datetime) AS INTEGER) % 5) = 0
                  AND CAST(strftime('%S', datetime) AS INTEGER) = 0
                ORDER BY datetime
                """,
                (equity._id, start_date, end_date),
            )
        else:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ?
                  AND datetime >= ?
                  AND (CAST(strftime('%M', datetime) AS INTEGER) % 5) = 0
                  AND CAST(strftime('%S', datetime) AS INTEGER) = 0
                ORDER BY datetime
                """,
                (equity._id, start_date),
            )

        return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    
    def _period_timedelta(self, period: str) -> pd.Timedelta:
        match period:
            case "1 day":   return pd.Timedelta(days=1)
            case "1 hour":  return pd.Timedelta(hours=1)
            case "5 mins":  return pd.Timedelta(minutes=5)
            case _: raise ValueError("Invalid period")

    def _align_filter(self, dts: pd.Series, start_date: datetime, period: str) -> pd.Series:
        """
        Return boolean mask where dts are aligned to the period grid anchored at start_date.
        Assumes naive exchange-local datetimes.
        """
        dts = pd.to_datetime(dts, utc=False).dt.tz_localize(None)

        # anchor everything to the same day baseline for stable modulo arithmetic
        # We only care about time-of-day alignment, so we compute minutes since midnight.
        start_m = start_date.hour * 60 + start_date.minute
        start_s = start_date.second

        mins = dts.dt.hour * 60 + dts.dt.minute
        secs = dts.dt.second

        if period == "1 hour":
            step = 60
        elif period == "5 mins":
            step = 5
        else:
            # daily handled elsewhere
            raise ValueError("intraday only")

        return ((mins - start_m) % step == 0) & (secs == start_s)

    def _fetch_intraday_aligned(
        self,
        equity: Equity,
        period: Literal["1 hour", "5 mins"],
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        cur = self.connection.cursor()
        if end_date:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ? AND datetime BETWEEN ? AND ?
                ORDER BY datetime
                """,
                (equity._id, start_date, end_date),
            )
        else:
            cur.execute(
                """
                SELECT equity_id, datetime AS date, open, high, low, close, volume
                FROM equity_prices
                WHERE equity_id = ? AND datetime >= ?
                ORDER BY datetime
                """,
                (equity._id, start_date),
            )

        df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)

        mask = self._align_filter(df["date"], start_date, period)
        return df.loc[mask]

    def get_prices(self, equity: Equity, period: Literal["5 mins", "1 hour", "1 day"],
                start_date: datetime, end_date: datetime | None = None) -> pd.DataFrame:
        if period not in periods:
            raise ValueError("Period required")

        match period:
            case "1 day":
                prices = self._fetch_daily(equity, start_date, end_date)
            case "1 hour" | "5 mins":
                prices = self._fetch_intraday_aligned(equity, period, start_date, end_date)
            case _:
                raise ValueError("Invalid period")

        return prices.drop(columns=["equity_id"]) if "equity_id" in prices.columns else prices


    # ---------- CREATE ----------

    def create(self, equity: Equity, datetime: datetime, close: float, *, open: float, high: float, low: float, volume: int) -> int:
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO equity_prices (equity_id, datetime, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (equity._id, datetime, open, high, low, close, volume),
        )
        self.connection.commit()
        return int(cur.lastrowid)

    # ---------- ENSURE ----------

    def get_or_create_ensure(
        self,
        equity: Equity,
        period: Literal["5 mins", "1 hour", "1 day"],
        start_date: datetime,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Ensure price data exists in DB for [start_date, end_date] at `period`.

        Design goals (per your requirements):
        - Avoid overfetching when possible (rate limits)
        - If we do overfetch (IBKR session quirks / trading-hours snapping), INSERT EVERYTHING anyway
        - For intraday ("1 hour", "5 mins"), do NOT try to "fill" calendar-day gaps like 00:00->09:30
            (that's what was causing pointless fetches)
        - Use DB PK (equity_id, datetime) to dedupe inserts
        """
        if end_date is None:
            end_date = datetime.now()

        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        # --- helpers ---
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
            # your system expects exchange-local naive datetimes
            return pd.to_datetime(dt_series, utc=False).dt.tz_localize(None)

        def _insert_all(df: pd.DataFrame) -> None:
            if df is None or df.empty:
                return

            # Normalize datetime
            df = df.copy()
            df["datetime"] = _to_naive(df["datetime"])

            # Insert everything; rely on PK to ignore duplicates
            for _, row in df.iterrows():
                dt = row["datetime"].to_pydatetime()
                try:
                    self.create(
                        equity,
                        dt,
                        close=float(row["close"]),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        volume=int(row["volume"]) if pd.notna(row["volume"]) else 0,
                    )
                except sql.IntegrityError:
                    pass

        def _fetch_and_insert(fetch_start: datetime, fetch_end: datetime) -> None:
            ex = equity._ticker.get_exchange()
            rth_open = _parse_hms(ex.rth_open) if ex and ex.rth_open else None
            rth_close = _parse_hms(ex.rth_close) if ex and ex.rth_close else None

            df = self.hub.service.fetch_equity_prices(
                equity._ticker.symbol,
                equity._ticker.get_exchange().name,
                start_date=fetch_start,
                end_date=fetch_end,
                bar_size=period,
                rth_open=rth_open,
                rth_close=rth_close
            )
            _insert_all(df)
            
        def _clip_window_to_rth_per_day(
            s: datetime,
            e: datetime,
            rth_open: time,
            rth_close: time,
        ) -> list[tuple[datetime, datetime]]:
            """
            Returns 0+ sub-windows of [s,e] clipped to RTH for each day.
            If a day is fully outside RTH, it yields nothing for that day.
            """
            if e <= s:
                return []

            out: list[tuple[datetime, datetime]] = []
            day = s.replace(hour=0, minute=0, second=0, microsecond=0)
            last = e.replace(hour=0, minute=0, second=0, microsecond=0)

            while day <= last:
                if day.weekday() >= 5:  # skip weekends
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
        
        def _stitch_slices(slices: list[tuple[datetime, datetime]], *, rth_open: time, rth_close: time) -> list[tuple[datetime, datetime]]:
            """
            Stitch per-day RTH slices into bigger windows when they are "adjacent" across days:
            - if previous ends exactly at rth_close AND next starts exactly at rth_open of a later day,
                treat as continuous RTH block and stitch.
            """
            if not slices:
                return []

            slices = sorted(slices, key=lambda x: x[0])
            out: list[tuple[datetime, datetime]] = []
            cur_s, cur_e = slices[0]

            for s, e in slices[1:]:
                # if cur ends at close AND next begins at open, stitch
                if cur_e.time() == rth_close and s.time() == rth_open:
                    # allow gap of >= 0 days (overnight/weekend). We'll stitch anyway.
                    cur_e = e
                else:
                    out.append((cur_s, cur_e))
                    cur_s, cur_e = s, e

            out.append((cur_s, cur_e))
            return out


        # --- 1) Read what we already have ---
        existing = self.get_prices(equity, period, start_date, end_date)

        # If nothing exists, fetch exactly what the user asked for once.
        if existing is None or existing.empty:
            _fetch_and_insert(start_date, end_date)
            return self.get_prices(equity, period, start_date, end_date)

        # --- 2) For intraday, only patch true gaps between existing bars ---
        # Key change: we NEVER create "00:00 -> market open" fetch windows.
        # We only look at gaps between bars that already exist in the DB
        # and fetch the missing interior segments, plus optional edges.
        period_delta = _period_delta(period)

        # Your get_prices returns column "date" for aggregated periods
        # Ensure it's a consistent datetime series
        existing_dt = _to_naive(existing["date"])

        # De-dupe + sort
        existing_dt = pd.Series(sorted(existing_dt.unique()))

        fetch_windows: list[tuple[datetime, datetime]] = []

        # --- Edge patching (minimal, avoids big overfetch) ---
        # If the first stored bar is after start_date by >= 1 period, fetch the left edge.
        first = existing_dt.iloc[0].to_pydatetime()
        if first - start_date >= period_delta:
            fetch_windows.append((start_date, first))

        # If the last stored bar is before end_date by >= 1 period, fetch the right edge.
        last = existing_dt.iloc[-1].to_pydatetime()
        if end_date - last >= period_delta:
            fetch_windows.append((last, end_date))

        # --- Interior gaps ---
        # Only fetch if there is a gap >= 2 periods (meaning at least one bar missing).
        for i in range(len(existing_dt) - 1):
            a = existing_dt.iloc[i].to_pydatetime()
            b = existing_dt.iloc[i + 1].to_pydatetime()
            gap = b - a
            if gap >= (period_delta * 2):
                # fetch the interior only; don't include endpoints (likely already stored)
                fetch_windows.append((a + period_delta, b))

        # Nothing to do
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
            # merge if overlaps or touches within 1 period
            if s <= pe + period_delta:
                merged[-1] = (ps, max(pe, e))
            else:
                merged.append((s, e))

        # --- 4) Fetch + insert ---
        ex = equity._ticker.get_exchange()

        rth_open = _parse_hms(ex.rth_open) if ex and getattr(ex, "rth_open", None) else time(9, 30)
        rth_close = _parse_hms(ex.rth_close) if ex and getattr(ex, "rth_close", None) else time(16, 0)

        for s, e in merged:
            if e <= s:
                continue

            # Clip into per-day RTH slices
            slices = _clip_window_to_rth_per_day(s, e, rth_open, rth_close)
            if not slices:
                continue

            # Stitch slices back together to reduce IBKR calls
            stitched = _stitch_slices(slices, rth_open=rth_open, rth_close=rth_close)

            for ss, ee in stitched:
                print("STITCHED FETCH:", ss, ee)
                _fetch_and_insert(ss, ee)

        # IMPORTANT: return AFTER processing all windows
        return self.get_prices(equity, period, start_date, end_date)



    # ---------- UPDATE ----------

    def update(self, equity: Equity, datetime: datetime, *, open: float, high: float, low: float, close: float, volume: int) -> int:
        cur = self.connection.cursor()
        cur.execute(
            """
            UPDATE equity_prices
            SET open = ?, high = ?, low = ?, close = ?, volume = ?
            WHERE equity_id = ? AND datetime = ?
            """,
            (open, high, low, close, volume, equity._id, datetime),
        )
        self.connection.commit()
        return cur.rowcount

    # ---------- DELETE ----------

    def delete(self, equity_id: int) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equity_prices WHERE equity_id = ?", (equity_id,))
        self.connection.commit()
        return cur.rowcount

    def delete_days(self, equity_id: int, start_date: datetime, end_date: datetime) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equity_prices WHERE equity_id = ? AND datetime BETWEEN ? AND ?", (equity_id, start_date, end_date))
        self.connection.commit()
        return cur.rowcount

    def delete_all(self) -> int:
        cur = self.connection.cursor()
        cur.execute("DELETE FROM equity_prices")
        self.connection.commit()
        return cur.rowcount
