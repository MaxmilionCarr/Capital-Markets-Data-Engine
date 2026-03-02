from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Literal, Optional

import pandas as pd
from ib_async import IB, Contract
import time as _time
import random as _random
from collections import deque as _deque

from data_providers.clients.base import MarketDataProvider, Provider, IssuerInfo, EquityInfo


# ---------------- helpers ----------------
def _hhmm_to_hms(hhmm: str) -> str:
    # "0930" -> "09:30:00"
    hhmm = hhmm.strip()
    return f"{hhmm[:2]}:{hhmm[2:4]}:00"

def _extract_first_session(hours: str) -> tuple[str | None, str | None]:
    """
    Parse IBKR tradingHours/liquidHours:
      "YYYYMMDD:HHMM-YYYYMMDD:HHMM;YYYYMMDD:CLOSED;..."
    Return first non-CLOSED (open_hms, close_hms).
    """
    if not hours:
        return None, None

    for seg in hours.split(";"):
        if not seg:
            continue
        day, rhs = seg.split(":", 1)
        if rhs == "CLOSED":
            continue

        start_s, end_s = rhs.split("-", 1)

        # normalize "YYYYMMDD:HHMM" -> "HHMM"
        if ":" in start_s:
            start_s = start_s.split(":", 1)[1]
        if ":" in end_s:
            end_s = end_s.split(":", 1)[1]

        # sometimes IBKR can give "HHMM-HHMM,HHMM-HHMM" (multiple sessions)
        # take first session only
        if "," in start_s:
            start_s = start_s.split(",", 1)[0]
        if "," in end_s:
            end_s = end_s.split(",", 1)[0]

        return _hhmm_to_hms(start_s), _hhmm_to_hms(end_s)

    return None, None


def _parse_hhmm(hhmm: str) -> time:
    # "0930" -> time(9,30)
    return time(int(hhmm[:2]), int(hhmm[2:]))


def _parse_ibkr_hours(hours: str) -> dict[str, tuple[time, time]]:
    """
    IBKR liquidHours / tradingHours format:
      "YYYYMMDD:HHMM-YYYYMMDD:HHMM;YYYYMMDD:CLOSED;..."
    Returns map: "YYYYMMDD" -> (open_time, close_time) for non-closed days.
    """
    out: dict[str, tuple[time, time]] = {}
    if not hours:
        return out

    for seg in hours.split(";"):
        if not seg:
            continue
        day, rhs = seg.split(":", 1)
        if rhs == "CLOSED":
            continue
        start_s, end_s = rhs.split("-", 1)
        # start_s like "0930" or "20260130:0930" depending on field; normalize
        if ":" in start_s:
            start_s = start_s.split(":", 1)[1]
        if ":" in end_s:
            end_s = end_s.split(":", 1)[1]
        out[day] = (_parse_hhmm(start_s), _parse_hhmm(end_s))
    return out


def _clip_to_rth_bounds(start_dt: datetime, end_dt: datetime, rth_open: time, rth_close: time) -> tuple[datetime, datetime] | None:
    """
    Clip an arbitrary [start_dt, end_dt] to the same-day RTH window.
    Returns None if there is no overlap (e.g. 00:00-09:30).
    """
    day0 = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    rth_start = day0.replace(hour=rth_open.hour, minute=rth_open.minute, second=0, microsecond=0)
    rth_end = day0.replace(hour=rth_close.hour, minute=rth_close.minute, second=0, microsecond=0)

    s = max(start_dt, rth_start)
    e = min(end_dt, rth_end)
    if s >= e:
        return None
    return s, e


def _normalize_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], utc=False).dt.tz_localize(None)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime")
    return df.reset_index(drop=True)


def _filter_anchor(df: pd.DataFrame, start_date: datetime, bar_size: str) -> pd.DataFrame:
    """
    Enforce user-defined anchor:
      - for 1 hour: keep bars whose minute == start_date.minute and second==0
      - for 5 mins: keep bars aligned to 5-min grid AND (minute-start_min)%5==0
    """
    if df.empty:
        return df

    if bar_size == "1 hour":
        m = start_date.minute
        return df[(df["datetime"].dt.minute == m) & (df["datetime"].dt.second == 0)]

    if bar_size == "5 mins":
        m0 = start_date.minute
        return df[((df["datetime"].dt.minute - m0) % 5 == 0) & (df["datetime"].dt.second == 0)]

    return df


def _duration_components(total_seconds: int) -> dict[str, int]:
    """
    IBKR durationStr supports Y, W, D, S.
    This returns a greedy split that keeps your "granularity" idea.
    """
    remaining = int(total_seconds)
    years, remaining = divmod(remaining, 365 * 24 * 3600)
    weeks, remaining = divmod(remaining, 7 * 24 * 3600)
    days, remaining = divmod(remaining, 24 * 3600)
    seconds = remaining
    return {"years": years, "weeks": weeks, "days": days, "seconds": seconds}

class _HistPacer:
    """
    Keeps you under IBKR historical pacing:
    - avoid bursts (e.g. 6 req in 2s)
    - avoid >60 in 10min
    - back off when IB starts slowing responses
    """
    def __init__(
        self,
        min_interval_s: float = 0.5,   # ~2.2 req/s, safely under 6/2s rule
        max_10min: int = 10000,            # keep headroom under 60
        adapt_threshold_s: float = 1.5, # if a request takes >1.5s, treat as throttling
        stop_threshold_s: float = 3.0
    ):
        self.min_interval_s = float(min_interval_s)
        self.max_10min = int(max_10min)
        self.adapt_threshold_s = float(adapt_threshold_s)
        self.stop_threshold_s = float(stop_threshold_s)

        self._last_req_t = 0.0
        self._req_times = _deque()  # timestamps of last 10 minutes
        self._penalty_until = 0.0   # if throttled, wait until this time

    def before_request(self) -> None:
        print("Checking pacing before request...")
        now = _time.time()

        # 1) enforce adaptive penalty window
        if now < self._penalty_until:
            print("Currently in penalty window until", datetime.fromtimestamp(self._penalty_until), "sleeping for", self._penalty_until - now, "seconds...")
            _time.sleep(self._penalty_until - now)

        # 2) enforce min interval
        now = _time.time()
        dt = now - self._last_req_t
        if dt < self.min_interval_s:
            print(f"Only {dt:.2f}s since last request, sleeping for {self.min_interval_s - dt:.2f}s to respect min interval...")
            _time.sleep(self.min_interval_s - dt)

        # 3) enforce <= max_10min requests in rolling 10 min
        now = _time.time()
        cutoff = now - 600.0
        while self._req_times and self._req_times[0] < cutoff:
            self._req_times.popleft()
        if len(self._req_times) >= self.max_10min:
            # sleep until the oldest request exits the 10-min window (+ tiny jitter)
            sleep_s = (self._req_times[0] + 600.0) - now + _random.uniform(0.05, 0.20)
            if sleep_s > 0:
                print(f"Made {len(self._req_times)} requests in the last 10 minutes, sleeping for {sleep_s:.2f}s to respect max_10min...")
                _time.sleep(sleep_s)

        # mark request time
        self._last_req_t = _time.time()
        self._req_times.append(self._last_req_t)

    def after_request(self, elapsed_s: float) -> None:
        # If IB starts responding slowly, back off a bit to avoid the “spiral”
        if elapsed_s >= self.adapt_threshold_s:
            # ramp penalty proportional to slowness, cap it
            penalty = min(10.0, 0.5 * elapsed_s) + _random.uniform(0.05, 0.25)
            self._penalty_until = max(self._penalty_until, _time.time() + penalty)

# ---------------- provider ----------------

@dataclass
class IBKRConfig:
    host: str = "127.0.0.1"
    port: int = 55000
    client_id: int = 1
    timeout: int = 10

    reconnect_ttl_seconds: Optional[float] = 10.0
    auto_reconnect: bool = True

    pacer: Optional[_HistPacer] = _HistPacer()

class IBKRProvider(MarketDataProvider):
    provider = Provider.IBKR

    def __init__(self, config: IBKRConfig = IBKRConfig()):
        self._cfg = config
        self._ib = IB()
        self._connected = False

    # ---- connection ----
    def connect(self):
        if self._connected:
            return
        self._ib.connect(self._cfg.host, self._cfg.port, clientId=self._cfg.client_id, timeout=self._cfg.timeout)
        self._connected = True

    def disconnect(self):
        if not self._connected:
            return
        self._ib.disconnect()
        self._connected = False

    # ---- issuer info ----
    def get_issuer_information(self, symbol: str, exchange_name: Optional[str] = None):
        if not self._connected:
            raise ConnectionError("Not connected to IBKR. Call connect() first.")

        candidates = self._ib.reqMatchingSymbols(symbol)
        if not candidates:
            raise ValueError(f"No matching symbols found for {symbol}")

        contract = None
        if exchange_name:
            for c in candidates:
                if c.contract.primaryExchange == exchange_name and c.contract.symbol == symbol:
                    contract = c.contract
                    break
        if contract is None:
            contract = candidates[0].contract

        details = self._ib.reqContractDetails(contract)
        if not details:
            raise ValueError(f"No contract details found for {symbol}")

        d0 = details[0]
        trading_hours = getattr(d0, "tradingHours", "") or ""
        liquid_hours = getattr(d0, "liquidHours", "") or ""

        # prefer liquidHours for “RTH-ish” liquidity, fallback to tradingHours
        o, c = _extract_first_session(liquid_hours) 
        if o is None or c is None:
            o, c = _extract_first_session(trading_hours)

        return IssuerInfo(
            symbol=contract.symbol,
            exchange=getattr(contract, "primaryExchange", None) or None,
            currency=getattr(contract, "currency", None) or None,
            full_name=getattr(d0, "longName", None),
            timezone=getattr(d0, "timeZoneId", None),
            sec_type=contract.secType,
            provider=self.provider,
            rth_open=o,
            rth_close=c,
        )

    # ---- Equity Info ----
    def get_equity_information(self, symbol: str, exchange_name: str = None, currency: str = None) -> EquityInfo:
        if not self._connected:
            raise ConnectionError("Not connected to IBKR. Call connect() first.")

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.primaryExchange = exchange_name
        if currency is None:
            # IBKR requires currency for contract qualification; default to USD if not provided
            contract.currency = "USD"
        else:
            contract.currency = currency

        details = self._ib.reqContractDetails(contract)
        if not details:
            raise ValueError(f"No contract details found for {symbol}")
        d0 = details[0]

        return EquityInfo(
            industry=getattr(d0, "industry", None),
            sector=getattr(d0, "category", None),
            dividend_yield=getattr(d0, "dividendYield", None),
            pe_ratio=getattr(d0, "peRatio", None),
            eps=getattr(d0, "eps", None),
            beta=getattr(d0, "beta", None),
            market_cap=getattr(d0, "marketCap", None),
        )

    # ---- Trading hours (per-contract) ----
    def _get_liquid_hours_map(self, contract: Contract) -> tuple[str | None, dict[str, tuple[time, time]]]:
        """
        Returns (timezone_id, day->(open,close)) from IBKR ContractDetails.liquidHours.
        Day key is "YYYYMMDD".
        """
        details = self._ib.reqContractDetails(contract)
        if not details:
            return None, {}
        d0 = details[0]
        tz = getattr(d0, "timeZoneId", None)
        liquid = getattr(d0, "liquidHours", "") or ""
        return tz, _parse_ibkr_hours(liquid)

    # ---- prices ----
    #TODO: CACHE CONTRACT DETAILS + LIQUID HOURS MAP in a seperate function call with lru-cache
    #TODO: WAY TO VERBOSE CUT DOWN AND TRIM (THINK ABOUT MAKING SEPERATE CLASS INSTANCES FOR TECHNICAL DATA, FUNDAMENTAL, ETC)
    def get_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        bar_size: Literal["1 day", "1 hour", "30 mins", "5 mins"] = "1 day",
        *,
        rth_open: time | None = None,
        rth_close: time | None = None,
    ) -> pd.DataFrame:
        pacer = self._cfg.pacer

        if not self._connected:
            raise ConnectionError("Not connected to IBKR. Call connect() first.")

        end_dt = end_date or datetime.now()
        if end_dt <= start_date:
            raise ValueError("end_date must be after start_date")

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.primaryExchange = exchange_name
        contract = self._ib.qualifyContracts(contract)[0]

        open_t = rth_open or time(9, 30)
        close_t = rth_close or time(16, 0)

        def _snap_end(dt: datetime) -> datetime:
            # Snap every request endDateTime to market close (RTH close)
            # so chunks don't end at 00:00 and accidentally slice sessions.
            return dt.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0)

        def _ib_end_str(dt: datetime) -> str:
            return dt.strftime("%Y%m%d %H:%M:%S")

        request_counter = 0

        def _req(end_dt_req: datetime, duration_str: str) -> list:
            nonlocal request_counter
            request_counter += 1

            if pacer:
                pacer.before_request()
            t0 = _time.perf_counter()

            end_dt_req = _snap_end(end_dt_req)

            print(f"Making IBKR req {request_counter}: end={end_dt_req} dur={duration_str}...")
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=_ib_end_str(end_dt_req),
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
            )

            elapsed = _time.perf_counter() - t0
            if pacer:
                pacer.after_request(elapsed)

            n = 0 if not bars else len(bars)
            print(f"Req {request_counter}: end={end_dt_req} dur={duration_str} n={n} time_ms={elapsed*1000:.0f}")
            return bars

        def _append_bars(bars, out: list[dict]) -> None:
            for b in bars:
                out.append(
                    {
                        "datetime": b.date,
                        "open": b.open,
                        "high": b.high,
                        "low": b.low,
                        "close": b.close,
                        "volume": b.volume,
                    }
                )

        MAX_CHUNK_BY_BAR: dict[str, timedelta] = {
            "5 mins": timedelta(days=21),
            "30 mins": timedelta(days=30),
            "1 hour": timedelta(days=30),
            "1 day": timedelta(days=365 * 2),
        }
        MIN_CHUNK_BY_BAR: dict[str, timedelta] = {
            "5 mins": timedelta(days=1),
            "30 mins": timedelta(days=1),
            "1 hour": timedelta(days=1),
            "1 day": timedelta(days=1),
        }

        max_chunk = MAX_CHUNK_BY_BAR.get(bar_size, timedelta(days=30))
        min_chunk = MIN_CHUNK_BY_BAR.get(bar_size, timedelta(days=1))

        def _plan_step(remaining: timedelta) -> tuple[str, int, timedelta] | None:
            if remaining < min_chunk:
                return None

            year_td = timedelta(days=365)
            week_td = timedelta(days=7)
            day_td = timedelta(days=1)

            def _cap_count(remaining_td: timedelta, unit_td: timedelta, max_td: timedelta) -> int:
                return max(0, min(int(remaining_td // unit_td), int(max_td // unit_td)))

            if max_chunk >= year_td and remaining >= year_td and min_chunk <= max_chunk:
                n = _cap_count(remaining, year_td, max_chunk)
                if n > 0:
                    step_td = year_td * n
                    if step_td >= min_chunk:
                        return "Y", n, step_td

            if max_chunk >= week_td and remaining >= week_td:
                n = _cap_count(remaining, week_td, max_chunk)
                if n > 0:
                    step_td = week_td * n
                    if step_td >= min_chunk:
                        return "W", n, step_td

            if max_chunk >= day_td and remaining >= day_td:
                n = _cap_count(remaining, day_td, max_chunk)
                if n > 0:
                    step_td = day_td * n
                    if step_td >= min_chunk:
                        return "D", n, step_td

            min_s = int(min_chunk.total_seconds())
            max_s = int(max_chunk.total_seconds())
            rem_s = int(remaining.total_seconds())
            if min_s >= 1 and max_s >= min_s and rem_s >= min_s:
                n = min(rem_s, max_s)
                return "S", n, timedelta(seconds=n)

            return None

        data: list[dict] = []
        cur_end = _snap_end(end_dt)

        while cur_end > start_date:
            remaining = cur_end - start_date
            plan = _plan_step(remaining)

            if plan is None:
                # keep your existing tail fetch logic unchanged, just snap end
                if remaining.total_seconds() > 0:
                    tail_end = start_date + min_chunk
                    if tail_end > cur_end:
                        tail_end = cur_end
                    tail_end = _snap_end(tail_end)

                    tail_secs = int(min_chunk.total_seconds())
                    tail_days = tail_secs // (24 * 3600)
                    if tail_days >= 1 and bar_size == "1 day":
                        duration_str = f"{tail_days} D"
                    else:
                        duration_str = f"{tail_secs} S"

                    bars = _req(tail_end, duration_str)
                    _append_bars(bars, data)
                break

            unit, n, step_td = plan
            duration_str = f"{n} {unit}"

            bars = _req(cur_end, duration_str)
            _append_bars(bars, data)

            next_end = cur_end - step_td
            if _ib_end_str(next_end) == _ib_end_str(cur_end):
                next_end = cur_end - timedelta(seconds=1)
            cur_end = _snap_end(next_end)

        df = _normalize_bars_df(pd.DataFrame(data))

        if not df.empty:
            df = df[(df["datetime"] >= start_date) & (df["datetime"] <= end_dt)]

        if not df.empty and bar_size != "1 day":
            df = df[(df["datetime"].dt.time >= open_t) & (df["datetime"].dt.time <= close_t)]

        df = _filter_anchor(df, start_date, bar_size)
        return df

    # ---- bonds (unchanged) ----
    '''
    def get_bond_information(self, CUSID: str, exchange_name: str = None, currency: str = None) -> TickerInfo:
        contract = Contract()
        contract.secType = "BOND"
        contract.symbol = CUSID
        contract.exchange = "SMART"
        contract.currency = currency

        details = self._ib.reqContractDetails(contract)
        if not details:
            raise ValueError(f"No contract details found for CUSID {CUSID}")

        d0 = details[0]
        contract = d0.contract
        return TickerInfo(
            symbol=contract.symbol,
            exchange=getattr(contract, "primaryExchange", None) or None,
            currency=getattr(contract, "currency", None) or None,
            long_name=getattr(d0, "longName", None),
            industry=getattr(d0, "industry", None),
            timezone=getattr(d0, "timeZoneId", None),
            sec_type=contract.secType,
            provider=self.provider,
        )

    def get_bond_prices(self, CUSID: str, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        if not self._connected:
            raise ConnectionError("Not connected to IBKR. Call connect() first.")

        contract = Contract()
        contract.secType = "BOND"
        contract.symbol = CUSID
        contract.exchange = "SMART"
        contract.currency = "USD"

        if end_date is None:
            end_dt = datetime.now()
        else:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        delta_days = (end_dt - start_dt).days
        if delta_days <= 0:
            raise ValueError("end_date must be after start_date")

        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime=end_dt.strftime("%Y%m%d %H:%M:%S"),
            durationStr=f"{delta_days} D",
            barSizeSetting="1 day",
            whatToShow="BID",
            useRTH=True,
        )

        df = pd.DataFrame(
            [
                {"datetime": b.date, "open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}
                for b in bars
            ]
        )
        return _normalize_bars_df(df)
    '''
        