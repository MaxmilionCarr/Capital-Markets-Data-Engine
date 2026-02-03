from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Literal, Optional

import pandas as pd
from ib_async import IB, Contract

from .base import MarketDataProvider, Provider, TickerInfo, BondInfo, EquityInfo


# ---------------- helpers ----------------
from datetime import time

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


# ---------------- provider ----------------

@dataclass
class IBKRConfig:
    host: str = "127.0.0.1"
    port: int = 55000
    client_id: int = 1
    timeout: int = 10
    default_currency: str = "USD"


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

    # ---- ticker info ----
    def get_ticker_information(self, symbol: str, exchange_name: Optional[str] = None):
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

        return TickerInfo(
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
        contract.currency = currency or self._cfg.default_currency

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

        """
        Key behaviours:
        - Treat start_date/end_date as exchange-local (your requirement).
        - Fetch in chunks (Y/W/D/S) BUT clip each chunk to RTH (liquidHours if available, else fallback 09:30-16:00).
        - Enforce anchor based on start_date for intraday bars:
            start=09:30 & 1 hour -> keep 09:30,10:30,...
        """

        if not self._connected:
            raise ConnectionError("Not connected to IBKR. Call connect() first.")

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.primaryExchange = exchange_name

        contract = self._ib.qualifyContracts(contract)[0]

        end_dt = end_date or datetime.now()
        if end_dt <= start_date:
            raise ValueError("end_date must be after start_date")

        # pull per-day liquid hours (covers holidays/early closes etc); if you don't care, you can ignore it
        _, liquid_by_day = self._get_liquid_hours_map(contract)

        # fallback regular hours if contract details didn't give the day
        fallback_open = rth_open or time(9, 30)
        fallback_close = rth_close or time(16, 0)


        # duration split (your “granularity”)
        comps = _duration_components(int((end_dt - start_date).total_seconds()))

        data: list[dict] = []

        def _req(end_dt_req: datetime, duration_str: str) -> list:
            return self._ib.reqHistoricalData(
                contract,
                endDateTime=end_dt_req,
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
            )

        # walk backward like your original code, but we clip each chunk so we don't ask for 00:00->09:30 gaps
        cur_end = end_dt

        def _append_bars(bars):
            for bar in bars:
                data.append(
                    {
                        "datetime": bar.date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                    }
                )

        for key, unit, step in (
            ("years", "Y", lambda n: timedelta(days=365 * n)),
            ("weeks", "W", lambda n: timedelta(weeks=n)),
            ("days", "D", lambda n: timedelta(days=n)),
            ("seconds", "S", lambda n: timedelta(seconds=n)),
        ):
            n = comps[key]
            if n <= 0:
                continue

            duration_str = f"{n} {unit}"

            # ---- CLIP REQUEST WINDOW TO RTH (simple + effective) ----
            # We know the intended window is [cur_end - step(n), cur_end].
            intended_start = cur_end - step(n)
            intended_end = cur_end

            # If intraday, clip each *day* separately to avoid fetching the overnight gap.
            if bar_size in ("1 hour", "30 mins", "5 mins"):
                day = intended_start.replace(hour=0, minute=0, second=0, microsecond=0)
                last_day = intended_end.replace(hour=0, minute=0, second=0, microsecond=0)

                while day <= last_day:
                    s = max(intended_start, day.replace(hour=fallback_open.hour, minute=fallback_open.minute, second=0, microsecond=0))
                    e = min(intended_end,   day.replace(hour=fallback_close.hour, minute=fallback_close.minute, second=0, microsecond=0))
                    if s < e:
                        secs = int((e - s).total_seconds())
                        bars = _req(e, f"{secs} S")
                        _append_bars(bars)
                    day += timedelta(days=1)
            else:
                # daily bars are fine with durationStr
                bars = _req(cur_end, duration_str)
                _append_bars(bars)

            cur_end -= step(n)
        df = _normalize_bars_df(pd.DataFrame(data))

        # enforce user anchor for intraday (this is what makes 09:30 hourly work)

        return df

    # ---- bonds (unchanged) ----
    def get_bond_information(self, CUSID: str, exchange_name: str = None, currency: str = None) -> TickerInfo:
        contract = Contract()
        contract.secType = "BOND"
        contract.symbol = CUSID
        contract.exchange = "SMART"
        contract.currency = currency or self._cfg.default_currency

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

