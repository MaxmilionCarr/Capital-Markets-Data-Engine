from __future__ import annotations

import time as _time
import threading
from typing import List, Literal, Optional
import pandas as pd
from datetime import datetime, time

from data_providers.clients import IBKRConfig, IBKRProvider
from data_providers.clients.base import IssuerInfo, ExchangeInfo, EquityInfo


class IBKRService:
    name = "IBKR"
    issuer_capabilities = IBKRProvider.issuer_capabilities
    equity_capabilities = IBKRProvider.equity_capabilities
    def __init__(self, config: IBKRConfig):
        self._config = config
        self._client = IBKRProvider(config)

        self._lock = threading.RLock()
        self._connected = False
        self._last_touch_ts: float = 0.0

    # -------------------------
    # Connection management
    # -------------------------

    def _now(self) -> float:
        return _time.monotonic()

    def _ensure_connected(self) -> None:
        with self._lock:
            now = self._now()

            if not self._connected:
                self._client.connect()
                self._connected = True
                self._last_touch_ts = now
                return

            # auto reconnect disabled → just reuse
            if not self._config.auto_reconnect:
                self._last_touch_ts = now
                return

            ttl = self._config.reconnect_ttl_seconds

            # stale → reconnect
            if ttl is not None and (now - self._last_touch_ts) > ttl:
                try:
                    self._client.disconnect()
                except Exception:
                    pass  # reconnect anyway

                self._client.connect()
                self._connected = True
                self._last_touch_ts = now
                return

            # still fresh
            self._last_touch_ts = now

    def close(self) -> None:
        with self._lock:
            if self._connected:
                try:
                    self._client.disconnect()
                finally:
                    self._connected = False
                    self._last_touch_ts = 0.0

    # -------------------------
    # Service API
    # -------------------------

    def fetch_issuer(self, symbol: str, exchange_name: str = None) -> IssuerInfo | List[IssuerInfo]:
        self._ensure_connected()
        return self._client.get_issuer_information(symbol, exchange_name)

    def fetch_exchange(self, symbol: str, exchange_name: str) -> ExchangeInfo:
        self._ensure_connected()
        return self._client.get_exchange_information(symbol, exchange_name)

    def fetch_equity(self, symbol: str, exchange_name: str = None, currency: str = None) -> EquityInfo:
        self._ensure_connected()
        return self._client.get_equity_information(symbol, exchange_name, currency)

    def fetch_equity_prices(
        self,
        symbol: str,
        exchange_name: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        bar_size: Literal["5 mins", "1 hour", "1 day"] = "1 day",
        rth_open: Optional[time] = None,
        rth_close: Optional[time] = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        return self._client.get_equity_prices(
            symbol,
            exchange_name,
            start_date,
            end_date,
            bar_size,
            rth_open=rth_open,
            rth_close=rth_close,
        )

    #TODO
    def fetch_bond(self, symbol):
        self._ensure_connected()
        return self._client.get_bond(symbol)
