from __future__ import annotations
import json
import requests

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Literal, Optional

import pandas as pd

# TODO: do exchange handling in service

#TODO
from data_providers.clients.base import FundamentalDataProvider, MarketDataProvider, IssuerInfo, EquityInfo, Provider

@dataclass
class MassiveConfig:
    api_key: str
    base_url: Optional[str] = "/v3/reference"

class MassiveProvider(MarketDataProvider, FundamentalDataProvider):
    provider = Provider.MASSIVE

    def __init__(self, config: MassiveConfig):
        self.api_key = config.api_key
        self.base_url = config.base_url
        
    def connect(self):
        # No persistent connection needed for FMP, but we can validate the API key
        pass
    
    def disconnect(self):
        pass
    
    # TODO: Implement a way to fetch a mic off of exchange_name and go backwards
    def get_issuer_information(self, symbol: str, exchange_name: str) -> IssuerInfo:
        extension = f"/tickers?ticker={symbol}&active=true&order=asc&limit=100&sort=ticker&apiKey={self.api_key}"

        url = self.base_url + extension

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch issuer information for {symbol}: {response.text}")
        
        data = response.json()
        if not data or "results" not in data or len(data["results"]) == 0:
            raise Exception(f"No issuer information found for {symbol}")
        
        # For simplicity, we take the first result. In a real implementation, you might want to handle multiple matches.
        result = data["results"][0]

        return IssuerInfo(
            symbol=result.get("ticker"),
            # THIS NEEDS TO CHANGE TODO
            exchange=exchange_name,
            currency=result.get("currency"),
            full_name=result.get("name"),
            sec_type=result.get("type"),
            timezone=result.get("timezone"),
            provider=self.provider,
            cik=result.get("cik")
        )
            