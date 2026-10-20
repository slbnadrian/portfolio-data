"""Nasdaq public website API: stock screener snapshot, price history, dividends, earnings dates.

These are the same endpoints nasdaq.com's own pages call. Personal, low-volume use only.
"""
from __future__ import annotations

import datetime as dt
from urllib.parse import quote

import pandas as pd

from .common import num

API = "https://api.nasdaq.com/api"


def _rows(obj):
    """Find the first list called 'rows' anywhere in a Nasdaq JSON payload."""
    if isinstance(obj, dict):
        if isinstance(obj.get("rows"), list):
            return obj["rows"]
        for v in obj.values():
            found = _rows(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _rows(v)
            if found is not None:
                return found
    return None


def parse_screener(payload: dict) -> pd.DataFrame:
    rows = _rows(payload) or []
    if not rows:
        raise ValueError("screener payload has no rows")
    df = pd.DataFrame(rows)
    url = df.get("url", pd.Series([""] * len(df))).fillna("")
    out = pd.DataFrame({
        "symbol": df["symbol"].astype(str).str.strip(),
        # the path nasdaq.com uses for this ticker, e.g. 'brk.b' -- the quote API wants it
        "api_symbol": url.map(lambda u: u.rstrip("/").rsplit("/", 1)[-1] if u else ""),
        "name": df.get("name"),
        "last_sale": df["lastsale"].map(num),
        "change_1d": df["pctchange"].map(num) / 100.0,
        "volume": df["volume"].map(num),
        "market_cap": df["marketCap"].map(num),
        "country": df.get("country"),
        "ipo_year": df.get("ipoyear").map(num) if "ipoyear" in df else None,
        "sector": df.get("sector"),
        "industry": df.get("industry"),
    })
    out.loc[out["api_symbol"] == "", "api_symbol"] = out["symbol"].str.lower()
    return out


def screener_snapshot(session) -> pd.DataFrame:
    r = session.get(f"{API}/screener/stocks",
                    params={"tableonly": "true", "limit": "25", "offset": "0", "download": "true"},
                    timeout=120)
    r.raise_for_status()
    return parse_screener(r.json())


def parse_historical(payload: dict, symbol: str) -> pd.DataFrame:
    rows = _rows(payload) or []
    if not rows:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    return pd.DataFrame({
        "symbol": symbol,
        "date": pd.to_datetime(df["date"], format="%m/%d/%Y").dt.date,
        "open": df["open"].map(num),
        "high": df["high"].map(num),
        "low": df["low"].map(num),
        "close": df["close"].map(num),
        "volume": df["volume"].map(num),
    })


def historical(session, api_symbol: str, assetclass: str, start: dt.date, end: dt.date,
               label: str | None = None) -> pd.DataFrame:
    r = session.get(f"{API}/quote/{quote(api_symbol, safe='')}/historical",
                    params={"assetclass": assetclass, "fromdate": start.isoformat(),
                            "todate": end.isoformat(), "limit": "9999"},
                    timeout=60)
    r.raise_for_status()
    return parse_historical(r.json(), label or api_symbol.upper())


def parse_earnings(payload: dict, day: dt.date) -> pd.DataFrame:
    rows = _rows(payload) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["date", "symbol", "time", "fiscal_quarter_ending", "eps_forecast"])
    return pd.DataFrame({
        "date": day,
        "symbol": df["symbol"].astype(str).str.strip(),
        "time": df.get("time"),
        "fiscal_quarter_ending": df.get("fiscalQuarterEnding"),
        "eps_forecast": df.get("epsForecast").map(num) if "epsForecast" in df else None,
    })


def earnings_on(session, day: dt.date) -> pd.DataFrame:
    r = session.get(f"{API}/calendar/earnings", params={"date": day.isoformat()}, timeout=60)
    r.raise_for_status()
    return parse_earnings(r.json(), day)
