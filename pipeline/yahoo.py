"""Yahoo Finance chart endpoint — used only for SPY's dividend history.

Nasdaq's dividend endpoint covers Nasdaq-listed securities only (SPY trades on NYSE Arca), which the
first live run showed on 2026-10-20. Unofficial endpoint, one call a day.
"""
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

HOSTS = ("https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com")
NY = ZoneInfo("America/New_York")


def parse_dividends(payload: dict, symbol: str) -> pd.DataFrame:
    result = ((payload.get("chart") or {}).get("result") or [None])[0] or {}
    divs = ((result.get("events") or {}).get("dividends") or {}).values()
    rows = [{"symbol": symbol,
             "ex_date": dt.datetime.fromtimestamp(int(d["date"]), tz=NY).date(),
             "amount": float(d["amount"])} for d in divs if d.get("date") and d.get("amount") is not None]
    df = pd.DataFrame(rows, columns=["symbol", "ex_date", "amount"])
    return df.sort_values("ex_date").reset_index(drop=True)


def dividends(session, symbol: str = "SPY") -> pd.DataFrame:
    last = None
    for host in HOSTS:
        try:
            r = session.get(f"{host}/v8/finance/chart/{symbol}",
                            params={"range": "max", "interval": "1mo", "events": "div"}, timeout=60)
            r.raise_for_status()
            return parse_dividends(r.json(), symbol)
        except Exception as exc:  # noqa: BLE001 - try the other host
            last = exc
    raise RuntimeError(f"Yahoo dividends failed on both hosts: {last}")
