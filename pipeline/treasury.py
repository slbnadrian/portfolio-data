"""US Treasury daily par yield curve (official source) — the 10-year yield.

home.treasury.gov publishes one CSV per year. FRED's DGS10 is the same series but timed out from
GitHub's runners on the first live run (2026-10-20), so it is only a fallback here.
"""
from __future__ import annotations

import datetime as dt
import io

import pandas as pd

URL = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
       "daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"
       "&field_tdr_date_value={year}&page&_format=csv")


def parse_year(text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text))
    if "10 Yr" not in df.columns:
        raise ValueError(f"no '10 Yr' column in Treasury CSV: {list(df.columns)[:8]}")
    out = pd.DataFrame({
        "date": pd.to_datetime(df["Date"], format="%m/%d/%Y").dt.date,
        "value_pct": pd.to_numeric(df["10 Yr"], errors="coerce"),
    }).dropna()
    out["series"] = "UST10Y"
    return out


def ten_year(session, years: list[int]) -> pd.DataFrame:
    frames = []
    for y in years:
        r = session.get(URL.format(year=y), timeout=60)
        r.raise_for_status()
        frames.append(parse_year(r.text))
    return pd.concat(frames, ignore_index=True).drop_duplicates("date").sort_values("date").reset_index(drop=True)


def years_to_fetch(have_history: bool, today: dt.date) -> list[int]:
    return [today.year - 1, today.year] if have_history else list(range(today.year - 10, today.year + 1))
