"""FRED: 10-year Treasury constant-maturity yield (series DGS10), no key needed."""
from __future__ import annotations

import io

import pandas as pd

URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def parse_series(text: str, series: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text))
    date_col = df.columns[0]  # 'observation_date' (current) or 'DATE' (older)
    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col]).dt.date,
        "value_pct": pd.to_numeric(df[series], errors="coerce"),  # '.' marks a holiday
    }).dropna()
    out["series"] = series
    return out


def series(session, series_id: str = "DGS10") -> pd.DataFrame:
    r = session.get(URL, params={"id": series_id}, timeout=60)
    r.raise_for_status()
    return parse_series(r.text, series_id)
