"""Daily job (weekday evenings): SPY prices and dividends, the stock universe snapshot, 10-year Treasury.

Each step runs independently; a failed step is recorded in manifest.json and the job exits
non-zero after publishing whatever succeeded.
"""
from __future__ import annotations

import datetime as dt
import sys

from . import fred, nasdaq, treasury as ust, yahoo
from .common import (Manifest, ensure_release, log, nasdaq_session, norm_symbol, plain_session,
                     publish, quick_session, read_parquet, upsert, utcnow, write_parquet)

UNIVERSE_FLOOR = 250e6  # keep everything above $250M so names crossing the $1B floor have history


def spy_prices(m: Manifest, ns) -> dt.date | None:
    old = read_parquet("spy_daily.parquet")
    today = utcnow().date()
    start = dt.date(2015, 1, 1) if old is None or old.empty else max(old["date"]) - dt.timedelta(days=45)
    new = nasdaq.historical(ns, "spy", "etf", start, today, label="SPY")
    if new.empty or not (new["close"] > 0).all():
        raise ValueError("SPY history empty or has non-positive closes")
    df = upsert(old, new, ["date"])
    moves = df["close"].pct_change().abs().dropna()
    if (moves > 0.15).any():
        raise ValueError(f"SPY day-over-day move above 15% on {df.loc[moves[moves > 0.15].index, 'date'].tolist()}")
    publish([write_parquet(df, "spy_daily.parquet")])
    last = max(df["date"])
    m.record("spy_daily.parquet", "ok", len(df), last_date=str(last), last_close=float(df["close"].iloc[-1]))
    return last


SESSION_CHECK_SYMBOLS = ("AAPL", "MSFT", "KO")


def snapshot_session(snap, ns, latest: dt.date, tol: float = 1e-4):
    """Find the trading session whose official closes the screener's last_sale values equal.

    Returns (date, source) when at least two of the check symbols agree on a session, else None.
    """
    votes: dict[dt.date, int] = {}
    for sym in SESSION_CHECK_SYMBOLS:
        row = snap[snap["symbol"] == sym]
        if row.empty:
            continue
        last = float(row["last_sale"].iloc[0])
        try:
            hist = nasdaq.historical(ns, sym.lower(), "stocks", latest - dt.timedelta(days=10), latest, label=sym)
        except Exception as exc:  # noqa: BLE001 - an unverified date is reported, not fatal
            log(f"session check: {sym} history failed ({exc})")
            continue
        for day, close in sorted(zip(hist["date"], hist["close"]), reverse=True):
            if day <= latest and close > 0 and abs(last / close - 1) < tol:
                votes[day] = votes.get(day, 0) + 1
                break
    agreed = [d for d, n in votes.items() if n >= 2]
    if not agreed:
        return None
    day = max(agreed)
    return day, f"session matched to official closes ({votes[day]}/{len(SESSION_CHECK_SYMBOLS)} check symbols)"


def universe_snapshot(m: Manifest, ns, trading_date: dt.date | None) -> None:
    snap = nasdaq.screener_snapshot(ns)
    n_all = len(snap)
    if n_all < 3000:
        raise ValueError(f"screener returned only {n_all} rows")
    for sym in ("AAPL", "MSFT", "KO"):
        row = snap[snap["symbol"] == sym]
        if row.empty or not (row["last_sale"] > 0).all():
            raise ValueError(f"screener sanity check failed for {sym}")
    status = "ok"
    date_source = "SPY latest session"
    if trading_date is None:
        trading_date = (utcnow() - dt.timedelta(hours=8)).date()
        date_source = "run date (SPY step failed)"
    # The screener's last_sale can lag SPY's latest session (seen 2026-10-20 to 10-24: every
    # snapshot held the previous session's closes). Date the snapshot by the session whose
    # official closes it matches, not by SPY's latest session.
    matched = snapshot_session(snap, ns, trading_date)
    if matched is not None:
        trading_date, date_source = matched
    else:
        status = "partial"
        date_source += " (unverified: last_sale matched no official close of the check symbols)"
    snap = snap[snap["market_cap"] >= UNIVERSE_FLOOR].copy()
    snap.insert(0, "date", trading_date)
    snap["key"] = snap["symbol"].map(norm_symbol)
    snap["fetched_utc"] = utcnow().isoformat()
    df = upsert(read_parquet("prices_daily.parquet"), snap, ["date", "symbol"])
    publish([write_parquet(df, "prices_daily.parquet")])
    m.record("prices_daily.parquet", status, len(df), last_date=str(trading_date), date_source=date_source,
             symbols_last_date=len(snap), screener_rows=n_all,
             note="last_sale is Nasdaq's last trade at fetch time and can include after-hours; "
                  "use closes_official for decision prices")


def spy_dividends(m: Manifest) -> None:
    new = yahoo.dividends(nasdaq_session(), "SPY")
    if len(new) < 8 or not (new["amount"] > 0).all():
        raise ValueError(f"SPY dividends look wrong: {len(new)} rows")
    df = upsert(read_parquet("spy_dividends.parquet"), new, ["ex_date"])
    publish([write_parquet(df, "spy_dividends.parquet")])
    m.record("spy_dividends.parquet", "ok", len(df), last_ex_date=str(max(df["ex_date"])),
             last_amount=float(df["amount"].iloc[-1]), source="Yahoo Finance chart events")


def treasury(m: Manifest) -> None:
    old = read_parquet("treasury_10y.parquet")
    source = "home.treasury.gov daily par yield curve"
    try:
        new = ust.ten_year(plain_session(), ust.years_to_fetch(old is not None and not old.empty, utcnow().date()))
    except Exception as exc:  # noqa: BLE001 - fall back to FRED's copy of the same series
        log(f"treasury.gov failed ({exc}); trying FRED")
        new = fred.series(quick_session(), "DGS10")
        source = "FRED DGS10 (fallback)"
    if new.empty:
        raise ValueError("10-year yield empty")
    df = upsert(old, new.drop(columns=["series"]).assign(series="UST10Y"), ["date"])
    publish([write_parquet(df, "treasury_10y.parquet")])
    m.record("treasury_10y.parquet", "ok", len(df), last_date=str(max(df["date"])),
             last_value_pct=float(df["value_pct"].iloc[-1]), source=source)


def main() -> int:
    ensure_release()
    m = Manifest()
    ns = nasdaq_session()
    trading_date = None
    steps = [
        ("spy_daily.parquet", lambda: spy_prices(m, ns)),
        ("prices_daily.parquet", lambda: universe_snapshot(m, ns, trading_date)),
        ("spy_dividends.parquet", lambda: spy_dividends(m)),
        ("treasury_10y.parquet", lambda: treasury(m)),
    ]
    for asset, fn in steps:
        try:
            result = fn()
            if asset == "spy_daily.parquet":
                trading_date = result
        except Exception as exc:  # noqa: BLE001 - record and carry on
            m.record(asset, "failed", error=f"{type(exc).__name__}: {exc}")
    m.save_and_publish("daily")
    log(f"daily done; failures: {m.failures or 'none'}")
    return 1 if m.failures else 0


if __name__ == "__main__":
    sys.exit(main())
