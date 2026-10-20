"""Weekly job (Saturday, after the SEC's nightly bulk rebuild): financials, filings, insider trades,
earnings dates and official closing prices for the investable universe.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from . import nasdaq, sec
from .common import (WORK, Manifest, RateLimiter, download_file, ensure_release, log, nasdaq_session,
                     norm_symbol, publish, read_parquet, sec_session, upsert, utcnow, write_parquet)

FACTS_FLOOR = 250e6        # financials and filings for everything above $250M
DETAIL_FLOOR = 1e9         # insider trades and official closes for the $1B+ universe
FORM4_LOOKBACK_DAYS = 120
FORM4_MAX_PER_RUN = int(os.environ.get("FORM4_MAX_PER_RUN", "30000"))
FILINGS_LOOKBACK_YEARS = 3


def latest_snapshot() -> pd.DataFrame:
    prices = read_parquet("prices_daily.parquet")
    if prices is None or prices.empty:
        raise RuntimeError("prices_daily.parquet missing: run the daily job first")
    last = max(prices["date"])
    return prices[prices["date"] == last].copy()


def universe(m: Manifest, s) -> tuple[pd.DataFrame, pd.DataFrame]:
    r = s.get(sec.TICKERS_URL, timeout=60)
    r.raise_for_status()
    tickers = sec.parse_tickers(r.json())
    tickers["key"] = tickers["ticker"].map(norm_symbol)
    publish([write_parquet(tickers, "sec_tickers.parquet")])
    m.record("sec_tickers.parquet", "ok", len(tickers))
    snap = latest_snapshot()
    uni = snap.merge(tickers[["key", "cik"]].drop_duplicates("key"), on="key", how="left")
    matched = uni["cik"].notna().mean()
    m.record("universe_match", "ok" if matched >= 0.85 else "partial", int(uni["cik"].notna().sum()),
             share_matched=round(float(matched), 3), snapshot_date=str(max(snap["date"])))
    return uni.dropna(subset=["cik"]).astype({"cik": int}), tickers


def filings_and_companies(m: Manifest, s, ciks: set[int]) -> pd.DataFrame:
    path = download_file(s, sec.SUBMISSIONS_ZIP, WORK / "submissions.zip")
    since = utcnow().date() - dt.timedelta(days=int(365.25 * FILINGS_LOOKBACK_YEARS))
    companies, filings = sec.read_submissions(path, ciks, since)
    path.unlink(missing_ok=True)
    if companies.empty or filings.empty:
        raise ValueError("no companies or filings parsed")
    publish([write_parquet(companies, "companies.parquet"), write_parquet(filings, "filings.parquet")])
    m.record("companies.parquet", "ok", len(companies), coverage=round(len(companies) / max(len(ciks), 1), 3))
    m.record("filings.parquet", "ok", len(filings), latest_filing=str(filings["filing_date"].max()))
    return filings


def financials(m: Manifest, s, ciks: set[int]) -> None:
    today = utcnow().date()
    path = download_file(s, sec.FACTS_ZIP, WORK / "companyfacts.zip")
    facts = sec.read_companyfacts(path, ciks, today)
    path.unlink(missing_ok=True)
    if facts.empty:
        raise ValueError("no facts parsed")
    annual = sec.build_annual(facts)
    ttm = sec.build_ttm(facts)
    publish([write_parquet(facts, "facts.parquet"), write_parquet(annual, "annual.parquet"),
             write_parquet(ttm, "ttm.parquet")])
    m.record("facts.parquet", "ok", len(facts), companies=int(facts["cik"].nunique()),
             latest_filed=str(facts["filed"].max()))
    m.record("annual.parquet", "ok", len(annual), companies=int(annual["cik"].nunique()))
    m.record("ttm.parquet", "ok", len(ttm),
             with_net_income=int(ttm["net_income"].notna().sum()) if "net_income" in ttm else 0)


def insiders(m: Manifest, s, filings: pd.DataFrame, detail_ciks: set[int]) -> None:
    cutoff = (utcnow().date() - dt.timedelta(days=FORM4_LOOKBACK_DAYS)).isoformat()
    f4 = filings[filings["form"].isin(["4", "4/A"]) & (filings["filing_date"] >= cutoff)
                 & filings["cik"].isin(detail_ciks)]
    old = read_parquet("insider.parquet")
    done = set(old["accession"]) if old is not None and not old.empty else set()
    todo = f4[~f4["accession"].isin(done)].drop_duplicates("accession").sort_values("filing_date", ascending=False)
    new, failures = sec.fetch_form4s(s, todo, FORM4_MAX_PER_RUN)
    keep_from = (utcnow().date() - dt.timedelta(days=int(365.25 * 3))).isoformat()
    frames = [d for d in (old, new) if d is not None and not d.empty]
    if not frames:
        raise ValueError(f"no Form 4 rows (queued {len(todo)}, failures {failures})")
    df = pd.concat(frames, ignore_index=True)
    df = df[df["filing_date"] >= keep_from].reset_index(drop=True)
    publish([write_parquet(df, "insider.parquet")])
    backlog = max(len(todo) - FORM4_MAX_PER_RUN, 0)
    status = "ok" if failures <= max(20, 0.02 * len(todo)) else "partial"
    m.record("insider.parquet", status, len(df), fetched=len(todo) - backlog, failures=failures, backlog=backlog)


def earnings_calendar(m: Manifest, ns) -> None:
    today = utcnow().date()
    frames, failed = [], 0
    for i in range(0, 43):
        day = today + dt.timedelta(days=i)
        if day.weekday() >= 5:
            continue
        try:
            frames.append(nasdaq.earnings_on(ns, day))
        except Exception as exc:  # noqa: BLE001
            failed += 1
            log(f"earnings {day}: {exc}")
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        raise ValueError("no earnings dates returned")
    df["key"] = df["symbol"].map(norm_symbol)
    publish([write_parquet(df, "earnings_calendar.parquet")])
    m.record("earnings_calendar.parquet", "ok" if failed == 0 else "partial", len(df), days_failed=failed,
             through=str(df["date"].max()))


def official_closes(m: Manifest, ns, uni: pd.DataFrame) -> None:
    today = utcnow().date()
    start = today - dt.timedelta(days=21)
    targets = uni[uni["market_cap"] >= DETAIL_FLOOR][["symbol", "api_symbol"]].drop_duplicates("symbol")
    limiter = RateLimiter(4.0)

    def one(row):
        limiter.wait()
        return nasdaq.historical(ns, row.api_symbol, "stocks", start, today, label=row.symbol)

    frames, failures = [], 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(one, r) for r in targets.itertuples(index=False)]
        for f in as_completed(futures):
            try:
                frames.append(f.result())
            except Exception:  # noqa: BLE001
                failures += 1
    new = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    covered = new["symbol"].nunique() if not new.empty else 0
    coverage = covered / max(len(targets), 1)
    if new.empty:
        raise ValueError("no official closes returned")
    new["key"] = new["symbol"].map(norm_symbol)
    df = upsert(read_parquet("closes_official.parquet"), new, ["symbol", "date"])
    df = df[df["date"] >= today - dt.timedelta(days=int(365.25 * 5))]
    publish([write_parquet(df, "closes_official.parquet")])
    m.record("closes_official.parquet", "ok" if coverage >= 0.9 else "partial", len(df),
             symbols_requested=len(targets), symbols_covered=int(covered), coverage=round(coverage, 3),
             last_date=str(max(new["date"])))


def main() -> int:
    ensure_release()
    m = Manifest()
    s = sec_session()
    ns = nasdaq_session()
    try:
        uni, _ = universe(m, s)
    except Exception as exc:  # noqa: BLE001 - nothing below can run without the universe
        m.record("universe", "failed", error=f"{type(exc).__name__}: {exc}")
        m.save_and_publish("weekly")
        return 1
    ciks = set(uni.loc[uni["market_cap"] >= FACTS_FLOOR, "cik"])
    detail = set(uni.loc[uni["market_cap"] >= DETAIL_FLOOR, "cik"])
    log(f"universe: {len(ciks)} companies >= $250M, {len(detail)} >= $1B")

    filings = None
    for asset, fn in [
        ("filings.parquet", lambda: filings_and_companies(m, s, ciks)),
        ("facts.parquet", lambda: financials(m, s, ciks)),
        ("insider.parquet", lambda: insiders(m, s, filings, detail) if filings is not None
            else (_ for _ in ()).throw(RuntimeError("filings step failed"))),
        ("earnings_calendar.parquet", lambda: earnings_calendar(m, ns)),
        ("closes_official.parquet", lambda: official_closes(m, ns, uni)),
    ]:
        try:
            result = fn()
            if asset == "filings.parquet":
                filings = result
        except Exception as exc:  # noqa: BLE001
            m.record(asset, "failed", error=f"{type(exc).__name__}: {exc}")
    m.save_and_publish("weekly")
    log(f"weekly done; failures: {m.failures or 'none'}")
    return 1 if [f for f in m.failures if m.data["assets"][f]["status"] == "failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
