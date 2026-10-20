"""Offline end-to-end run of both jobs with every network call stubbed."""
import datetime as dt
import importlib
import json
import zipfile

import pandas as pd

from tests.test_parsers import EARNINGS, FACTS, FORM4, HISTORICAL, SCREENER, SUBMISSION, TREASURY_CSV


class FakeResponse:
    def __init__(self, payload=None, text=""):
        self.payload, self.text = payload, text

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSEC:
    def get(self, url, timeout=None, **_):
        if url.endswith("company_tickers_exchange.json"):
            return FakeResponse({"fields": ["cik", "name", "ticker", "exchange"],
                                 "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"],
                                          [21344, "COCA COLA CO", "KO", "NYSE"],
                                          [1067983, "BERKSHIRE HATHAWAY INC", "BRK-B", "NYSE"]]})
        return FakeResponse(text=FORM4)


def big_screener():
    rows = list(SCREENER["data"]["rows"])
    rows.append({**rows[0], "symbol": "MSFT", "url": "/market-activity/stocks/msft"})
    rows.append({**rows[0], "symbol": "KO", "lastsale": "$93.90", "marketCap": "404009373918",
                 "url": "/market-activity/stocks/ko"})
    filler = [{**rows[0], "symbol": f"F{i:04d}", "marketCap": "500000000", "url": ""} for i in range(3100)]
    return {"data": {"rows": rows + filler}}


def test_daily_and_weekly(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCAL_ONLY", "1")
    monkeypatch.setenv("WORK_DIR", str(tmp_path))
    monkeypatch.setenv("SEC_USER_AGENT", "Test User test@example.com")
    import pipeline.build_daily as bd
    import pipeline.build_weekly as bw
    import pipeline.common as common
    for mod in (common, bd, bw):
        importlib.reload(mod)
    import pipeline.nasdaq as nq
    import pipeline.treasury as ust
    import pipeline.yahoo as yh

    monkeypatch.setattr(nq, "screener_snapshot", lambda s: nq.parse_screener(big_screener()))
    monkeypatch.setattr(nq, "historical",
                        lambda s, sym, ac, a, b, label=None: nq.parse_historical(HISTORICAL, label or sym.upper()))
    many = {"chart": {"result": [{"events": {"dividends": {
        str(i): {"amount": 1.5 + i / 100, "date": 1600000000 + i * 7_900_000} for i in range(10)}}}]}}
    monkeypatch.setattr(yh, "dividends", lambda s, sym="SPY": yh.parse_dividends(many, sym))
    monkeypatch.setattr(nq, "earnings_on", lambda s, day: nq.parse_earnings(EARNINGS, day))
    monkeypatch.setattr(ust, "ten_year", lambda s, years: ust.parse_year(TREASURY_CSV))

    assert bd.main() == 0
    prices = pd.read_parquet(tmp_path / "prices_daily.parquet")
    assert set(prices["date"]) == {dt.date(2026, 10, 17)}          # dated by SPY's latest session
    assert "TINY" not in set(prices["symbol"])                     # below the $250M floor

    # bulk SEC files, built from the fixtures
    def fake_download(session, url, dest, attempts=4):
        with zipfile.ZipFile(dest, "w") as z:
            if "submissions" in url:
                sub = json.loads(json.dumps(SUBMISSION))
                sub["filings"]["recent"]["filingDate"][1] = (dt.date.today() - dt.timedelta(days=5)).isoformat()
                z.writestr("CIK0000320193.json", json.dumps(sub))
            else:
                z.writestr("CIK0000320193.json", json.dumps(FACTS))
        return dest

    monkeypatch.setattr(bw, "download_file", fake_download)
    monkeypatch.setattr(bw, "sec_session", lambda: FakeSEC())
    assert bw.main() == 0
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    for asset in ("prices_daily.parquet", "spy_daily.parquet", "filings.parquet", "facts.parquet",
                  "annual.parquet", "ttm.parquet", "insider.parquet", "earnings_calendar.parquet",
                  "closes_official.parquet", "treasury_10y.parquet", "spy_dividends.parquet"):
        assert manifest["assets"][asset]["status"] in ("ok", "partial"), asset
    insider = pd.read_parquet(tmp_path / "insider.parquet")
    assert insider.iloc[0]["code"] == "P"
    assert bw.main() == 0                                          # second run: Form 4 already fetched
    assert len(pd.read_parquet(tmp_path / "insider.parquet")) == 1
