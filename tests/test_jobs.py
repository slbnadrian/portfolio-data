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
    check_close = {"AAPL": "$255.46", "MSFT": "$255.46", "KO": "$93.90"}   # the screener fixture's last sales

    def fake_historical(s, sym, ac, a, b, label=None):
        label = label or sym.upper()
        if label in check_close:
            row = {"date": "10/17/2026", "close": check_close[label], "volume": "1", "open": "$1", "high": "$1",
                   "low": "$1"}
            return nq.parse_historical({"data": {"tradesTable": {"rows": [row]}}}, label)
        return nq.parse_historical(HISTORICAL, label)

    monkeypatch.setattr(nq, "historical", fake_historical)
    many = {"chart": {"result": [{"events": {"dividends": {
        str(i): {"amount": 1.5 + i / 100, "date": 1600000000 + i * 7_900_000} for i in range(10)}}}]}}
    monkeypatch.setattr(yh, "dividends", lambda s, sym="SPY": yh.parse_dividends(many, sym))
    monkeypatch.setattr(nq, "earnings_on", lambda s, day: nq.parse_earnings(EARNINGS, day))
    monkeypatch.setattr(ust, "ten_year", lambda s, years: ust.parse_year(TREASURY_CSV))

    assert bd.main() == 0
    prices = pd.read_parquet(tmp_path / "prices_daily.parquet")
    assert set(prices["date"]) == {dt.date(2026, 10, 17)}          # dated by the session its closes match
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


def test_snapshot_session_dates_by_matching_close(monkeypatch):
    import pipeline.build_daily as bd
    import pipeline.nasdaq as nq
    closes = {"AAPL": [("10/17/2026", "$257.10"), ("10/16/2026", "$255.46")],
              "MSFT": [("10/17/2026", "$512.00"), ("10/16/2026", "$510.00")],
              "KO": [("10/17/2026", "$94.20"), ("10/16/2026", "$93.90")]}

    def fake_hist(s, sym, ac, a, b, label=None):
        rows = [{"date": d, "close": c, "volume": "1", "open": c, "high": c, "low": c} for d, c in closes[label]]
        return nq.parse_historical({"data": {"tradesTable": {"rows": rows}}}, label)

    monkeypatch.setattr(nq, "historical", fake_hist)
    snap = pd.DataFrame({"symbol": ["AAPL", "MSFT", "KO"], "last_sale": [255.46, 510.00, 93.90]})
    day, source = bd.snapshot_session(snap, None, dt.date(2026, 10, 17))
    assert day == dt.date(2026, 10, 16) and "3/3" in source        # a lagging snapshot gets its own date
    snap["last_sale"] = [257.10, 512.00, 1.0]
    assert bd.snapshot_session(snap, None, dt.date(2026, 10, 17))[0] == dt.date(2026, 10, 17)
    snap["last_sale"] = [1.0, 2.0, 3.0]
    assert bd.snapshot_session(snap, None, dt.date(2026, 10, 17)) is None
