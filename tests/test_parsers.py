"""Offline tests: each parser against a payload in the source's own format."""
import datetime as dt
import io
import json
import math
import zipfile

import pandas as pd
import pytest

from pipeline import fred, nasdaq, sec
from pipeline.common import norm_symbol, num, upsert

# ---------------------------------------------------------------- helpers

def test_num_and_symbols():
    assert num("$1,234.50") == 1234.5
    assert num("-0.48%") == -0.48
    assert math.isnan(num("NA")) and math.isnan(num("")) and math.isnan(num(None))
    assert norm_symbol("BRK/B") == norm_symbol("brk.b") == norm_symbol("BRK-B") == "BRK-B"


def test_upsert_replaces_same_key():
    old = pd.DataFrame({"date": [1, 2], "v": [10, 20]})
    new = pd.DataFrame({"date": [2, 3], "v": [21, 30]})
    out = upsert(old, new, ["date"])
    assert out["v"].tolist() == [10, 21, 30]

# ---------------------------------------------------------------- Nasdaq

SCREENER = {"data": {"headers": {}, "rows": [
    {"symbol": "AAPL", "name": "Apple Inc. Common Stock", "lastsale": "$255.46", "netchange": "-1.23",
     "pctchange": "-0.479%", "volume": "40211234", "marketCap": "3,790,000,000,000.00",
     "country": "United States", "ipoyear": "1980", "industry": "Computer Manufacturing",
     "sector": "Technology", "url": "/market-activity/stocks/aapl"},
    {"symbol": "BRK/B", "name": "Berkshire Hathaway Inc.", "lastsale": "$532.02", "netchange": "1.0",
     "pctchange": "0.19%", "volume": "4081654", "marketCap": "1173801775599.0", "country": "United States",
     "ipoyear": "", "industry": "", "sector": "", "url": "/market-activity/stocks/brk.b"},
    {"symbol": "TINY", "name": "Tiny Corp", "lastsale": "$1.02", "netchange": "0", "pctchange": "0%",
     "volume": "100", "marketCap": "", "country": "", "ipoyear": "", "industry": "", "sector": "", "url": ""},
]}}


def test_screener():
    df = nasdaq.parse_screener(SCREENER)
    aapl = df[df.symbol == "AAPL"].iloc[0]
    assert aapl.last_sale == 255.46 and aapl.market_cap == 3.79e12
    assert aapl.change_1d == pytest.approx(-0.00479)
    assert df[df.symbol == "BRK/B"].iloc[0].api_symbol == "brk.b"
    assert df[df.symbol == "TINY"].iloc[0].api_symbol == "tiny"
    assert math.isnan(df[df.symbol == "TINY"].iloc[0].market_cap)


HISTORICAL = {"data": {"symbol": "SPY", "totalRecords": 2, "tradesTable": {"asOf": None, "headers": {}, "rows": [
    {"date": "10/17/2026", "close": "$664.39", "volume": "96,500,868", "open": "$660.10", "high": "$665.00",
     "low": "$659.80"},
    {"date": "10/16/2026", "close": "$660.64", "volume": "90,000,000", "open": "$662.00", "high": "$663.10",
     "low": "$658.00"}]}}}


def test_historical():
    df = nasdaq.parse_historical(HISTORICAL, "SPY")
    assert df.iloc[0].date == dt.date(2026, 10, 17) and df.iloc[0].close == 664.39
    assert df.iloc[0].volume == 96500868
    assert nasdaq.parse_historical({"data": None}, "X").empty


# shape observed from GitHub's runners on 2026-10-20 (probe workflow)
YAHOO_DIVS = {"chart": {"result": [{"meta": {"symbol": "SPY"}, "timestamp": [1756699200, 1764565200],
    "events": {"dividends": {
        "1764565200": {"amount": 1.993, "date": 1766154600},
        "1756699200": {"amount": 1.831, "date": 1758288600}}}}], "error": None}}


def test_yahoo_dividends():
    from pipeline import yahoo
    df = yahoo.parse_dividends(YAHOO_DIVS, "SPY")
    assert df.ex_date.tolist() == [dt.date(2025, 9, 19), dt.date(2025, 12, 19)]   # New York dates, sorted
    assert df.amount.tolist() == [1.831, 1.993]
    assert yahoo.parse_dividends({"chart": {"result": None}}, "SPY").empty


TREASURY_CSV = ('Date,"1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr","3 Yr","5 Yr","7 Yr",'
                '"10 Yr","20 Yr","30 Yr"\n'
                "10/19/2026,4.11,4.14,4.17,4.27,4.34,4.37,4.50,4.77,4.85,4.94,5.04,5.15,5.53,5.48\n"
                "10/16/2026,4.12,4.14,4.17,4.26,4.32,4.34,4.46,4.74,4.82,4.89,4.98,5.09,5.47,5.43\n")


def test_treasury():
    from pipeline import treasury
    df = treasury.parse_year(TREASURY_CSV)
    assert df.iloc[0].date == dt.date(2026, 10, 19) and df.iloc[0].value_pct == 5.15
    assert treasury.years_to_fetch(True, dt.date(2026, 10, 20)) == [2025, 2026]
    assert len(treasury.years_to_fetch(False, dt.date(2026, 10, 20))) == 11


EARNINGS = {"data": {"asOf": "Tue, Oct 20, 2026", "headers": {}, "rows": [
    {"lastYearRptDt": "10/22/2025", "lastYearEPS": "$0.97", "time": "time-after-hours", "symbol": "KO",
     "name": "Coca-Cola", "marketCap": "$404,009,373,918", "fiscalQuarterEnding": "Sep/2026",
     "epsForecast": "$0.82", "noOfEsts": "11"}]}}


def test_earnings():
    df = nasdaq.parse_earnings(EARNINGS, dt.date(2026, 10, 21))
    assert df.iloc[0].symbol == "KO" and df.iloc[0].eps_forecast == 0.82

# ---------------------------------------------------------------- FRED

def test_fred_both_header_styles():
    for header in ("observation_date", "DATE"):
        df = fred.parse_series(f"{header},DGS10\n2026-10-15,4.01\n2026-10-16,.\n2026-10-17,4.05\n", "DGS10")
        assert df["value_pct"].tolist() == [4.01, 4.05]
        assert df.iloc[-1].date == dt.date(2026, 10, 17)

# ---------------------------------------------------------------- SEC tickers and submissions

def test_tickers():
    payload = {"fields": ["cik", "name", "ticker", "exchange"],
               "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"], [1067983, "BERKSHIRE HATHAWAY INC", "BRK-B", "NYSE"]]}
    df = sec.parse_tickers(payload)
    assert df.cik.tolist() == [320193, 1067983]


SUBMISSION = {
    "cik": "0000320193", "entityType": "operating", "sic": "3571", "sicDescription": "Electronic Computers",
    "name": "Apple Inc.", "tickers": ["AAPL"], "exchanges": ["Nasdaq"], "fiscalYearEnd": "0927",
    "category": "Large accelerated filer", "stateOfIncorporation": "CA",
    "filings": {"recent": {
        "accessionNumber": ["0000320193-26-000101", "0001140361-26-000222", "0000320193-26-000090",
                            "0000320193-20-000001", "0000320193-26-000080"],
        "filingDate": ["2026-10-30", "2026-10-10", "2026-08-01", "2020-01-01", "2026-07-15"],
        "reportDate": ["2026-09-26", "2026-10-08", "2026-06-27", "2019-12-28", "2026-07-14"],
        "acceptanceDateTime": ["", "", "", "", ""],
        "form": ["10-K", "4", "10-Q", "10-Q", "8-K"],
        "items": ["", "", "", "", "4.01,9.01"],
        "primaryDocument": ["aapl-20260926.htm", "xslF345X05/wk-form4_1.xml", "aapl-20260627.htm",
                            "a.htm", "d8k.htm"],
    }, "files": []},
}


def test_submission_parse_and_zip():
    company, rows = sec.parse_submission(SUBMISSION, dt.date(2023, 10, 20))
    assert company["sic"] == "3571" and company["tickers"] == "AAPL"
    assert [r["form"] for r in rows] == ["10-K", "4", "10-Q", "8-K"]  # the 2020 10-Q is outside the window
    eightk = [r for r in rows if r["form"] == "8-K"][0]
    assert eightk["items"] == "4.01,9.01"
    assert rows[0]["url"] == "https://www.sec.gov/Archives/edgar/data/320193/000032019326000101/aapl-20260926.htm"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("CIK0000320193.json", json.dumps(SUBMISSION))
        z.writestr("CIK0000320193-submissions-001.json", json.dumps({"ignored": True}))
        z.writestr("CIK0000000001.json", json.dumps({**SUBMISSION, "cik": "0000000001"}))
    buf.seek(0)
    companies, filings = sec.read_submissions(buf, {320193}, dt.date(2023, 10, 20))
    assert companies.cik.tolist() == [320193] and len(filings) == 4

# ---------------------------------------------------------------- SEC companyfacts

def fact(val, end, filed, form, start=None, fy=2026, fp="FY", accn="a"):
    f = {"end": end, "val": val, "accn": accn, "fy": fy, "fp": fp, "form": form, "filed": filed}
    if start:
        f["start"] = start
    return f


FACTS = {"cik": 320193, "entityName": "Apple Inc.", "facts": {
    "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
        fact(15_000_000_000, "2026-10-17", "2026-10-30", "10-K")]}}},
    "us-gaap": {
        # FY2025 (Oct-24 to Sep-25) and FY2026, plus 6-month YTD for FY2027 and its prior-year match
        "NetIncomeLoss": {"units": {"USD": [
            fact(90e9, "2025-09-27", "2025-10-31", "10-K", "2024-09-29"),
            fact(100e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28"),
            fact(91e9, "2025-09-27", "2026-10-30", "10-K", "2024-09-29"),          # restated in a later 10-K
            fact(30e9, "2025-12-27", "2026-01-30", "10-Q", "2025-09-28"),          # Q1 FY26 (3 months)
            fact(55e9, "2026-03-28", "2026-05-01", "10-Q", "2025-09-28"),          # 6-month YTD FY26
            fact(35e9, "2026-12-26", "2027-01-29", "10-Q", "2026-09-27"),          # Q1 FY27 (3 months)
        ]}},
        # Revenues missing for FY2025, so the next-priority tag fills that year
        "Revenues": {"units": {"USD": [fact(400e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28")]}},
        "RevenueFromContractWithCustomerExcludingAssessedTax": {"units": {"USD": [
            fact(380e9, "2025-09-27", "2025-10-31", "10-K", "2024-09-29"),
            fact(999e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28")]}},   # lower priority: ignored
        "CostOfRevenue": {"units": {"USD": [fact(220e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28")]}},
        "Assets": {"units": {"USD": [fact(360e9, "2026-09-26", "2026-10-30", "10-K"),
                                     fact(350e9, "2025-09-27", "2025-10-31", "10-K"),
                                     fact(365e9, "2026-12-26", "2027-01-29", "10-Q")]}},
        "StockholdersEquity": {"units": {"USD": [fact(70e9, "2026-09-26", "2026-10-30", "10-K")]}},
        "LongTermDebtNoncurrent": {"units": {"USD": [fact(85e9, "2026-09-26", "2026-10-30", "10-K")]}},
        "LongTermDebtCurrent": {"units": {"USD": [fact(10e9, "2026-09-26", "2026-10-30", "10-K")]}},
        "CommercialPaper": {"units": {"USD": [fact(5e9, "2026-09-26", "2026-10-30", "10-K")]}},
        "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": [
            fact(120e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28")]}},
        "PaymentsToAcquirePropertyPlantAndEquipment": {"units": {"USD": [
            fact(12e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28")]}},
        "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": [
            fact(15.2e9, "2026-09-26", "2026-10-30", "10-K", "2025-09-28"),
            fact(15.0e9, "2026-12-26", "2027-01-29", "10-Q", "2026-09-27")]}},
        "OddPeriod": {"units": {"USD": [fact(1, "2026-09-26", "2026-10-30", "10-K", "2026-05-01")]}},
    }}}


def test_companyfacts_pipeline():
    rows = sec.parse_companyfacts(FACTS, "2014-01-01")
    facts = sec.trim_window(sec.dedupe_facts(rows), dt.date(2027, 2, 1))

    ni = facts[(facts.concept == "net_income") & (facts.months == 12)].sort_values("end")
    assert ni.value.tolist() == [91e9, 100e9]               # restatement wins ...
    assert ni.first_value.tolist() == [90e9, 100e9]         # ... and the as-first-filed value is kept

    rev = facts[(facts.concept == "revenue")].sort_values("end")
    assert rev.value.tolist() == [380e9, 400e9]             # per-period tag fallback, priority respected
    assert set(rev.tag) == {"RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"}

    annual = sec.build_annual(facts)
    fy26 = annual[annual.fiscal_year_end == "2026-09-26"].iloc[0]
    assert fy26.total_debt == 85e9 + 10e9 + 5e9
    assert fy26.gross_profit_filled == 400e9 - 220e9
    assert fy26.fcf == 108e9
    assert fy26.total_assets == 360e9

    ttm = sec.build_ttm(facts).iloc[0]
    # FY26 (100) + Q1 FY27 (35) - Q1 FY26 (30) = 105
    assert ttm.net_income == 105e9 and ttm.ttm_end == "2026-12-26"
    assert ttm.revenue == 400e9                             # no YTD revenue reported: falls back to FY
    assert ttm.total_assets == 365e9 and ttm.balance_sheet_end == "2026-12-26"
    assert ttm.shares_diluted == 15.0e9                     # latest weighted average, never summed
    assert ttm.shares_outstanding == 15_000_000_000


def test_total_debt_without_noncurrent_tag():
    w = pd.DataFrame({"lt_debt_total": [50.0], "short_term_borrowings": [5.0]})
    assert sec._total_debt(w).iloc[0] == 55.0
    none = pd.DataFrame({"revenue": [1.0]})
    assert math.isnan(sec._total_debt(none).iloc[0])         # nothing tagged is unknown, not zero

# ---------------------------------------------------------------- Form 4

FORM4 = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0508</schemaVersion>
  <documentType>4</documentType>
  <aff10b5One>0</aff10b5One>
  <issuer><issuerCik>0000021344</issuerCik><issuerName>COCA COLA CO</issuerName>
    <issuerTradingSymbol>KO</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0001</rptOwnerCik><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>0</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-08</value></transactionDate>
      <transactionCoding><transactionFormType>4</transactionFormType><transactionCode>P</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved></transactionCoding>
      <transactionAmounts><transactionShares><value>10000</value></transactionShares>
        <transactionPricePerShare><value>93.10</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>25000</value>
        </sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


def test_form4():
    rows = sec.parse_form4(FORM4)
    assert len(rows) == 1
    r = rows[0]
    assert (r["symbol"], r["code"], r["shares"], r["price"]) == ("KO", "P", 10000.0, 93.10)
    assert r["is_director"] and not r["is_officer"] and not r["plan_10b5_1"]
    assert r["issuer_cik"] == 21344
    assert sec.raw_form4_document("xslF345X05/wk-form4_1.xml") == "wk-form4_1.xml"
    assert sec.raw_form4_document("form4.xml") == "form4.xml"
