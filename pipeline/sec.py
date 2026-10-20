"""SEC EDGAR: tickers, filing index, XBRL financial statements and Form 4 insider trades.

Sources (all free, no key; fair access = a declared User-Agent and < 10 requests/second):
  company_tickers_exchange.json  ticker -> CIK -> exchange
  submissions.zip                every filer's filing history, rebuilt nightly ~3am ET
  companyfacts.zip               every filer's XBRL facts, rebuilt nightly ~3am ET
  Archives/edgar/data/...        individual Form 4 XML documents
"""
from __future__ import annotations

import datetime as dt
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from .common import RateLimiter, log, num

TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SUBMISSIONS_ZIP = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
FACTS_ZIP = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
ARCHIVES = "https://www.sec.gov/Archives/edgar/data"

# ------------------------------------------------------------------ tickers

def parse_tickers(payload: dict) -> pd.DataFrame:
    df = pd.DataFrame(payload["data"], columns=payload["fields"])
    df["cik"] = df["cik"].astype(int)
    return df[["cik", "name", "ticker", "exchange"]]


# ------------------------------------------------------------------ filing index

KEEP_FORMS = {
    "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "NT 10-K", "NT 10-K/A", "NT 10-Q",
    "NT 10-Q/A", "DEF 14A", "DEFA14A", "4", "4/A", "20-F", "20-F/A", "40-F", "40-F/A", "6-K",
    "SC 13D", "SC 13D/A", "SCHEDULE 13D", "SCHEDULE 13D/A",
}
CIK_FILE = re.compile(r"^CIK(\d{10})\.json$")  # main file only; skip CIK...-submissions-001.json


def parse_submission(obj: dict, since: dt.date) -> tuple[dict, list[dict]]:
    cik = int(obj["cik"])
    company = {
        "cik": cik,
        "name": obj.get("name"),
        "sic": str(obj.get("sic") or ""),
        "sic_description": obj.get("sicDescription"),
        "entity_type": obj.get("entityType"),
        "filer_category": obj.get("category"),
        "fiscal_year_end": obj.get("fiscalYearEnd"),
        "state_of_incorporation": obj.get("stateOfIncorporation"),
        "tickers": ",".join(t for t in (obj.get("tickers") or []) if t),
        "exchanges": ",".join(e for e in (obj.get("exchanges") or []) if e),
    }
    recent = (obj.get("filings") or {}).get("recent") or {}
    accs = recent.get("accessionNumber") or []
    n = len(accs)

    def col(name):
        v = recent.get(name) or []
        return v if len(v) == n else [None] * n

    forms, dates, docs = col("form"), col("filingDate"), col("primaryDocument")
    items, reports, accepted = col("items"), col("reportDate"), col("acceptanceDateTime")
    rows = []
    cutoff = since.isoformat()
    for i in range(n):
        form = forms[i]
        if form not in KEEP_FORMS or not dates[i] or dates[i] < cutoff:
            continue
        acc = accs[i]
        doc = docs[i] or ""
        rows.append({
            "cik": cik,
            "form": form,
            "filing_date": dates[i],
            "report_date": reports[i] or None,
            "accepted": accepted[i],
            "accession": acc,
            "items": items[i] or "",
            "primary_document": doc,
            "url": f"{ARCHIVES}/{cik}/{acc.replace('-', '')}/{doc}" if doc else
                   f"{ARCHIVES}/{cik}/{acc.replace('-', '')}/",
        })
    return company, rows


def read_submissions(zip_path, ciks: set[int], since: dt.date) -> tuple[pd.DataFrame, pd.DataFrame]:
    companies, filings = [], []
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            m = CIK_FILE.match(name)
            if not m or int(m.group(1)) not in ciks:
                continue
            try:
                company, rows = parse_submission(json.loads(z.read(name)), since)
            except Exception as exc:  # noqa: BLE001 - one bad file must not stop the run
                log(f"submissions: skipped {name}: {exc}")
                continue
            companies.append(company)
            filings.extend(rows)
    return pd.DataFrame(companies), pd.DataFrame(filings)


# ------------------------------------------------------------------ XBRL financials

FLOW, INSTANT = "flow", "instant"
# (concept, kind, unit, tags in priority order). A tag without a prefix is us-gaap.
CONCEPTS: list[tuple[str, str, str, list[str]]] = [
    ("revenue", FLOW, "USD", ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                              "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet",
                              "SalesRevenueGoodsNet"]),
    ("cost_of_revenue", FLOW, "USD", ["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold",
                                      "CostOfServices"]),
    ("gross_profit", FLOW, "USD", ["GrossProfit"]),
    ("operating_income", FLOW, "USD", ["OperatingIncomeLoss"]),
    ("pretax_income", FLOW, "USD", [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"]),
    ("income_tax", FLOW, "USD", ["IncomeTaxExpenseBenefit"]),
    ("interest_expense", FLOW, "USD", ["InterestExpense", "InterestExpenseNonoperating", "InterestExpenseDebt"]),
    ("net_income", FLOW, "USD", ["NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic", "ProfitLoss"]),
    ("eps_diluted", FLOW, "USD/shares", ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"]),
    ("shares_diluted", FLOW, "shares", ["WeightedAverageNumberOfDilutedSharesOutstanding",
                                        "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"]),
    ("d_and_a", FLOW, "USD", ["DepreciationDepletionAndAmortization", "DepreciationAmortizationAndAccretionNet",
                              "DepreciationAndAmortization", "Depreciation"]),
    ("cfo", FLOW, "USD", ["NetCashProvidedByUsedInOperatingActivities",
                          "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]),
    ("capex", FLOW, "USD", ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets",
                            "PaymentsForCapitalImprovements"]),
    ("sbc", FLOW, "USD", ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"]),
    ("acquisitions", FLOW, "USD", ["PaymentsToAcquireBusinessesNetOfCashAcquired"]),
    ("dividends_paid", FLOW, "USD", ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"]),
    ("buybacks", FLOW, "USD", ["PaymentsForRepurchaseOfCommonStock"]),
    ("total_assets", INSTANT, "USD", ["Assets"]),
    ("current_assets", INSTANT, "USD", ["AssetsCurrent"]),
    ("total_liabilities", INSTANT, "USD", ["Liabilities"]),
    ("current_liabilities", INSTANT, "USD", ["LiabilitiesCurrent"]),
    ("total_equity", INSTANT, "USD", ["StockholdersEquity",
                                      "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]),
    ("cash", INSTANT, "USD", ["CashAndCashEquivalentsAtCarryingValue",
                              "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "Cash"]),
    ("st_investments", INSTANT, "USD", ["ShortTermInvestments", "MarketableSecuritiesCurrent",
                                        "AvailableForSaleSecuritiesDebtSecuritiesCurrent"]),
    ("inventory", INSTANT, "USD", ["InventoryNet"]),
    ("receivables", INSTANT, "USD", ["AccountsReceivableNetCurrent"]),
    ("goodwill", INSTANT, "USD", ["Goodwill"]),
    ("intangibles", INSTANT, "USD", ["IntangibleAssetsNetExcludingGoodwill"]),
    # debt pieces are kept separately so total_debt can be assembled without double counting
    ("lt_debt_noncurrent", INSTANT, "USD", ["LongTermDebtNoncurrent", "LongTermDebtAndCapitalLeaseObligations"]),
    ("lt_debt_total", INSTANT, "USD", ["LongTermDebt"]),
    ("lt_debt_current", INSTANT, "USD", ["LongTermDebtCurrent", "LongTermDebtAndCapitalLeaseObligationsCurrent"]),
    ("debt_current", INSTANT, "USD", ["DebtCurrent"]),
    ("short_term_borrowings", INSTANT, "USD", ["ShortTermBorrowings"]),
    ("commercial_paper", INSTANT, "USD", ["CommercialPaper"]),
    ("shares_outstanding", INSTANT, "shares", ["dei:EntityCommonStockSharesOutstanding"]),
]
FACT_FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A", "10-KT", "10-KT/A", "20-F", "20-F/A", "40-F", "40-F/A"}
NON_ADDITIVE = {"shares_diluted"}  # weighted averages: never summed into a TTM
FACT_COLUMNS = ["cik", "concept", "prio", "tag", "start", "end", "months", "value", "filed", "form", "accn"]


def _months(start: str, end: str) -> int | None:
    days = (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days + 1
    for m in (3, 6, 9, 12):
        if abs(days - m * 30.4375) <= 20:
            return m
    return None


def parse_companyfacts(obj: dict, min_end: str) -> list[tuple]:
    cik = int(obj.get("cik"))
    facts = obj.get("facts") or {}
    out = []
    for concept, kind, unit, tags in CONCEPTS:
        for prio, full in enumerate(tags):
            ns, tag = full.split(":", 1) if ":" in full else ("us-gaap", full)
            node = (facts.get(ns) or {}).get(tag)
            if not node:
                continue
            for f in (node.get("units") or {}).get(unit, []):
                end, form = f.get("end"), f.get("form") or ""
                if not end or end < min_end or form not in FACT_FORMS or f.get("val") is None:
                    continue
                start = f.get("start")
                if kind == FLOW:
                    if not start:
                        continue
                    months = _months(start, end)
                    if months is None:
                        continue
                else:
                    if start:
                        continue
                    start, months = "", 0
                out.append((cik, concept, prio, tag, start, end, months, float(f["val"]),
                            f.get("filed"), form, f.get("accn")))
    return out


def dedupe_facts(rows: list[tuple]) -> pd.DataFrame:
    """One value per (company, concept, period): best tag, latest filing; keep the as-first-filed value too."""
    df = pd.DataFrame(rows, columns=FACT_COLUMNS)
    if df.empty:
        return df
    key = ["cik", "concept", "start", "end"]
    df = df[df["prio"] == df.groupby(key)["prio"].transform("min")]
    df = df.sort_values(["filed", "accn"])
    g = df.groupby(key, sort=False)
    first = g.head(1)[key + ["value", "filed"]].rename(columns={"value": "first_value", "filed": "first_filed"})
    latest = g.tail(1)
    out = latest.merge(first, on=key, how="left").drop(columns=["prio"])
    return out.reset_index(drop=True)


def trim_window(facts: pd.DataFrame, today: dt.date) -> pd.DataFrame:
    """Annual flows and instants: 11 years. Quarterly / year-to-date flows: 3 years."""
    long_cut = (today - dt.timedelta(days=int(365.25 * 11))).isoformat()
    short_cut = (today - dt.timedelta(days=int(365.25 * 3))).isoformat()
    keep = ((facts["months"].isin([0, 12]) & (facts["end"] >= long_cut)) |
            (facts["months"].isin([3, 6, 9]) & (facts["end"] >= short_cut)))
    return facts[keep].reset_index(drop=True)


def _total_debt(w: pd.DataFrame) -> pd.Series:
    def c(name):
        return w[name] if name in w else pd.Series(float("nan"), index=w.index)

    stb_cp = c("short_term_borrowings").fillna(0) + c("commercial_paper").fillna(0)
    current = c("debt_current").where(c("debt_current").notna(), c("lt_debt_current").fillna(0) + stb_cp)
    any_debt_tag = pd.concat([c(n).notna() for n in ("lt_debt_noncurrent", "lt_debt_total", "lt_debt_current",
                                                     "debt_current", "short_term_borrowings",
                                                     "commercial_paper")], axis=1).any(axis=1)
    total = c("lt_debt_noncurrent") + current
    total = total.where(c("lt_debt_noncurrent").notna(), c("lt_debt_total") + stb_cp)
    total = total.where(total.notna(), current)
    return total.where(any_debt_tag)  # NaN = nothing tagged: check the balance sheet, do not assume zero


def _add_derived(w: pd.DataFrame) -> pd.DataFrame:
    if "gross_profit" in w or ("revenue" in w and "cost_of_revenue" in w):
        gp = w["gross_profit"] if "gross_profit" in w else pd.Series(float("nan"), index=w.index)
        calc = (w["revenue"] - w["cost_of_revenue"]) if {"revenue", "cost_of_revenue"} <= set(w) else gp
        w["gross_profit_filled"] = gp.where(gp.notna(), calc)
    w["total_debt"] = _total_debt(w)
    if {"cfo", "capex"} <= set(w):
        w["fcf"] = w["cfo"] - w["capex"]
    return w


def build_annual(facts: pd.DataFrame) -> pd.DataFrame:
    """One row per company fiscal year: 12-month flows plus balance-sheet values at that year end."""
    fy = facts[(facts["months"] == 12) & facts["form"].str.match(r"^(10-K|20-F|40-F)")]
    if fy.empty:
        return pd.DataFrame()
    flows = fy.pivot_table(index=["cik", "end"], columns="concept", values="value", aggfunc="first")
    starts = fy.groupby(["cik", "end"])["start"].min().rename("start")
    inst = facts[facts["months"] == 0].pivot_table(index=["cik", "end"], columns="concept",
                                                  values="value", aggfunc="first")
    w = flows.join(starts).join(inst, how="left", rsuffix="_inst").reset_index()
    w = w.rename(columns={"end": "fiscal_year_end", "start": "fiscal_year_start"})
    return _add_derived(w)


def build_ttm(facts: pd.DataFrame) -> pd.DataFrame:
    """Latest trailing-twelve-month flows (last fiscal year + current YTD - prior-year YTD) and latest balance sheet."""
    out: dict[int, dict] = {}
    flows = facts[facts["months"] > 0]
    for (cik, concept), g in flows.groupby(["cik", "concept"], sort=False):
        rec = out.setdefault(cik, {"cik": cik})
        fy = g[g["months"] == 12].sort_values("end")
        if fy.empty:
            continue
        fy_end, fy_val = fy["end"].iloc[-1], fy["value"].iloc[-1]
        later = g[(g["months"].isin([3, 6, 9])) & (g["end"] > fy_end)].sort_values(["end", "months"])
        if concept in NON_ADDITIVE:
            latest = g.sort_values(["end", "months"]).iloc[-1]
            rec[concept] = latest["value"]
            continue
        value, end = fy_val, fy_end
        if not later.empty:
            cur = later.iloc[-1]
            target = dt.date.fromisoformat(cur["end"]) - dt.timedelta(days=365)
            prior = g[(g["months"] == cur["months"])].copy()
            prior["gap"] = prior["end"].map(lambda e: abs((dt.date.fromisoformat(e) - target).days))
            prior = prior[prior["gap"] <= 20].sort_values("gap")
            if not prior.empty:
                value = fy_val + cur["value"] - prior["value"].iloc[0]
                end = cur["end"]
        rec[concept] = value
        if concept == "net_income":
            rec["ttm_end"] = end
            rec["last_fy_end"] = fy_end
    inst = facts[facts["months"] == 0].sort_values("end")
    for (cik, concept), g in inst.groupby(["cik", "concept"], sort=False):
        rec = out.setdefault(cik, {"cik": cik})
        rec[concept] = g["value"].iloc[-1]
        if concept == "total_assets":
            rec["balance_sheet_end"] = g["end"].iloc[-1]
        if concept == "shares_outstanding":
            rec["shares_outstanding_date"] = g["end"].iloc[-1]
    return _add_derived(pd.DataFrame(list(out.values())))


def read_companyfacts(zip_path, ciks: set[int], today: dt.date) -> pd.DataFrame:
    min_end = (today - dt.timedelta(days=int(365.25 * 12))).isoformat()
    rows: list[tuple] = []
    seen = 0
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            m = CIK_FILE.match(name)
            if not m or int(m.group(1)) not in ciks:
                continue
            try:
                rows.extend(parse_companyfacts(json.loads(z.read(name)), min_end))
                seen += 1
            except Exception as exc:  # noqa: BLE001
                log(f"companyfacts: skipped {name}: {exc}")
    log(f"companyfacts: parsed {seen} companies, {len(rows)} raw facts")
    return trim_window(dedupe_facts(rows), today)


# ------------------------------------------------------------------ Form 4 insider trades

def _text(el, path):
    x = el.find(path)
    return x.text.strip() if x is not None and x.text and x.text.strip() else None


def _flag(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes")


def raw_form4_document(primary_document: str) -> str:
    # 'xslF345X05/form4.xml' is the rendered view; the raw XML has the same name one level up
    return primary_document.split("/", 1)[1] if primary_document.startswith("xsl") and "/" in primary_document \
        else primary_document


def parse_form4(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    owners = root.findall("reportingOwner")
    names = "; ".join(filter(None, (_text(o, "reportingOwnerId/rptOwnerName") for o in owners)))
    rel = lambda tag: any(_flag(_text(o, f"reportingOwnerRelationship/{tag}")) for o in owners)  # noqa: E731
    titles = "; ".join(filter(None, (_text(o, "reportingOwnerRelationship/officerTitle") for o in owners)))
    base = {
        "issuer_cik": int(_text(root, "issuer/issuerCik") or 0),
        "symbol": _text(root, "issuer/issuerTradingSymbol"),
        "owner_names": names,
        "is_director": rel("isDirector"),
        "is_officer": rel("isOfficer"),
        "is_ten_pct_owner": rel("isTenPercentOwner"),
        "officer_title": titles or None,
        "plan_10b5_1": _flag(_text(root, "aff10b5One")),
    }
    rows = []
    for tx in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        rows.append({
            **base,
            "security": _text(tx, "securityTitle/value"),
            "transaction_date": _text(tx, "transactionDate/value"),
            "code": _text(tx, "transactionCoding/transactionCode"),
            "shares": num(_text(tx, "transactionAmounts/transactionShares/value")),
            "price": num(_text(tx, "transactionAmounts/transactionPricePerShare/value")),
            "acquired_disposed": _text(tx, "transactionAmounts/transactionAcquiredDisposedCode/value"),
            "shares_after": num(_text(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")),
            "direct_indirect": _text(tx, "ownershipNature/directOrIndirectOwnership/value"),
        })
    return rows


def fetch_form4s(session, filings: pd.DataFrame, limit: int, per_second: float = 8.0) -> tuple[pd.DataFrame, int]:
    """Fetch and parse Form 4 XML for the given filing rows. Returns (rows, failures)."""
    limiter = RateLimiter(per_second)
    todo = filings.head(limit)

    def one(row):
        doc = raw_form4_document(row.primary_document)
        url = f"{ARCHIVES}/{row.cik}/{row.accession.replace('-', '')}/{doc}"
        limiter.wait()
        r = session.get(url, timeout=30)
        r.raise_for_status()
        parsed = parse_form4(r.text)
        for p in parsed:
            p.update({"accession": row.accession, "filing_date": row.filing_date, "url": url})
        if not parsed:  # holdings-only or derivative-only filing: remember it so it is not refetched
            parsed = [{"issuer_cik": int(row.cik), "accession": row.accession, "filing_date": row.filing_date,
                       "url": url, "code": None}]
        return parsed

    out, failures = [], 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(one, r) for r in todo.itertuples(index=False)]
        for f in as_completed(futures):
            try:
                out.extend(f.result())
            except Exception as exc:  # noqa: BLE001
                failures += 1
                if failures <= 20:
                    log(f"form4 failed: {exc}")
    return pd.DataFrame(out), failures
