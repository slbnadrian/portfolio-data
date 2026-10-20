# portfolio-data

Free, automated market data for a personal long-only equity model portfolio. Two GitHub
Actions jobs download public data from the SEC, Nasdaq and FRED and publish it as Parquet
files on the `data` release. The portfolio's analysis runs elsewhere and downloads these files.

This repository holds **public market data only** — never holdings, trades or account details.

## Setup (once)

1. **Settings → Secrets and variables → Actions → Secrets → New repository secret**
   - Name: `SEC_USER_AGENT`
   - Value: your name and email, e.g. `Jane Doe jane@example.com`
   The SEC requires automated users to identify themselves this way. It is used only in
   requests to sec.gov. Store it as a **secret**, not a variable: GitHub prints variables in
   job logs, and in a public repository anyone can read those logs.
2. **Actions tab → enable workflows** if GitHub asks.
3. Run **daily** once, then **weekly** (Actions → choose workflow → *Run workflow*). The weekly
   job needs the daily job's universe snapshot.

## Schedule

| Job | When (UTC) | Publishes |
|---|---|---|
| `daily` | 01:23 Tue–Sat (US weekday evenings) | `spy_daily`, `prices_daily`, `spy_dividends`, `treasury_10y` |
| `weekly` | 09:17 Saturday (after the SEC's ~3am ET bulk rebuild) | `sec_tickers`, `companies`, `filings`, `facts`, `annual`, `ttm`, `insider`, `earnings_calendar`, `closes_official` |

The weekly job commits `data/manifest.json`, which keeps GitHub from switching off the
schedules after 60 days without a commit.

## Files

Download any file from `https://github.com/<owner>/<repo>/releases/download/data/<file>`.

| File | Grain | Key columns | Source |
|---|---|---|---|
| `manifest.json` | one entry per file | `built_utc`, `status` (ok / partial / failed), `rows`, checks, errors | this pipeline |
| `prices_daily.parquet` | symbol × trading day, every US stock ≥ $250M market cap | `date`, `symbol`, `key`, `api_symbol`, `last_sale`, `change_1d`, `volume`, `market_cap`, `sector`, `industry`, `country` | Nasdaq screener |
| `closes_official.parquet` | symbol × day, ≥ $1B market cap, refreshed weekly (last 3 weeks each run, 5 years kept) | `symbol`, `key`, `date`, `open`, `high`, `low`, `close`, `volume` | Nasdaq historical |
| `spy_daily.parquet` | SPY × day since 2015 | `date`, `open`, `high`, `low`, `close`, `volume` | Nasdaq historical |
| `spy_dividends.parquet` | SPY distributions | `symbol`, `ex_date`, `amount` | Yahoo Finance chart events (Nasdaq's dividend history covers Nasdaq-listed securities only) |
| `treasury_10y.parquet` | day | `date`, `value_pct`, `series` | US Treasury daily par yield curve (home.treasury.gov); FRED `DGS10` as fallback |
| `sec_tickers.parquet` | ticker | `cik`, `name`, `ticker`, `exchange`, `key` | SEC |
| `companies.parquet` | company | `cik`, `name`, `sic`, `sic_description`, `entity_type`, `filer_category`, `fiscal_year_end`, `tickers` | SEC submissions |
| `filings.parquet` | filing, last 3 years | `cik`, `form`, `filing_date`, `report_date`, `accession`, `items` (8-K item numbers), `url` | SEC submissions |
| `facts.parquet` | company × concept × period | `cik`, `concept`, `tag`, `start`, `end`, `months` (0 = balance sheet, 3/6/9 = year-to-date, 12 = fiscal year), `value`, `filed`, `form`, `accn`, `first_value`, `first_filed` | SEC XBRL |
| `annual.parquet` | company × fiscal year (11 years) | `fiscal_year_end`, every concept, plus `gross_profit_filled`, `total_debt`, `fcf` | derived from `facts` |
| `ttm.parquet` | company | trailing-twelve-month flows, latest balance sheet, `ttm_end`, `balance_sheet_end`, `total_debt`, `fcf` | derived from `facts` |
| `insider.parquet` | Form 4 transaction, last 3 years, ≥ $1B issuers | `issuer_cik`, `symbol`, `owner_names`, `is_director`, `is_officer`, `officer_title`, `code` (P = open-market buy), `shares`, `price`, `plan_10b5_1`, `filing_date`, `url` | SEC Form 4 |
| `earnings_calendar.parquet` | symbol × report date, next 6 weeks | `date`, `symbol`, `key`, `time`, `fiscal_quarter_ending`, `eps_forecast` | Nasdaq |

### Notes that matter when using the data

- **`last_sale` is not the official close.** It is Nasdaq's last trade when the job ran and can
  include after-hours trades. Use `closes_official` or `spy_daily` for any price a decision or a
  valuation depends on.
- **`key`** is the ticker normalised across sources (`BRK/B`, `BRK.B` and `BRK-B` all become `BRK-B`).
- **Financial concepts** come from several XBRL tags tried in a fixed priority order, chosen per
  period; `tag` records which one was used. `value` is the latest filed figure (restatements win);
  `first_value` is the figure as first reported.
- **`total_debt` is blank when a company tags no debt at all.** Blank means "check the balance
  sheet", not zero. It is non-current long-term debt + current debt (or current long-term debt +
  the larger of short-term borrowings and commercial paper, since paper is usually inside
  borrowings). Leases are excluded. Short-term notes tagged with an unusual element are missed —
  Coca-Cola's come out 0.1-0.8% light — so confirm debt against the filing before relying on it.
- **Trailing twelve months** = last fiscal year + current year-to-date − prior-year
  year-to-date. A company with no newer quarter falls back to the fiscal year.
- **Gaps:** foreign filers reporting under IFRS (20-F, 40-F) and US filers reporting in another
  currency (only USD facts are parsed) have no financials here; there are no
  analyst consensus estimates beyond Nasdaq's next-quarter EPS forecast.

## Tests

```bash
pip install -r requirements.txt
python -m pytest -q tests
```

The tests run offline against payloads in each source's format, including an end-to-end run of
both jobs with the network stubbed. Both workflows run them before building.

## Data terms

SEC EDGAR data is public; this pipeline follows the SEC's fair-access policy (a declared
User-Agent, under 10 requests per second, bulk files instead of per-company calls where possible).
Nasdaq, Yahoo Finance, Treasury and FRED data are used for personal, non-commercial purposes at a
few requests per day.

## Diagnosing a broken source, and reading filings

The `probe` workflow (Actions → probe → Run workflow) fetches any space-separated URLs from GitHub's
network and publishes the raw responses as `probe.json` on the `data` release, keeping the first
`max_bytes` of each (default 20,000; up to 25,000,000, enough for a full 10-K or proxy). It is also
how filings are read from a session that cannot reach sec.gov: a filing folder's `FilingSummary.xml`
lists its rendered statement pages (`R2.htm`, `R3.htm`, …), and the main document holds the text.
