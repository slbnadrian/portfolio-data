"""Shared helpers: HTTP sessions, rate limiting, release assets and the manifest.

Every published file is a GitHub Release asset on the tag in RELEASE_TAG (default
"data"). Set LOCAL_ONLY=1 to skip all release operations when testing on a laptop.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import pathlib
import subprocess
import threading
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORK = pathlib.Path(os.environ.get("WORK_DIR", str(ROOT / "work")))
WORK.mkdir(parents=True, exist_ok=True)
TAG = os.environ.get("RELEASE_TAG", "data")
LOCAL = os.environ.get("LOCAL_ONLY") == "1"


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def log(msg: str) -> None:
    print(f"[{utcnow().isoformat()}] {msg}", flush=True)


# ---------------------------------------------------------------- HTTP

def _session(headers: dict) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=2.0,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=16))
    s.headers.update(headers)
    return s


def nasdaq_session() -> requests.Session:
    # api.nasdaq.com rejects requests that do not look like its own website.
    return _session({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    })


def sec_session() -> requests.Session:
    # SEC fair-access policy: declare who you are in the User-Agent, stay under 10 req/s.
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if "@" not in ua:
        raise RuntimeError(
            "SEC_USER_AGENT is not set. Add a repository variable named SEC_USER_AGENT "
            "with your name and email, e.g. 'Jane Doe jane@example.com'."
        )
    return _session({"User-Agent": ua, "Accept-Encoding": "gzip, deflate"})


def plain_session() -> requests.Session:
    return _session({"User-Agent": "portfolio-data/1.0"})


class RateLimiter:
    """At most `per_second` calls per second across threads."""

    def __init__(self, per_second: float):
        self.interval = 1.0 / per_second
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.next_at:
                time.sleep(self.next_at - now)
                now = time.monotonic()
            self.next_at = now + self.interval


def download_file(session: requests.Session, url: str, dest: pathlib.Path,
                  attempts: int = 4) -> pathlib.Path:
    """Stream a large file to disk, retrying from scratch on a broken transfer."""
    last = None
    for i in range(attempts):
        try:
            with session.get(url, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
                tmp.replace(dest)
                return dest
        except Exception as exc:  # noqa: BLE001 - retry any transfer failure
            last = exc
            log(f"download failed ({i + 1}/{attempts}) {url}: {exc}")
            time.sleep(30 * (i + 1))
    raise RuntimeError(f"could not download {url}: {last}")


def num(x) -> float:
    """'$1,234.50' / '12.3%' / 'NA' / '' -> float or NaN."""
    if x is None:
        return math.nan
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("$", "").replace(",", "").replace("%", "")
    if s in ("", "NA", "N/A", "--", "-", "None", "null"):
        return math.nan
    try:
        return float(s)
    except ValueError:
        return math.nan


def norm_symbol(sym: str) -> str:
    """Common key for tickers across sources: BRK/B, BRK.B and BRK-B -> BRK-B."""
    return str(sym).strip().upper().replace("/", "-").replace(".", "-").replace("^", "-")


# ---------------------------------------------------------------- release assets

def gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], check=check, capture_output=True, text=True)


def ensure_release() -> None:
    if LOCAL:
        return
    if gh("release", "view", TAG, check=False).returncode != 0:
        gh("release", "create", TAG, "--title", "Portfolio data",
           "--notes", "Published by the pipeline. See manifest.json for build times and checks.")


def fetch_asset(name: str) -> pathlib.Path | None:
    """Download the current copy of a release asset, if there is one."""
    path = WORK / name
    if LOCAL:
        return path if path.exists() else None
    r = gh("release", "download", TAG, "-p", name, "-D", str(WORK), "--clobber", check=False)
    return path if r.returncode == 0 and path.exists() else None


def publish(paths: list[pathlib.Path]) -> None:
    if LOCAL or not paths:
        return
    gh("release", "upload", TAG, *[str(p) for p in paths], "--clobber")


def write_parquet(df: pd.DataFrame, name: str) -> pathlib.Path:
    path = WORK / name
    df.to_parquet(path, index=False, compression="zstd")
    return path


def read_parquet(name: str) -> pd.DataFrame | None:
    path = fetch_asset(name)
    return pd.read_parquet(path) if path else None


# ---------------------------------------------------------------- manifest

class Manifest:
    """manifest.json: one entry per asset with build time, rows, status and checks."""

    def __init__(self) -> None:
        path = fetch_asset("manifest.json")
        self.data = json.loads(path.read_text()) if path else {"assets": {}}
        self.data.setdefault("assets", {})
        self.failures: list[str] = []

    def record(self, asset: str, status: str, rows: int | None = None, **extra) -> None:
        entry = {"built_utc": utcnow().isoformat(), "status": status, "rows": rows}
        entry.update({k: v for k, v in extra.items() if v is not None})
        self.data["assets"][asset] = entry
        if status != "ok":
            self.failures.append(asset)
        log(f"{asset}: {status} rows={rows} {extra if extra else ''}")

    def save_and_publish(self, job: str) -> pathlib.Path:
        self.data[f"last_{job}_run_utc"] = utcnow().isoformat()
        text = json.dumps(self.data, indent=2, sort_keys=True, default=str)
        path = WORK / "manifest.json"
        path.write_text(text)
        if not LOCAL:  # the repo copy is committed by the weekly workflow; tests must not touch it
            (ROOT / "data").mkdir(exist_ok=True)
            (ROOT / "data" / "manifest.json").write_text(text)
        publish([path])
        return path


def upsert(old: pd.DataFrame | None, new: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Append new rows, replacing any old row with the same key."""
    if old is None or old.empty:
        out = new.copy()
    else:
        out = pd.concat([old, new], ignore_index=True)
    return out.drop_duplicates(subset=keys, keep="last").sort_values(keys).reset_index(drop=True)
