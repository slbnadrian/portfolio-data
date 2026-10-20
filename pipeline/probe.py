"""Diagnostic job: fetch URLs from GitHub's network and publish the raw responses as probe.json.

Used to see exactly what a source returns when a parser breaks; the analysis workspace cannot
reach these hosts itself. Run it from the Actions tab (workflow "probe") with space-separated URLs.
"""
from __future__ import annotations

import json
import os
import sys

import requests

from .common import WORK, ensure_release, nasdaq_session, publish, utcnow

HEAD_BYTES = 20_000


def headers_for(url: str) -> dict:
    if "nasdaq.com" in url:
        return dict(nasdaq_session().headers)
    if "sec.gov" in url:
        return {"User-Agent": os.environ.get("SEC_USER_AGENT", "").strip() or "missing-user-agent"}
    return {"User-Agent": "portfolio-data/1.0"}


def probe(url: str) -> dict:
    rec: dict = {"url": url}
    try:
        with requests.get(url, headers=headers_for(url), timeout=(20, 40), stream=True) as r:
            rec["status"] = r.status_code
            rec["headers"] = {k: v for k, v in r.headers.items()
                              if k.lower() in ("content-type", "content-length", "last-modified", "date")}
            body = b""
            for chunk in r.iter_content(8192):
                body += chunk
                if len(body) >= HEAD_BYTES:
                    rec["truncated"] = True
                    break
        rec["body_head"] = body[:HEAD_BYTES].decode("utf-8", "replace")
    except Exception as exc:  # noqa: BLE001 - the point is to record the failure
        rec["error"] = f"{type(exc).__name__}: {exc}"
    return rec


def main() -> int:
    urls = [u for u in os.environ.get("PROBE_URLS", "").split() if u]
    if not urls:
        print("no PROBE_URLS given")
        return 1
    results = [probe(u) for u in urls]
    for r in results:
        print(r["url"], r.get("status"), r.get("error", ""), len(r.get("body_head", "")))
    path = WORK / "probe.json"
    path.write_text(json.dumps({"run_utc": utcnow().isoformat(), "results": results}, indent=1))
    ensure_release()
    publish([path])
    return 0


if __name__ == "__main__":
    sys.exit(main())
