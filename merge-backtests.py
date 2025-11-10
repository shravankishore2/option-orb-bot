#!/usr/bin/env python3
"""
merge_backtests.py

- Lists repository artifacts using GitHub API.
- Downloads any artifact zips that contain 'backtest_opening_range.csv' created today.
- Extracts CSVs and merges/appends into repo backtest_opening_range.csv with deduplication.
- Writes the merged file into repo root (backtest_opening_range.csv).
"""

import os
import io
import sys
import zipfile
import requests
import pandas as pd
from datetime import datetime, timezone, date
from dateutil import parser

GITHUB_API = os.environ.get("GITHUB_API_URL", "https://api.github.com")
TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = os.environ.get("REPO")  # owner/repo

if not TOKEN or not REPO:
    print("GITHUB_TOKEN and REPO env vars required", file=sys.stderr)
    sys.exit(1)

HEADERS = {"Authorization": f"token {TOKEN}", "Accept": "application/vnd.github+json"}
owner, repo = REPO.split("/")

def list_artifacts():
    artifacts = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts?per_page=100&page={page}"
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        data = r.json()
        artifacts.extend(data.get("artifacts", []))
        if data.get("total_count", 0) == 0 or len(data.get("artifacts", [])) < 100:
            break
        page += 1
    return artifacts

def artifact_is_today(artifact):
    # created_at -> compare to repo timezone: we'll use UTC "today"
    created = parser.isoparse(artifact.get("created_at"))
    return created.date() == datetime.now(timezone.utc).date()

def download_artifact_zip(artifact_id):
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    r = requests.get(url, headers=HEADERS, stream=True, timeout=120)
    r.raise_for_status()
    return io.BytesIO(r.content)

def extract_backtest_csvs_from_zip(zip_bytes):
    results = []
    with zipfile.ZipFile(zip_bytes) as z:
        for name in z.namelist():
            if name.lower().endswith("backtest_opening_range.csv"):
                with z.open(name) as fh:
                    df = pd.read_csv(fh)
                    results.append(df)
    return results

def main():
    print("Listing artifacts...")
    artifacts = list_artifacts()
    # filter artifacts with 'backtest' in name and created today
    candidate = [a for a in artifacts if ("backtest" in (a.get("name","").lower()) or "backtest_opening_range" in (a.get("name","").lower())) and artifact_is_today(a)]
    if not candidate:
        print("No backtest artifacts for today found. Exiting.")
        return

    print(f"Found {len(candidate)} candidate artifact(s) for today. Downloading...")
    dfs = []
    for a in candidate:
        try:
            zipb = download_artifact_zip(a["id"])
            extracted = extract_backtest_csvs_from_zip(zipb)
            if extracted:
                print(f"  -> artifact {a['name']} provided {len(extracted)} CSV(s)")
                dfs.extend(extracted)
            else:
                print(f"  -> artifact {a['name']} had no backtest CSV.")
        except Exception as e:
            print(f"  -> failed to download/extract artifact {a['name']}: {e}")

    if not dfs:
        print("No CSVs extracted from found artifacts. Exiting.")
        return

    # standardize columns expected in backtest:
    for i, df in enumerate(dfs):
        dfs[i] = df.rename(columns=lambda c: c.strip()).copy()

    new_df = pd.concat(dfs, ignore_index=True, sort=False)
    if new_df.empty:
        print("Concatenated new dataframe empty — exiting.")
        return

    # Normalize column names expected by your backtest file
    # Provide some common fallbacks
    colmap = {
        "date": "date",
        "time": "time",
        "symbol": "symbol",
        "direction": "direction",
        "entry_price": "entry_price",
        "entry_close": "entry_price",
        "ORH": "ORH",
        "ORL": "ORL",
        "high": "ORH",
        "low": "ORL",
        "prev_close": "prev_close",
        "suggested_action": "suggested_action"
    }
    new_df.columns = [colmap.get(c.lower(), c) for c in new_df.columns]

    # Ensure date & time columns
    if "date" not in new_df.columns:
        # try to derive from timestamp column or created_at
        new_df["date"] = datetime.now().date().isoformat()
    if "time" not in new_df.columns:
        new_df["time"] = datetime.now().strftime("%H:%M:%S")

    # canonicalize symbol uppercase
    new_df["symbol"] = new_df["symbol"].astype(str).str.strip().str.upper()

    # path in repo
    out_path = "backtest_opening_range.csv"
    if os.path.exists(out_path):
        try:
            existing = pd.read_csv(out_path)
            existing.columns = [c.strip() for c in existing.columns]
        except Exception as e:
            print(f"Failed reading existing {out_path}: {e} — will overwrite with new merged file")
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    # concat, dedupe
    combined = pd.concat([existing, new_df], ignore_index=True, sort=False)
    # Deduplicate by a strong key (date,time,symbol,direction,entry_price,ORH,ORL)
    dedupe_cols = ["date", "time", "symbol", "direction", "entry_price", "ORH", "ORL"]
    # keep only columns that exist
    dedupe_cols = [c for c in dedupe_cols if c in combined.columns]
    before = len(combined)
    combined = combined.drop_duplicates(subset=dedupe_cols, keep="first")
    after = len(combined)
    print(f"Merged {len(new_df)} new rows; combined before={before}, after dedupe={after} rows.")

    # write CSV with stable column order: put important cols first
    cols_order = ["date", "time", "symbol", "direction", "entry_price", "ORH", "ORL", "prev_close", "suggested_action"]
    cols = [c for c in cols_order if c in combined.columns] + [c for c in combined.columns if c not in cols_order]
    combined.to_csv(out_path, index=False, columns=cols)
    print(f"Wrote merged file to {out_path}")

if __name__ == "__main__":
    main()