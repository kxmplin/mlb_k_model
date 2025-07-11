#!/usr/bin/env python3
"""
cache_predictions.py
--------------------
Append today's K‐projections to a running cache.

Reads:
  • data/today_ks_proj.csv    (season, game_id, side, pitcher_id, exp_raw, p_raw, exp_cal, p_cal)

Writes/Appends:
  • data/cached_predictions.csv
"""
import pandas as pd
from pathlib import Path

PRED      = Path("data/today_ks_proj.csv")
CACHE_CSV = Path("data/cached_predictions.csv")

def main():
    today = pd.read_csv(PRED)
    # include date for reference if you like:
    # today['run_date'] = pd.Timestamp.now().date()

    if CACHE_CSV.exists():
        df = pd.concat([pd.read_csv(CACHE_CSV), today], ignore_index=True)
    else:
        df = today

    df.to_csv(CACHE_CSV, index=False)
    print(f"✅ Appended {len(today)} rows → {CACHE_CSV}")

if __name__ == "__main__":
    main()
