#!/usr/bin/env python3
"""
cache_predictions.py
--------------------
Append today's strikeout projections to a persistent cache.
If the cache file doesn't exist, initialize it with headers.
By default, it will find the latest `today_ks_proj_YYYY-MM-DD.csv` in data/ if no --pred is provided.
Usage (from project root):
  python src/cache_predictions.py [--pred data/today_k_proj_YYYY-MM-DD.csv]
"""
import argparse
from pathlib import Path
import pandas as pd

data_dir = Path(__file__).resolve().parent.parent / 'data'
CACHE_PATH = data_dir / 'cached_predictions.csv'


def ensure_cache():
    if not CACHE_PATH.exists():
        # Initialize empty cache with the correct columns
        cols = ['date', 'game_id', 'side', 'pitcher_id', 'exp_raw', 'p_raw', 'exp_cal', 'p_cal']
        pd.DataFrame(columns=cols).to_csv(CACHE_PATH, index=False)


def find_latest_pred():
    # find all today_ks_proj.csv and return latest by filename
    files = sorted(data_dir.glob('today_ks_proj.csv'))
    if not files:
        raise FileNotFoundError("No today_ks_proj_*.csv files found in data/")
    return files[-1]


def main():
    parser = argparse.ArgumentParser(
        description="Append today's projections to cached_predictions.csv"
    )
    parser.add_argument(
        '--pred', required=False,
        help='Path to today_ks_proj_YYYY-MM-DD.csv'
    )
    args = parser.parse_args()

    # determine pred file
    if args.pred:
        pred_path = Path(args.pred)
    else:
        pred_path = find_latest_pred()
        print(f"ℹ️  No --pred provided, using latest: {pred_path.name}")

    if not pred_path.exists():
        print(f"❌ Prediction file not found: {pred_path}")
        return

    ensure_cache()

    # Load existing cache
    cache_df = pd.read_csv(CACHE_PATH)
    # Load today's projections
    pred_df = pd.read_csv(pred_path)

    # Extract date from filename
    date_str = pred_path.stem.split('_')[-1]
    pred_df['date'] = date_str

    # Select and order columns
    new_rows = pred_df[['date', 'game_id', 'side', 'pitcher_id', 'exp_raw', 'p_raw', 'exp_cal', 'p_cal']]

    # Append and save
    updated = pd.concat([cache_df, new_rows], ignore_index=True)
    updated.to_csv(CACHE_PATH, index=False)

    print(f"💾 Appended {len(new_rows)} rows to {CACHE_PATH}")

if __name__ == '__main__':
    main()
