#!/usr/bin/env python3
"""
schedule_fetch.py
=================
Pull MLB schedule (with probable pitchers & lineups) via statsapi
and cache it to data/schedule.duckdb for use by today_proj.py.

Usage:
  # for a single date
  python schedule_fetch.py --start 2025-07-11 --end 2025-07-11

  # for a full season
  python schedule_fetch.py --start 2025-03-20 --end 2025-10-05
"""
import argparse
from pathlib import Path

import pandas as pd
import duckdb
import statsapi

def fetch_schedule(start: str, end: str) -> pd.DataFrame:
    raw = statsapi.get(
        "schedule",
        {
            "sportId": 1,
            "startDate": start,
            "endDate": end,
            "hydrate": "probablePitchers,lineups"
        }
    )

    rows = []
    for day in raw.get("dates", []):
        date = day["date"]
        for g in day["games"]:
            sched = {
                "official_date": date,
                "game_id": g["gamePk"],
                "away_probable_pitcher": g["teams"]["away"]
                    .get("probablePitcher", {}).get("id", None),
                "home_probable_pitcher": g["teams"]["home"]
                    .get("probablePitcher", {}).get("id", None),
                "away_lineup": None,
                "home_lineup": None
            }

            for side in ("away", "home"):
                lo = g["teams"][side].get("lineup", [])
                if lo:
                    ids = [p["player"]["id"] for p in lo[:9]]
                    sched[f"{side}_lineup"] = ",".join(map(str, ids))

            rows.append(sched)

    return pd.DataFrame(rows)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True,
                   help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True,
                   help="End date YYYY-MM-DD")
    args = p.parse_args()

    df = fetch_schedule(args.start, args.end)
    if df.empty:
        print("⚠️  No games found in that date range.")
        return

    # determine project root and ensure data dir exists
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    DATA_DIR.mkdir(exist_ok=True)

    outdb = DATA_DIR / "schedule.duckdb"
    conn  = duckdb.connect(str(outdb))

    conn.execute("DROP TABLE IF EXISTS schedule;")
    conn.register("tmp", df)
    conn.execute("CREATE TABLE schedule AS SELECT * FROM tmp;")
    conn.close()

    print(f"✅ Schedule cached to {outdb} ({len(df)} games)")

if __name__ == "__main__":
    main()
