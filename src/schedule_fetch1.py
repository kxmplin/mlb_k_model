# Updated schedule_fetch.py with fallback for DuckDB write permissions

updated_code = """
#!/usr/bin/env python3
\"\"\"
schedule_fetch.py
-----------------
Fetch MLB schedule (probable pitchers + lineups) for a given date
(or today if no date is provided) and save to:
  • data/schedule.csv
  • data/schedule.duckdb (table: schedule), with fallback to src/schedule.db
\"\"\"
import argparse
from datetime import date
from pathlib import Path

import pandas as pd
import duckdb
import statsapi
from tqdm import tqdm

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH  = DATA_DIR / "schedule.duckdb"

def get_starter_from_box(box_side: dict) -> int | None:
    players = box_side.get("players", {})
    for pid, pdata in players.items():
        gs = pdata.get("stats", {}).get("pitching", {}).get("gamesStarted", 0)
        if gs >= 1:
            return int(pid.replace("ID", ""))
    best_pid, best_outs = None, -1
    for pid, pdata in players.items():
        ip = pdata.get("stats", {}).get("pitching", {}).get("inningsPitched", "0.0")
        try:
            w, frac = str(ip).split(".")
            outs = int(w)*3 + int(frac)
        except:
            outs = 0
        if outs > best_outs:
            best_outs, best_pid = outs, pid
    return int(best_pid.replace("ID", "")) if best_pid else None

def fetch_for_date(d: str) -> pd.DataFrame:
    rows = []
    resp = statsapi.get("schedule", {"sportId": 1, "date": d})
    for day in resp.get("dates", []):
        for g in day["games"]:
            if g["status"]["detailedState"] not in ("Pre-Game", "Final"):
                continue
            pk = g["gamePk"]
            box = statsapi.get("game_boxscore", {"gamePk": pk})
            rec = {
                "game_id":       pk,
                "official_date": d,
                "away_pid":      None,
                "home_pid":      None,
                "away_lineup":   "",
                "home_lineup":   "",
            }
            for side in ("away", "home"):
                pp = g["teams"][side].get("probablePitcher")
                if pp and pp.get("id"):
                    rec[f"{side}_pid"] = pp["id"]
                else:
                    rec[f"{side}_pid"] = get_starter_from_box(box["teams"][side])
                order = box["teams"][side].get("battingOrder") or []
                def pid_from_raw(raw):
                    if isinstance(raw, int):
                        return raw
                    try:
                        return int(str(raw).replace("ID",""))
                    except:
                        return None
                pids = []
                for raw in order[:9]:
                    pid = pid_from_raw(raw)
                    if pid is not None:
                        pids.append(pid)
                rec[f"{side}_lineup"] = ",".join(str(pid) for pid in pids)
            rows.append(rec)
    return pd.DataFrame(rows)

def main():
    p = argparse.ArgumentParser(
        description="Fetch MLB schedule for today or a given date."
    )
    p.add_argument(
        "--date",
        help="Date in YYYY-MM-DD; defaults to today if omitted",
        default=date.today().isoformat()
    )
    args = p.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    print(f"📅  Pulling schedule for {args.date}…")
    df = fetch_for_date(args.date)

    # Save CSV
    csv_path = DATA_DIR / "schedule.csv"
    df.to_csv(csv_path, index=False)
    print(f"✔️  Wrote {len(df)} rows → {csv_path.name}")

    # Save to DuckDB with fallback
    try:
        con = duckdb.connect(DB_PATH.as_posix())
        con.execute("CREATE SCHEMA IF NOT EXISTS main;")
        con.register("sched_df", df)
        con.execute("CREATE OR REPLACE TABLE main.schedule AS SELECT * FROM sched_df;")
        con.close()
        print(f"✔️  Updated DuckDB table → {DB_PATH.name}")
    except Exception as e:
        fallback = Path(__file__).resolve().parent / "schedule.db"
        print(f"⚠️  Could not write to {DB_PATH}: {e}")
        print(f"ℹ️  Falling back to local DB: {fallback.name}")
        con = duckdb.connect(fallback.as_posix())
        con.register("sched_df", df)
        con.execute("CREATE OR REPLACE TABLE schedule AS SELECT * FROM sched_df;")
        con.close()
        print(f"✔️  Saved DuckDB fallback → {fallback.name}")

if __name__ == "__main__":
    main()
"""

# Write the updated code to the file system for user to use
file_path = '/mnt/data/schedule_fetch_updated.py'
with open(file_path, 'w') as f:
    f.write(updated_code)

file_path
