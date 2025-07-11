
#!/usr/bin/env python3
"""
schedule_fetch.py
-----------------
Fetch MLB schedule (probable pitchers + lineups) for a given date
(positional or --date). Defaults to today if no date provided.
Includes Pre-Game & In Progress lineups for future/today, and Final for past.
Saves to:
  • data/schedule.csv
  • data/schedule.duckdb (table: schedule), with fallback to src/schedule.db
"""
import argparse
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import duckdb
import statsapi

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

def pid_from_raw(raw):
    if isinstance(raw, int):
        return raw
    try:
        return int(str(raw).replace("ID","")) 
    except:
        return None

def fetch_for_date(d: str) -> pd.DataFrame:
    """Pull schedule + lineups for a single YYYY-MM-DD date."""
    rows = []
    # determine allowed states
    today_str = date.today().isoformat()
    allow_states = ("Pre-Game", "In Progress") if d == today_str else ("Pre-Game", "In Progress", "Final")

    resp = statsapi.get("schedule", {"sportId": 1, "date": d})
    for day in resp.get("dates", []):
        for g in day["games"]:
            state = g["status"]["detailedState"]
            if state not in allow_states:
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
                # lineup extraction
                order = box["teams"][side].get("battingOrder") or []
                pids = [pid_from_raw(raw) for raw in order[:9] if pid_from_raw(raw) is not None]
                if len(pids) < 9:
                    tmp = []
                    for pid_str, pdata in box["teams"][side].get("players", {}).items():
                        bo = pdata.get("battingOrder")
                        if bo:
                            try:
                                spot = int(str(bo).split("-")[0])
                                pid_int = int(pid_str.replace("ID","")) 
                                tmp.append((spot, pid_int))
                            except:
                                continue
                    tmp.sort(key=lambda x: x[0])
                    pids = [pid for _, pid in tmp[:9]]
                if len(pids) < 9:
                    raw_batters = box["teams"][side].get("batters", [])
                    fb = [pid_from_raw(raw) for raw in raw_batters[:9] if pid_from_raw(raw) is not None]
                    if fb:
                        pids = fb
                rec[f"{side}_lineup"] = ",".join(str(pid) for pid in pids)
            rows.append(rec)
    return pd.DataFrame(rows)

def main():
    p = argparse.ArgumentParser(
        description="Fetch MLB schedule for a date (default today, accept positional)." 
    )
    p.add_argument(
        'date',
        nargs='?',
        help="Date in YYYY-MM-DD; defaults to today if omitted"
    )
    args = p.parse_args()

    fetch_date = args.date if args.date else date.today().isoformat()
    DATA_DIR.mkdir(exist_ok=True)

    print(f"📅  Pulling schedule for {fetch_date}…")
    df = fetch_for_date(fetch_date)

    # Save CSV
    csv_path = DATA_DIR / "schedule.csv"
    df.to_csv(csv_path, index=False)
    print(f"✔️  Wrote {len(df)} rows → {csv_path.name}")

    # Save to DuckDB with fallback, only if DataFrame has columns
    if not df.empty:
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
    else:
        print("⚠️  No data to save to DuckDB; schedule CSV is empty.")

if __name__ == "__main__":
    main()
