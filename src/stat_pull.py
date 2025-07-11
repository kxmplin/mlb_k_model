#!/usr/bin/env python3
"""
stat_pull.py
------------
Pull season-by-season Ks and opportunity counts for pitchers & batters,
save per-season CSV, and aggregate into a combined DuckDB table.
"""
import sys
from pathlib import Path

import pandas as pd
import duckdb
import statsapi
from tqdm import tqdm

# ── CONFIG ───────────────────────────────────────────────────────────────────
SEASONS = sys.argv[1:] or ["2024", "2025"]
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "player_stats.duckdb"

# ── HELPERS ──────────────────────────────────────────────────────────────────
def season_schedule(year: str):
    return statsapi.get("schedule", {"sportId": 1, "season": year, "gameTypes": "R"})["dates"]

def outs_from_ip(ip_str: str) -> int:
    if not ip_str:
        return 0
    if "." in ip_str:
        w,f = ip_str.split(".")
        return int(w)*3 + int(f)
    return int(ip_str)*3

def pull_for_season(season: str) -> pd.DataFrame:
    rows = []
    for d in tqdm(season_schedule(season), desc=f"Season {season}", unit="day"):
        for g in d["games"]:
            if g["status"]["detailedState"] != "Final":
                continue
            box = statsapi.get("game_boxscore", {"gamePk": g["gamePk"]})
            date = d["date"]
            for side in ("away", "home"):
                t = box["teams"][side]
                # PITCHER
                # pick the starter by gamesStarted >1, else max IP
                starter = None
                max_outs = -1
                for pid, pinfo in t["players"].items():
                    pitch = pinfo.get("stats", {}).get("pitching", {})
                    if pitch.get("gamesStarted", 0) >= 1:
                        starter = pid
                        break
                    outs = outs_from_ip(pitch.get("inningsPitched","0.0"))
                    if outs > max_outs:
                        max_outs, starter = outs, pid
                if not starter:
                    continue

                # record pitcher row
                pitch_stats = t["players"][starter].get("stats", {}).get("pitching", {})
                k_total = pitch_stats.get("strikeOuts", 0) or 0
                opps     = pitch_stats.get("battersFaced") or max_outs
                rows.append({
                    "season":       season,
                    "player_id":    int(starter.replace("ID","")),
                    "player_role":  "pitcher",
                    "k_total":      k_total,
                    "opportunities": opps,
                })

                # each batter in lineup
                lineup = t.get("battingOrder") or list(t["players"].keys())
                for spot, raw in enumerate(lineup[:9], start=1):
                    pid = int(str(raw).replace("ID",""))
                    bat_stat = t["players"][f"ID{pid}"].get("stats", {}).get("batting", {})
                    k_t      = bat_stat.get("strikeOuts", 0) or 0
                    opp_b    = bat_stat.get("plateAppearances", bat_stat.get("atBats", 0)) or 0
                    rows.append({
                        "season":       season,
                        "player_id":    pid,
                        "player_role":  "batter",
                        "k_total":      k_t,
                        "opportunities": opp_b,
                    })
    return pd.DataFrame(rows)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # pull per-season, save CSV, collect for DB
    all_dfs = []
    for season in SEASONS:
        df = pull_for_season(season)
        csv_path = DATA_DIR / f"player_stats_{season}.csv"
        df.to_csv(csv_path, index=False)
        print(f"✔️  Wrote {len(df):,} rows → {csv_path.name}")
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)

    # upsert into DuckDB
    con = duckdb.connect(DB_PATH.as_posix())
    con.execute("CREATE SCHEMA IF NOT EXISTS stats;")

    # aggregate by season/player/role
    con.register("full_df", combined)
    # replace entire tables
    con.execute("""
        CREATE OR REPLACE TABLE stats.pitcher_stats AS
        SELECT season, player_id,
               SUM(k_total)        AS k_total,
               SUM(opportunities)  AS opportunities,
               SUM(k_total) / NULLIF(SUM(opportunities),0) AS k_rate
          FROM full_df
         WHERE player_role = 'pitcher'
         GROUP BY season, player_id
    """)
    con.execute("""
        CREATE OR REPLACE TABLE stats.batter_stats AS
        SELECT season, player_id,
               SUM(k_total)        AS k_total,
               SUM(opportunities)  AS opportunities,
               SUM(k_total) / NULLIF(SUM(opportunities),0) AS k_rate
          FROM full_df
         WHERE player_role = 'batter'
         GROUP BY season, player_id
    """)

    print("\n📊 DuckDB tables now:")
    print(con.execute("SHOW TABLES IN stats;").fetchall())
    con.close()

if __name__ == "__main__":
    main()
