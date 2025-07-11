#!/usr/bin/env python3
"""
stat_pull.py
============
Pull season stats for batters & pitchers into DuckDB.

Usage:
    cd src
    python stat_pull.py 2025
"""
from pathlib import Path
import sys

import duckdb
import pandas as pd
import statsapi
from tqdm import tqdm

# where to store your player_stats.duckdb
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "player_stats.duckdb"

def pull_stats(season: str):
    rows = []
    dates = statsapi.get(
        "schedule",
        {"sportId": 1, "season": season, "gameTypes": "R"},
    )["dates"]

    for d in tqdm(dates, desc=f"Sched {season}"):
        for g in d["games"]:
            if g["status"]["detailedState"] != "Final":
                continue
            box = statsapi.get("game_boxscore", {"gamePk": g["gamePk"]})
            for side in ("away", "home"):
                players = box["teams"][side]["players"]
                for pid_key, pdata in players.items():
                    pid = int(pid_key.replace("ID", ""))
                    stats = pdata.get("stats", {})
                    # pitcher stats
                    pitch = stats.get("pitching")
                    if pitch:
                        k_total = pitch.get("strikeOuts", 0)
                        opps    = pitch.get("gamesStarted", 0)
                        rows.append((season, pid, "pitcher", k_total, opps))
                    # batter stats
                    bat = stats.get("batting")
                    if bat:
                        k_total = bat.get("strikeOuts", 0)
                        opps    = bat.get("atBats", 0)
                        rows.append((season, pid, "batter", k_total, opps))

    # build DataFrame, rename 'group' → 'player_role'
    df = pd.DataFrame(
        rows,
        columns=[
            "season",
            "player_id",
            "player_role",
            "k_total",
            "opportunities",
        ],
    )
    # compute k_rate safely
    df["k_rate"] = df["k_total"] / df["opportunities"].replace(0, 1)

    # write into DuckDB
    con = duckdb.connect(str(DB_PATH))
    # register under a safe name
    con.register("stats_df", df)
    # create pitcher_stats
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pitcher_stats AS
        SELECT season, player_id, k_total, opportunities, k_rate
        FROM stats_df
        WHERE player_role = 'pitcher';
        """
    )
    # create batter_stats
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS batter_stats AS
        SELECT season, player_id, k_total, opportunities, k_rate
        FROM stats_df
        WHERE player_role = 'batter';
        """
    )
    con.close()
    print(f"✅ Pulled stats for {season} → {DB_PATH}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python stat_pull.py <SEASON>")
        sys.exit(1)
    pull_stats(sys.argv[1])
