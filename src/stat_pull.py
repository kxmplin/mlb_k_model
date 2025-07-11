#!/usr/bin/env python3
"""
stat_pull.py
-----------

Pulls batter & pitcher strikeout totals and opportunities from MLB's stats API
and writes them to both per-season CSVs and a combined DuckDB for inspection.
Usage:
  cd src
  python stat_pull.py 2024 2025
"""
import sys
import statsapi
import pandas as pd
import duckdb
from pathlib import Path
from tqdm import tqdm

# ── CONFIG ───────────────────────────────────────────────────────────────────
SEASONS = sys.argv[1:]
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "player_stats.duckdb"


def pull_stats(season: str) -> pd.DataFrame:
    """
    Pull season stats for batters & pitchers into a DataFrame.
    Uses strikeOuts and atBats/gamesStarted as opportunities.
    """
    rows = []
    print(f"⏳  Pulling stats for season {season}…")
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
                    pitch = stats.get("pitching") or {}
                    if pitch.get("gamesStarted", 0) >= 1:
                        k_total = pitch.get("strikeOuts", 0)
                        opps = pitch.get("gamesStarted", 0)
                        rows.append((season, pid, "pitcher", k_total, opps))
                    # batter stats
                    bat = stats.get("batting") or {}
                    if bat.get("atBats", 0) > 0:
                        k_total = bat.get("strikeOuts", 0)
                        opps = bat.get("atBats", 0)
                        rows.append((season, pid, "batter", k_total, opps))

    df = pd.DataFrame(
        rows,
        columns=["season", "player_id", "player_role", "k_total", "opportunities"],
    )
    df["k_rate"] = df["k_total"] / df["opportunities"].replace(0, 1)
    return df


def main():
    if not SEASONS:
        print("Usage: python stat_pull.py <season> [season ...]")
        sys.exit(1)

    # initialize DuckDB
    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=4")

    all_dfs = []
    for season in SEASONS:
        df = pull_stats(season)
        all_dfs.append(df)

        # save per-season CSV
        csv_path = DATA_DIR / f"player_stats_{season}.csv"
        df.to_csv(csv_path, index=False)
        print(f"✅  Wrote {len(df):,} rows to {csv_path.name}")

        # write/update season-specific tables
        con.register("stats_df", df)
        for role in ("pitcher", "batter"):
            tbl = f"{role}_stats_{season}"
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS \
                 SELECT season, player_id, player_role, k_total, opportunities, k_rate \
                 FROM stats_df WHERE player_role = '{role}';"
            )
            print(f"✅  DuckDB table '{tbl}' updated")
        con.unregister("stats_df")

    # combined table
    combined = pd.concat(all_dfs, ignore_index=True)
    con.register("combined_df", combined)
    con.execute(
        "CREATE OR REPLACE TABLE player_stats AS \
         SELECT season, player_id, player_role, k_total, opportunities, k_rate \
         FROM combined_df"
    )
    con.unregister("combined_df")

    # show tables for debug
    tables = con.execute("SHOW TABLES").fetchall()
    print("Tables in DuckDB:", tables)

    con.close()
    print(f"✅  DuckDB updated at {DB_PATH} with combined table 'player_stats'")


if __name__ == "__main__":
    main()
