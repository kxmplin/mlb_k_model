#!/usr/bin/env python3
"""
02_stat_pull.py
---------------
Pull every player’s raw strike-out totals and opportunities
for each season, then save to CSV + DuckDB for downstream use.

Outputs:
  • data/player_stats.csv
  • data/player_stats.duckdb (table: player_stats)

Usage (from /app/src):
    python 02_stat_pull.py
"""
from pathlib import Path
import duckdb
import pandas as pd
import statsapi
from tqdm import tqdm

# ---- CONFIG ----
SEASONS = ["2024", "2025"]
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
OUT_CSV = DATA_DIR / "player_stats.csv"
OUT_DB  = DATA_DIR / "player_stats.duckdb"

def season_schedule(year: str):
    return statsapi.get(
        "schedule",
        {"sportId": 1, "season": year, "gameTypes": "R"},
    )["dates"]

rows = []
for yr in SEASONS:
    for d in tqdm(season_schedule(yr), desc=f"Season {yr}"):
        for g in d["games"]:
            if g["status"]["detailedState"] != "Final":
                continue
            box = statsapi.get("game_boxscore", {"gamePk": g["gamePk"]})
            for side in ("away", "home"):
                for pid_key, pdata in box["teams"][side]["players"].items():
                    pid = int(pid_key.replace("ID", ""))

                    # Pitching line (if any)
                    pstat = pdata.get("stats", {}).get("pitching")
                    if pstat:
                        opps = pstat.get("gamesStarted", 0)
                        ktot = pstat.get("strikeOuts", 0)
                        rows.append({
                            "season":        yr,
                            "player_id":     pid,
                            "role":          "pitcher",
                            "opportunities": opps,
                            "k_total":       ktot
                        })

                    # Batting line (if any)
                    bstat = pdata.get("stats", {}).get("batting")
                    if bstat:
                        opps = bstat.get("atBats", 0)
                        ktot = bstat.get("strikeOuts", 0)
                        rows.append({
                            "season":        yr,
                            "player_id":     pid,
                            "role":          "batter",
                            "opportunities": opps,
                            "k_total":       ktot
                        })

# Build DataFrame and compute rates
df = pd.DataFrame(rows)
df["k_rate"] = df["k_total"] / df["opportunities"].replace(0, 1)

# Save to CSV
df.to_csv(OUT_CSV, index=False)

# Save to DuckDB
con = duckdb.connect(str(OUT_DB))
con.register("stats_df", df)
con.execute("""
  CREATE OR REPLACE TABLE player_stats AS
  SELECT season, player_id, role AS group,
         opportunities, k_total, k_rate
    FROM stats_df
""")
con.close()

print(f"✅ Saved {len(df)} records →\n  • {OUT_CSV}\n  • {OUT_DB} (table: player_stats)")
