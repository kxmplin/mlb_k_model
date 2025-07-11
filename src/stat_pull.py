#!/usr/bin/env python3
"""
Pull season stats for batters & pitchers into DuckDB.
"""
from pathlib import Path
import duckdb
import pandas as pd
import statsapi
from tqdm import tqdm

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH  = DATA_DIR / "player_stats.duckdb"

def pull_stats(season: str):
    rows = []
    dates = statsapi.get("schedule", {"sportId":1,"season":season,"gameTypes":"R"})["dates"]
    for d in tqdm(dates, desc=f"Sched {season}"):
        for g in d["games"]:
            if g["status"]["detailedState"]!="Final":
                continue
            box = statsapi.get("game_boxscore",{"gamePk":g["gamePk"]})
            for side in ("away","home"):
                players = box["teams"][side]["players"]
                for pid_key,p in players.items():
                    pid = int(pid_key.replace("ID",""))
                    stats = p.get("stats",{})
                    if stats.get("pitching"):
                        k = stats["pitching"].get("strikeOuts",0)
                        games = stats["pitching"].get("gamesStarted",0)
                        rows.append((season,pid,"pitcher",k,games))
                    if stats.get("batting"):
                        k = stats["batting"].get("strikeOuts",0)
                        ab= stats["batting"].get("atBats",0)
                        rows.append((season,pid,"batter",k,ab))
    df = pd.DataFrame(rows,columns=[
        "season","player_id","group","k_total","opportunities"
    ])
    df["k_rate"] = df["k_total"] / df["opportunities"].replace(0,1)
    con = duckdb.connect(DB_PATH)
    con.execute("""
      CREATE TABLE IF NOT EXISTS pitcher_stats AS
      SELECT * FROM df WHERE group='pitcher';
    """)
    con.execute("""
      CREATE TABLE IF NOT EXISTS batter_stats AS
      SELECT * FROM df WHERE group='batter';
    """)
    con.close()
    print("✅ Pulled stats into", DB_PATH)

if __name__=="__main__":
    import sys
    pull_stats(sys.argv[1])
