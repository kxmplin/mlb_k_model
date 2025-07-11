"""
Step 3 – Season-to-date stats + K metrics (folder-aware)

Run inside the Docker container:

    python src/02_stat_pull.py           # current season
    python src/02_stat_pull.py 2024      # back-fill another year
"""

import sys
import time
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import statsapi
from tqdm import tqdm

# -------------------------------------------------------------------
# 1. Directories
# -------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent   # project root
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# -------------------------------------------------------------------
# 2. Season argument & I/O paths
# -------------------------------------------------------------------
season = sys.argv[1] if len(sys.argv) > 1 else str(date.today().year)

roster_db = DATA_DIR / f"players_{season}.duckdb"
stats_csv = DATA_DIR / f"player_stats_{season}.csv"
stats_db  = DATA_DIR / f"player_stats_{season}.duckdb"

print(f"\n📊  Pulling season stats for {season}…")

# -------------------------------------------------------------------
# 3. Load roster
# -------------------------------------------------------------------
conn_roster = duckdb.connect(str(roster_db))
roster_df = conn_roster.execute(
    "SELECT player_id, name, position, team_abbr FROM players"
).fetch_df()
conn_roster.close()

def is_pitcher(pos: str) -> bool:
    return pos.upper() in {"P", "SP", "RP"}

# -------------------------------------------------------------------
# 4. Helper: fetch a stat dict
# -------------------------------------------------------------------
def fetch_stat_line(pid: int, group: str) -> dict:
    params = {
        "playerId": pid,
        "stats": "season",
        "group": group,
        "season": season,
    }
    try:
        data = statsapi.get("stats", params)
        splits = data["stats"][0]["splits"]
        return splits[0]["stat"] if splits else {}
    except Exception as e:
        print(f"⚠️  {pid}: {e}")
        return {}

# -------------------------------------------------------------------
# 5. Pull stats for every player
# -------------------------------------------------------------------
rows = []
for _, row in tqdm(
    roster_df.iterrows(), total=len(roster_df), desc="Players"
):
    pid      = int(row.player_id)
    name     = row.name
    position = row.position
    team     = row.team_abbr

    group = "pitching" if is_pitcher(position) else "hitting"
    stat  = fetch_stat_line(pid, group)

    # ---------- derive Ks ----------
    if group == "pitching":
        bf = stat.get("battersFaced") or 0
        k  = stat.get("strikeOuts")   or 0
        ip = stat.get("inningsPitched") or "0.0"

        outs = (
            lambda s: int(s.split(".")[0]) * 3 + int(s.split(".")[1])
            if "." in s
            else int(s) * 3
        )(ip)

        k_per_9 = (k / (outs / 3)) * 9 if outs else None
        k_rate  = k / bf if bf else None
    else:
        pa    = stat.get("plateAppearances") or 0
        k     = stat.get("strikeOuts")       or 0
        k_rate  = k / pa if pa else None
        k_per_9 = None

    rows.append(
        {
            "player_id": pid,
            "name": name,
            "team": team,
            "position": position,
            "group": group,
            # raw counts
            "strikeouts":         stat.get("strikeOuts"),
            "plate_appearances":  stat.get("plateAppearances"),
            "batters_faced":      stat.get("battersFaced"),
            "innings_pitched":    stat.get("inningsPitched"),
            # derived
            "k_rate":  k_rate,
            "k_per_9": k_per_9,
            "season":  season,
        }
    )

    time.sleep(0.05)   # polite pause

# -------------------------------------------------------------------
# 6. Save to CSV + DuckDB
# -------------------------------------------------------------------
stat_df = pd.DataFrame(rows)
stat_df.to_csv(stats_csv, index=False)

conn = duckdb.connect(str(stats_db))
conn.register("stat_df", stat_df)
conn.execute("CREATE TABLE IF NOT EXISTS player_stats AS SELECT * FROM stat_df")
conn.close()

print(f"\n✅  Saved stats for {len(stat_df):,} players → {stats_csv.name} & {stats_db.name}")
