"""
Step 2 – Roster + Player-ID harvest (folder-aware)

Run inside the Docker container:

    python src/01_roster_pull.py          # current season
    python src/01_roster_pull.py 2024     # back-fill another year
"""

import sys
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
# 2. Season argument
# -------------------------------------------------------------------
season = sys.argv[1] if len(sys.argv) > 1 else str(date.today().year)
sport_id = 1          # 1 = MLB
print(f"\n📥  Harvesting MLB rosters for {season}…")

# -------------------------------------------------------------------
# 3. Active team IDs
# -------------------------------------------------------------------
teams = statsapi.get(
    "teams",
    {"sportIds": sport_id, "activeStatus": "Yes",
     "fields": "teams,id,name,abbreviation"},
)["teams"]

team_ids  = [t["id"] for t in teams]
abbr_map  = {t["id"]: t["abbreviation"] for t in teams}

# -------------------------------------------------------------------
# 4. Pull each roster
# -------------------------------------------------------------------
rows = []
for tid in tqdm(team_ids, desc="Teams"):
    roster = statsapi.get(
        "team_roster",
        {"teamId": tid, "season": season, "hydrate": "person"},
    )["roster"]

    for slot in roster:
        p = slot["person"]
        rows.append(
            {
                "player_id": p["id"],
                "name": p["fullName"],
                "team_id": tid,
                "team_abbr": abbr_map[tid],
                "position": slot["position"]["abbreviation"],
                "season": season,
            }
        )

# -------------------------------------------------------------------
# 5. Save to CSV + DuckDB
# -------------------------------------------------------------------
df = pd.DataFrame(rows).sort_values(["team_abbr", "name"])

csv_path = DATA_DIR / f"players_{season}.csv"
db_path  = DATA_DIR / f"players_{season}.duckdb"

df.to_csv(csv_path, index=False)

conn = duckdb.connect(str(db_path))
conn.register("players_df", df)
conn.execute("CREATE TABLE IF NOT EXISTS players AS SELECT * FROM players_df")
conn.close()

print(f"\n✅  Saved {len(df):,} players → {csv_path.name} & {db_path.name}")
