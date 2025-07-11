"""
cache_schedule.py  (URL version)
================================
Fetch the MLB schedule for <date> using the v1/schedule endpoint with
hydrate=team,lineups, then save BOTH:

    data/cache/schedule_<date>.csv
    data/cache/schedule_<date>.duckdb   (table = schedule)

Usage
-----
$ python src/cache_schedule.py              # Detroit-local today
$ python src/cache_schedule.py 2025-07-10   # specific date
"""

import sys, datetime, pathlib, requests, pandas as pd, duckdb, itertools

# ── select date (Detroit TZ default) ──────────────────────────────────────
if len(sys.argv) > 1:
    date_iso = sys.argv[1]
else:
    from zoneinfo import ZoneInfo
    date_iso = datetime.datetime.now(ZoneInfo("America/Detroit")).date().isoformat()

start = end = date_iso               # single-day window
base  = "https://statsapi.mlb.com/api/v1/schedule"
url   = f"{base}?hydrate=probablePitcher&sportId=1&startDate={start}&endDate={end}"

# ── fetch JSON ------------------------------------------------------------
data = requests.get(url, timeout=10).json()
games = list(itertools.chain.from_iterable(d["games"] for d in data.get("dates", [])))

# ── flatten rows ----------------------------------------------------------
rows = []
for g in games:
    if "teams" not in g:
        continue
    rows.append({
        "game_id"        : g["gamePk"],
        "game_type"      : g.get("gameType"),
        "description"    : g.get("description",""),
        "away_team_id"   : g["teams"]["away"]["team"]["id"],
        "away_team_name" : g["teams"]["away"]["team"]["name"],
        "home_team_id"   : g["teams"]["home"]["team"]["id"],
        "home_team_name" : g["teams"]["home"]["team"]["name"],
        "away_prob_name" : g["teams"]["away"].get("probablePitcher",{}).get("fullName"),
        "away_prob_id"   : g["teams"]["away"].get("probablePitcher",{}).get("id"),
        "home_prob_name" : g["teams"]["home"].get("probablePitcher",{}).get("fullName"),
        "home_prob_id"   : g["teams"]["home"].get("probablePitcher",{}).get("id"),
    })

# keep schema even if rows == 0
cols = [
    "game_id","game_type","description",
    "away_team_id","away_team_name",
    "home_team_id","home_team_name",
    "away_prob_name","away_prob_id",
    "home_prob_name","home_prob_id",
]
df = pd.DataFrame(rows, columns=cols)

# ── write CSV and DuckDB --------------------------------------------------
CACHE = pathlib.Path(__file__).resolve().parent.parent / "data" / "cache"
CACHE.mkdir(parents=True, exist_ok=True)

csv_path = CACHE / f"schedule_{date_iso}.csv"
db_path  = CACHE / f"schedule_{date_iso}.duckdb"
df.to_csv(csv_path, index=False)

duckdb.connect(db_path).execute(
    "CREATE OR REPLACE TABLE schedule AS SELECT * FROM df"
).close()

print(f"✅ schedule saved ({len(df)} rows) → {csv_path.name} & {db_path.name}")
