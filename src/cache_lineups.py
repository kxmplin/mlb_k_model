"""
cache_lineups.py  –  cache starting lineups from StatsAPI with yesterday fallback
================================================================================
Reads data/cache/schedule_<date>.duckdb (or .csv) and writes
data/cache/lineups_<date>.json, using:

1) statsapi.get("game", {"gamePk":…, "hydrate":"lineups"})
2) if fewer than 9 batters, fallback to yesterday’s lineup via
   statsapi.get("schedule",{"sportId":1,"date":<yesterday>})
"""
import sys, json, datetime, pathlib, pandas as pd, duckdb, statsapi

# ── choose date (America/Detroit) ─────────────────────────────────────────
if len(sys.argv) > 1:
    date_iso = sys.argv[1]
else:
    from zoneinfo import ZoneInfo
    date_iso = datetime.datetime.now(ZoneInfo("America/Detroit")).date().isoformat()

BASE   = pathlib.Path(__file__).resolve().parent.parent
CACHE  = BASE / "data" / "cache"
db_path  = CACHE / f"schedule_{date_iso}.duckdb"
csv_path = CACHE / f"schedule_{date_iso}.csv"

# ── load schedule cache (DuckDB > CSV) ───────────────────────────────────
if db_path.exists():
    with duckdb.connect(str(db_path), read_only=True) as con:
        sched_df = con.execute("SELECT * FROM schedule").fetchdf()
elif csv_path.exists():
    sched_df = pd.read_csv(csv_path)
else:
    sys.exit("⚠️  schedule cache missing – run cache_schedule.py first")

if sched_df.empty:
    sys.exit("⚠️  no games in schedule cache")

# ── helper: get lineup from game endpoint ─────────────────────────────────
def lineup_from_game(gid: int, side: str) -> list[int]:
    try:
        game = statsapi.get("game", {"gamePk": gid, "hydrate": "lineups"})
        return [int(pid) for pid in game["liveData"]["boxscore"]["teams"][side]["battingOrder"]][:9]
    except Exception:
        return []

# ── helper: pull yesterday’s lineup if today’s is missing  ───────────────
def previous_lineup(team_id: int) -> list[int]:
    prev_day = (datetime.date.fromisoformat(date_iso) - datetime.timedelta(days=1)).isoformat()
    sched = statsapi.get("schedule", {"sportId": 1, "date": prev_day})
    for d in sched.get("dates", []):
        for g in d.get("games", []):
            # find the game for this team
            teams = g.get("teams", {})
            if teams.get("away", {}).get("team", {}).get("id") == team_id:
                side = "away"
            elif teams.get("home", {}).get("team", {}).get("id") == team_id:
                side = "home"
            else:
                continue
            gid = g["gamePk"]
            lu  = lineup_from_game(gid, side)
            if len(lu) == 9:
                return lu
    return []

# ── assemble lineup JSON ──────────────────────────────────────────────────
lineup_map: dict[str, dict[str, list[int]]] = {}

for _, g in sched_df.iterrows():
    gid      = int(g.game_id)
    gid_s    = str(gid)
    away_tid = int(g.away_team_id)
    home_tid = int(g.home_team_id)

    # away batting
    lu = lineup_from_game(gid, "away")
    if len(lu) < 9:
        lu = previous_lineup(away_tid)
    if len(lu) == 9:
        lineup_map.setdefault(gid_s, {})["away"] = lu

    # home batting
    lu = lineup_from_game(gid, "home")
    if len(lu) < 9:
        lu = previous_lineup(home_tid)
    if len(lu) == 9:
        lineup_map.setdefault(gid_s, {})["home"] = lu

# ── write JSON cache ─────────────────────────────────────────────────────
out_path = CACHE / f"lineups_{date_iso}.json"
with open(out_path, "w") as f:
    json.dump(lineup_map, f, indent=2)

print(f"✅ {out_path.name} saved ({len(lineup_map)} games cached)")
