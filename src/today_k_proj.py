"""
today_k_proj.py  –  Daily strike-out projections with Statcast K-rates
=======================================================================
Prerequisites:
  • cache_schedule.py               → schedule_<date>.duckdb / .csv
  • cache_lineups.py                → lineups_<date>.json
  • pybaseball installed (for statcast pulls)

Outputs:
  • console summary
  • data/today_k_proj_<date>.csv
"""

from pathlib import Path
import json, numpy as np, pandas as pd, duckdb, statsapi
from datetime import datetime, timedelta
from k_pred_core import merge_prob, sim_many
from pybaseball import statcast_pitcher, statcast_batter

# ── date (America/Detroit) ───────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("America/Detroit")).date()
except Exception:
    from dateutil import tz
    today = datetime.now(tz.gettz("America/Detroit")).date()

date_iso = today.isoformat()
SEASON   = str(today.year)
LINE     = 6.5
SIMS     = 5_000

BASE   = Path(__file__).resolve().parent.parent
DATA   = BASE / "data"
CACHE  = DATA / "cache"

# ── load schedule cache (DuckDB > CSV) ───────────────────────────────────
db_path  = CACHE / f"schedule_{date_iso}.duckdb"
csv_path = CACHE / f"schedule_{date_iso}.csv"
if db_path.exists():
    with duckdb.connect(str(db_path), read_only=True) as con:
        sched_df = con.execute("SELECT * FROM schedule").fetchdf()
elif csv_path.exists():
    sched_df = pd.read_csv(csv_path)
else:
    raise FileNotFoundError("Run cache_schedule.py first")
if sched_df.empty:
    raise RuntimeError("No games in schedule cache")

# ── load lineup cache JSON ───────────────────────────────────────────────
lu_path = CACHE / f"lineups_{date_iso}.json"
if not lu_path.exists():
    raise FileNotFoundError("Run cache_lineups.py first")
with open(lu_path) as f:
    lineup_cache = json.load(f)          # { game_id: {side: [ids]} }

# ── prepare Statcast window & caches ────────────────────────────────────
END = date_iso
START = (today - timedelta(days=30)).isoformat()
_pit_cache = {}
_bat_cache = {}

def sc_k_rate_pitcher(pid: int) -> float:
    if pid in _pit_cache:
        return _pit_cache[pid]
    try:
        df = statcast_pitcher(START, END, pid)
        pas = df["at_bat_number"].nunique()
        ks  = df[df.event == "strikeout"]["at_bat_number"].nunique()
        rate = ks / pas if pas else 0.20
    except Exception:
        rate = 0.20
    _pit_cache[pid] = rate
    return rate

def sc_k_rate_batter(bid: int) -> float:
    if bid in _bat_cache:
        return _bat_cache[bid]
    try:
        df = statcast_batter(START, END, bid)
        pas = df.shape[0]
        ks  = (df.event == "strikeout").sum()
        rate = ks / pas if pas else 0.20
    except Exception:
        rate = 0.20
    _bat_cache[bid] = rate
    return rate

# ── fallback roster helper ───────────────────────────────────────────────
def roster_lineup(team_id: int) -> list[int]:
    r = statsapi.get("team_roster", {"teamId": team_id, "rosterType": "active"})
    return [p["person"]["id"] for p in r["roster"][:9]]

# ── run projections ──────────────────────────────────────────────────────
print(f"Projections for {date_iso} — line {LINE} Ks\n")
out_rows = []

for _, g in sched_df.iterrows():
    gid_str = str(int(g.game_id))
    for side in ("away", "home"):
        team_id   = int(getattr(g, f"{side}_team_id"))
        team_name = getattr(g, f"{side}_team_name")
        pid       = int(getattr(g, f"{side}_prob_id"))
        pname     = getattr(g, f"{side}_prob_name")

        if pd.isna(pid):
            print(f"{team_name}: starter ID missing – skip")
            continue

        lineup = lineup_cache.get(gid_str, {}).get(side, [])
        src = "cache"
        if len(lineup) < 9:
            lineup = roster_lineup(team_id); src = "roster"
        if len(lineup) < 9:
            print(f"{team_name} ({pname}): lineup incomplete"); continue

        k_p = sc_k_rate_pitcher(pid)
        kb_rates = [sc_k_rate_batter(b) for b in lineup]
        pks = np.array([merge_prob(k_p, kb) for kb in kb_rates])
        sims = sim_many(pks, SIMS)

        expK, probK = sims.mean(), (sims >= LINE).mean()
        print(f"{team_name} ({pname}) [LU:{src}]  ExpKs {expK:.2f} • P≥{LINE} {probK:.3f}")

        out_rows.append({
            "team": team_name,
            "pitcher": pname,
            "LU_src": src,
            "expK": round(expK,2),
            f"P(K≥{LINE})": round(probK,3)
        })

# ── save results ─────────────────────────────────────────────────────────
if out_rows:
    df_out = pd.DataFrame(out_rows)
    out_file = DATA / f"today_k_proj_{date_iso}.csv"
    df_out.to_csv(out_file, index=False)
    print(f"\nCSV saved → {out_file}")
else:
    print("\nNo projections generated.")
