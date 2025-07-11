"""
today_k_proj_pybaseball.py
==========================
Pure-pybaseball version (no Savant, no StatsAPI).
Probable starters from pybaseball.schedule(); lineups via yesterday/roster.
"""

from pathlib import Path
import datetime, numpy as np, pandas as pd, duckdb
from pybaseball import schedule, playerid_lookup
from k_pred_core import merge_prob, sim_many
from k_pred_sim   import fetch_k_rate
from src.today_k_proj import get_yesterday_lineup, fallback_lineup  # reuse helpers

# ---------- constants ----------
today  = datetime.date.today().isoformat()
SEASON = today[:4]
SIMS   = 5000
LINE   = 6.5

DATA = Path(__file__).resolve().parent.parent / "data"
stats_df = (
    duckdb.connect(DATA / f"player_stats_{SEASON}.duckdb")
           .execute("SELECT player_id, COALESCE(k_rate,0.20) AS k FROM player_stats")
           .fetch_df().set_index("player_id")
)

def k_rate(pid: int, grp: str) -> float:
    val = stats_df['k'].get(pid)
    return val if val is not None else fetch_k_rate(pid, SEASON, grp) or 0.20

def name_to_id(full_name: str) -> int | None:
    """Map 'Gerrit Cole' → 592450 (MLBAM ID)."""
    if not full_name or pd.isna(full_name):
        return None
    last, first = full_name.split()[-1], " ".join(full_name.split()[:-1])
    res = playerid_lookup(last, first)
    return int(res['key_mlbam'].iloc[0]) if not res.empty else None

# ---------- pull schedule ----------
sched = schedule(today, today)   # DataFrame
if sched.empty:
    print(f"No MLB games on {today}.")
    exit()

rows = []
for _, row in sched.iterrows():
    for side in ('away', 'home'):
        team      = row[f'{side}_team_name']
        pitcher   = row[f'{side}_probable_pitcher']
        pid       = name_to_id(pitcher)

        if not pid:
            print(f"{team}: no probable starter in pybaseball feed"); continue

        team_id = int(row[f'{side}_id'])
        lineup  = get_yesterday_lineup(team_id)
        src_l   = "yesterday"
        if len(lineup) < 9:
            lineup = fallback_lineup(team_id); src_l = "roster"

        if len(lineup) < 9:
            print(f"{team} ({pitcher}): lineup incomplete"); continue

        # ---- Monte-Carlo Ks ----
        pks  = np.array([merge_prob(k_rate(pid,"pitching"), k_rate(b,"hitting"))
                         for b in lineup])
        sims = sim_many(pks, SIMS)
        expK, probK = sims.mean(), (sims >= LINE).mean()

        print(f"{team} ({pitcher}) [LU:{src_l}]: "
              f"ExpKs {expK:.2f} • P(K ≥ {LINE}) {probK:.3f}")

        rows.append({
            "team": team, "pitcher": pitcher, "LU_src": src_l,
            "expK": round(expK,2), f"P(K≥{LINE})": round(probK,3)
        })

# ---------- CSV ----------
if rows:
    df = pd.DataFrame(rows)
    out = DATA / f"today_k_proj_pybaseball_{today}.csv"
    df.to_csv(out, index=False)
    print(f"\nCSV saved → {out}")
else:
    print("\nNo projections generated (starters or lineups missing).")
