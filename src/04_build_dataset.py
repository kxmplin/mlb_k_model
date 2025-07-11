"""
Step 5-A – Compile historical K dataset (2024-25)
-------------------------------------------------
Harvest every regular-season game for 2024‑25, capturing:
  • starting‑pitcher ID (robust detection)
  • actual strike‑outs
  • nine‑man batting order at first pitch

Outputs: data/historical_ks.csv and .duckdb.
"""

from pathlib import Path
import time

import duckdb
import pandas as pd
import statsapi
from tqdm import tqdm

# ---------------------------- CONFIG ------------------------------------
SEASONS = ["2024", "2025"]
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

OUT_CSV = DATA_DIR / "historical_ks.csv"
OUT_DB = DATA_DIR / "historical_ks.duckdb"

# -------------------------- HELPERS -------------------------------------

def season_schedule(year: str):
    return statsapi.get(
        "schedule",
        {"sportId": 1, "season": year, "gameTypes": "R"},
    )["dates"]


def outs_from_ip(ip: str) -> int:
    if not ip:
        return 0
    if "." in ip:
        whole, frac = ip.split(".")
        return int(whole) * 3 + int(frac)
    return int(ip) * 3


def starting_pitcher(players: dict) -> int | None:
    """Return starter id using gamesStarted, isStarter, else max outs."""
    # gamesStarted
    for pid, p in players.items():
        gs = p.get("stats", {}).get("pitching", {}).get("gamesStarted", 0)
        if gs:
            return int(pid.replace("ID", ""))
    # isStarter flag
    for pid, p in players.items():
        if p.get("gameStatus", {}).get("isStarter"):
            return int(pid.replace("ID", ""))
    # max outs
    best_id, best_outs = None, -1
    for pid, p in players.items():
        outs = outs_from_ip(p.get("stats", {}).get("pitching", {}).get("inningsPitched", "0"))
        if outs > best_outs:
            best_id, best_outs = pid, outs
    return int(best_id.replace("ID", "")) if best_id else None


def _pid_to_int(x):
    return int(x) if isinstance(x, int) else int(x.replace("ID", ""))


def extract_lineup(box_side: dict) -> list[int]:
    order = box_side.get("battingOrder", [])
    if len(order) >= 9:
        return [_pid_to_int(pid) for pid in order[:9]]
    # fallback via player objects
    spots = []
    for pid, pdata in box_side["players"].items():
        bo = pdata.get("battingOrder")
        if bo:
            try:
                spot = int(bo.split("-")[0])
                spots.append((spot, _pid_to_int(pid)))
            except ValueError:
                continue
    spots.sort()
    return [pid for _, pid in spots[:9]] if len(spots) >= 9 else []

# --------------------------- HARVEST ------------------------------------
rows = []
skip = {k: 0 for k in ["not_final", "box_err", "no_sp", "k_missing", "bad_lineup"]}
print("⏳  Harvesting historical games…")

for yr in SEASONS:
    for d in tqdm(season_schedule(yr), desc=f"Season {yr}"):
        gdate = d["date"]
        for g in d["games"]:
            if g["status"]["detailedState"] != "Final":
                skip["not_final"] += 1
                continue
            gid = g["gamePk"]
            try:
                box = statsapi.get("game_boxscore", {"gamePk": gid})
            except Exception:
                skip["box_err"] += 1
                continue

            sp_away = starting_pitcher(box["teams"]["away"]["players"])
            sp_home = starting_pitcher(box["teams"]["home"]["players"])
            if not sp_away or not sp_home:
                skip["no_sp"] += 1
                continue

            for side, sp in zip(("away", "home"), (sp_away, sp_home)):
                pkey = f"ID{sp}"
                k_act = (
                    box["teams"][side]["players"].get(pkey, {})
                    .get("stats", {})
                    .get("pitching", {})
                    .get("strikeOuts")
                )
                if k_act is None:
                    skip["k_missing"] += 1
                    continue

                lineup = extract_lineup(box["teams"][side])
                if len(lineup) != 9:
                    skip["bad_lineup"] += 1
                    continue

                rows.append(
                    {
                        "game_pk": gid,
                        "date": gdate,
                        "park_id": g["venue"]["id"],
                        "season": yr,
                        "side": side,
                        "pitcher_id": sp,
                        "k_actual": k_act,
                        "lineup_ids": ",".join(map(str, lineup)),
                    }
                )
            time.sleep(0.02)

print("Skip counts:", skip)

# ---------------------------- SAVE --------------------------------------
if rows:
    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False)
    con = duckdb.connect(OUT_DB)
    con.register("hist_df", df)
    con.execute("CREATE TABLE IF NOT EXISTS historical_ks AS SELECT * FROM hist_df")
    con.close()
    print(f"\n✅  Saved {len(df):,} starts → {OUT_CSV.name} & {OUT_DB.name}")
else:
    print("⚠️  Still zero rows – inspect skip counts above to diagnose.")
