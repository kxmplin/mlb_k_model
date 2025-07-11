#!/usr/bin/env python3
"""
build_historical_dataset.py
---------------------------

Harvest every regular-season start in 2024–25 and record:
  • season
  • side (“away”/“home”)
  • pitcher_id
  • k_actual      (total strike-outs that start)
  • lineup_ids    (comma-joined 9 batter IDs at first pitch)

Outputs:
  • data/historical_ks.csv
  • data/historical_ks.duckdb (table: historical_ks)
"""
import time
from pathlib import Path

import pandas as pd
import duckdb
import statsapi
from tqdm import tqdm

# ── CONFIG ───────────────────────────────────────────────────────────────────
SEASONS = ["2024", "2025"]
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
OUT_CSV = DATA_DIR / "historical_ks.csv"
OUT_DB  = DATA_DIR / "historical_ks.duckdb"


def season_schedule(year: str):
    return statsapi.get(
        "schedule",
        {
            "sportId": 1,
            "season": year,
            "gameTypes": "R",
        },
    )["dates"]


def outs_from_ip(ip_str: str) -> int:
    if not ip_str:
        return 0
    if "." in ip_str:
        w, f = ip_str.split(".")
        return int(w) * 3 + int(f)
    return int(ip_str) * 3


def starting_pitcher(players: dict) -> int | None:
    """
    Given the boxscore 'players' dict, returns the player ID of the starting pitcher.
    Tries in order:
      1) stats.pitching.gamesStarted >= 1
      2) gameStatus.isStarter == True
      3) pitcher with most outs recorded (IP * 3).
    """
    # 1) Look for any pitcher marked as having started
    for pid, pdata in players.items():
        pitch = pdata.get("stats", {}).get("pitching") or {}
        if pitch.get("gamesStarted", 0) >= 1:
            return int(pid.replace("ID", ""))

    # 2) Fallback: gameStatus flag
    for pid, pdata in players.items():
        if pdata.get("gameStatus", {}).get("isStarter"):
            return int(pid.replace("ID", ""))

    # 3) Last resort: highest IP
    best_pid, best_outs = None, -1
    for pid, pdata in players.items():
        pitch = pdata.get("stats", {}).get("pitching") or {}
        outs = outs_from_ip(pitch.get("inningsPitched", "0.0"))
        if outs > best_outs:
            best_outs, best_pid = outs, pid
    return int(best_pid.replace("ID", "")) if best_pid else None


def extract_lineup(box_side: dict) -> list[int]:
    """
    Given a box['teams'][side] dict, returns the nine-man batting order as ints.
    Handles both:
      - A list of strings like ['ID660271', 'ID518692', …]
      - A list of ints like [660271, 518692, …]
    Falls back to reading each player's battingOrder field if needed.
    """
    # Primary: MLB sometimes provides a battingOrder list
    order = box_side.get("battingOrder") or box_side.get("batters") or []
    if isinstance(order, list) and len(order) >= 9:
        lineup = []
        for pid in order[:9]:
            if isinstance(pid, int):
                lineup.append(pid)
            else:
                # strip 'ID' if present, then convert
                s = pid.replace("ID", "")
                try:
                    lineup.append(int(s))
                except ValueError:
                    continue
        return lineup

    # Fallback: inspect players for battingOrder metadata
    tmp = []
    for pid, pdata in box_side.get("players", {}).items():
        bo = pdata.get("battingOrder")
        if bo:
            try:
                spot = int(bo.split("-")[0])
                pid_int = int(pid.replace("ID", ""))
                tmp.append((spot, pid_int))
            except ValueError:
                continue
    tmp.sort()
    return [pid for _, pid in tmp[:9]] if len(tmp) >= 9 else []

# ── HARVEST ───────────────────────────────────────────────────────────────────
rows = []
skips = dict(not_final=0, box_err=0, no_sp=0, k_missing=0, bad_lineup=0)

print("⏳  Harvesting historical games…")
for yr in SEASONS:
    for d in tqdm(season_schedule(yr), desc=f"Season {yr}", unit="day"):
        for g in d["games"]:
            if g["status"]["detailedState"] != "Final":
                skips["not_final"] += 1
                continue

            gid = g["gamePk"]
            try:
                box = statsapi.get("game_boxscore", {"gamePk": gid})
            except Exception:
                skips["box_err"] += 1
                continue

            # starting pitchers for each side
            sp_away = starting_pitcher(box["teams"]["away"]["players"])
            sp_home = starting_pitcher(box["teams"]["home"]["players"])
            if not sp_away or not sp_home:
                skips["no_sp"] += 1
                continue

            for side, sp in zip(("away", "home"), (sp_away, sp_home)):
                pkey = f"ID{sp}"
                pitching = box["teams"][side]["players"].get(pkey, {}).get("stats", {}).get("pitching", {})
                k_act = pitching.get("strikeOuts")
                if k_act is None:
                    skips["k_missing"] += 1
                    continue

                lineup = extract_lineup(box["teams"][side])
                if len(lineup) != 9:
                    skips["bad_lineup"] += 1
                    continue

                rows.append({
                    "game_pk":    gid,
                    "date":       d["date"],
                    "season":     yr,
                    "side":       side,
                    "pitcher_id": sp,
                    "k_actual":   k_act,
                    "lineup_ids": ",".join(map(str, lineup)),
                })
            time.sleep(0.03)

print("Skip counts:", skips)

# ── SAVE ─────────────────────────────────────────────────────────────────────
if rows:
    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False)

    con = duckdb.connect(str(OUT_DB))
    con.register("hist_df", df)
    con.execute("CREATE OR REPLACE TABLE historical_ks AS SELECT * FROM hist_df;")
    con.close()

    print(f"\n✅  Saved {len(df):,} starts → {OUT_CSV.name} & {OUT_DB.name}")
else:
    print("⚠️  No rows harvested – inspect skip counts above to diagnose.")
