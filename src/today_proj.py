#!/usr/bin/env python3
"""
today_proj.py
-------------
On-the-day projection: uses your calibrated linear parameters to
output exp_ks & p(K≥line) per upcoming start. Falls back to live
statsapi pulls if your schedule DuckDB is empty or missing entries.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
import statsapi
from k_pred_core import sim_many
from kpred_sim  import fetch_k_rate

DEFAULT = 0.252

def load_calibration(json_path: Path):
    with json_path.open("r") as f:
        params = json.load(f)
    return params.get("slope", 1.0), params.get("intercept", 0.0)

def fetch_schedule_from_db(duckdb_path: Path, date: str) -> pd.DataFrame:
    con = duckdb.connect(duckdb_path.as_posix())
    try:
        df = con.execute(
            """
            SELECT
              game_id,
              away_probable_pitcher AS away_pid,
              home_probable_pitcher AS home_pid,
              away_lineup,
              home_lineup
            FROM schedule
            WHERE official_date = ?
            """,
            [date]
        ).fetchdf()
    except duckdb.CatalogException:
        df = pd.DataFrame()
    finally:
        con.close()
    return df

def fetch_schedule_live(date: str) -> pd.DataFrame:
    """Pull today's games + lineups live from statsapi."""
    sched = []
    for day in statsapi.get("schedule", {"sportId":1, "date": date})["dates"]:
        for g in day["games"]:
            if g["status"]["detailedState"] != "Final" and g["status"]["detailedState"] != "Pre-Game":
                continue
            box = statsapi.get("game_boxscore", {"gamePk": g["gamePk"]})
            rec = {"game_id": g["gamePk"]}
            for side in ("away", "home"):
                # probable pitcher
                pid = g["teams"][side]["probablePitcher"]
                rec[f"{side}_pid"] = pid and pid.get("id")
                # lineup
                order = box["teams"][side].get("battingOrder") or []
                # convert ['ID123','ID456',...] to [123,456,...]
                rec[f"{side}_lineup"] = ",".join(str(int(s.replace("ID","")))
                                                 for s in order[:9])
            sched.append(rec)
    return pd.DataFrame(sched)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--season", required=True,
                   help="Date in YYYY-MM-DD format")
    p.add_argument("--line", type=float, default=6.5,
                   help="Innings threshold for P(K ≥ line)")
    p.add_argument("--sims", type=int, default=10000,
                   help="Number of Monte Carlo trials per start")
    args = p.parse_args()

    # 1) Try DB first
    db_path = Path(__file__).resolve().parent.parent / "data" / "schedule.duckdb"
    sched = fetch_schedule_from_db(db_path, args.season)

    # 2) Fallback to live if empty
    if sched.empty:
        print(f"No entries in schedule DB for {args.season}, pulling live from statsapi…")
        sched = fetch_schedule_live(args.season)

    # Drop incomplete
    sched = sched.dropna(subset=["away_pid","home_pid","away_lineup","home_lineup"])
    if sched.empty:
        print(f"No valid games found for {args.season}; exiting.")
        return

    # Load calibration
    slope, intercept = load_calibration(Path("models")/"calibration.json")
    print(f"Calibration: E[K]_cal = {slope:.4f}×E[K]_raw + {intercept:.4f}")

    # Attach stats DB
    stats_db = Path(__file__).resolve().parent.parent / "data" / "player_stats.duckdb"
    con_stats = duckdb.connect(stats_db.as_posix())

    rows = []
    outs_thresh = int(args.line * 3)
    for _, g in sched.iterrows():
        for side in ("away","home"):
            pid = int(g[f"{side}_pid"])
            lineup = [int(x) for x in g[f"{side}_lineup"].split(",") if x]

            # fetch K rates
            rp = fetch_k_rate(con_stats, pid, args.season[:4], "pitcher") or DEFAULT
            batter_ps = [fetch_k_rate(con_stats, b, args.season[:4], "batter") or DEFAULT
                         for b in lineup]
            p_rates = np.array([rp] + batter_ps, dtype=float)

            sims = sim_many(p_rates, outs_thresh, args.sims)
            er_raw = sims.mean()
            pr_raw = (sims >= outs_thresh).mean()
            er_cal = slope * er_raw + intercept

            rows.append({
                "game_id":    g.game_id,
                "side":       side,
                "pitcher_id": pid,
                "exp_raw":    round(er_raw,2),
                "p_raw":      round(pr_raw,3),
                "exp_cal":    round(er_cal,2),
                "p_cal":      round(pr_raw,3),
            })

    con_stats.close()

    out = Path("../data/today_ks_proj.csv")
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"✅ Projections → {out}")

if __name__ == "__main__":
    main()
