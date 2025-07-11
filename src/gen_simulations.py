#!/usr/bin/env python3
"""
gen_simulations.py
------------------
Attach per-season DuckDBs, sample from each starter’s k_rate and lineup, 
and simulate total Ks per start.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from tqdm import tqdm

FALLBACK = 0.252

def fetch_k_rate(con: duckdb.DuckDBPyConnection, pid: int, season: str, role: str) -> float:
    tbl = "stats.pitcher_stats" if role == "pitcher" else "stats.batter_stats"
    q = f"""
        SELECT k_rate
          FROM {tbl}
         WHERE season = ? AND player_id = ?
         LIMIT 1
    """
    res = con.execute(q, [season, pid]).fetchone()
    return res[0] if res else FALLBACK

def sim_one(p_rates, avg_outs):
    """Cycle through p_rates until we record avg_outs outs, counting Ks."""
    outs = 0
    ks   = 0
    idx  = 0
    n    = len(p_rates)
    while outs < avg_outs:
        p = p_rates[idx % n]
        if np.random.rand() < p:
            ks += 1
        # assume 3 outs per non-K? Better to sample per PA but this is a start
        outs += 1
        idx += 1
    return ks

def sim_many(p_rates, avg_outs, n_sims):
    return np.array([sim_one(p_rates, avg_outs) for _ in range(n_sims)])

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hist", default=Path("../data/historical_ks.csv"))
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--out", default=Path("../data/historical_ks_sim.csv"))
    args = p.parse_args()

    df = pd.read_csv(args.hist)
    # attach stats DB
    db_path = Path(__file__).resolve().parent.parent / "data" / "player_stats.duckdb"
    con = duckdb.connect(db_path.as_posix())

    out_rows = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Simulating starts"):
        season  = row.season
        pid     = int(row.pitcher_id)
        lineup  = [int(x) for x in row.lineup_ids.split(",")]

        # fetch rates
        rp       = fetch_k_rate(con, pid, season, "pitcher")
        batter_ps = [fetch_k_rate(con, b, season, "batter") for b in lineup]
        p_rates  = np.array([rp] + batter_ps, dtype=float)

        # avg_outs: convert recorded IP into outs
        avg_outs = int(float(row.k_actual))  # fallback: use actual Ks? better to pull IP

        sims     = sim_many(p_rates, avg_outs, args.sims)
        out_rows.append({
            **row.to_dict(),
            "exp_ks":   float(sims.mean()),
            "p_over":   float((sims >= np.mean(sims)).mean()),
            "10th_pct": float(np.percentile(sims, 10)),
            "90th_pct": float(np.percentile(sims, 90)),
        })

    pd.DataFrame(out_rows).to_csv(args.out, index=False)
    print(f"\n✅ Generated {len(out_rows):,} sim results → {args.out}")
    con.close()

if __name__ == "__main__":
    main()
