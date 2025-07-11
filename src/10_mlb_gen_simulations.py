#!/usr/bin/env python3
"""
10_mlb_gen_simulations.py
==========================
For every recorded start in data/historical_ks.csv, run your simulator to
produce:

  • exp_ks   : mean K’s from N simulation runs  
  • p_over   : P(K ≥ line) from those sims  

Outputs data/historical_ks_sim.csv with one row per start:

 season, side, pitcher_id, k_actual, exp_ks, p_over
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

DEFAULT_K_RATE = 0.252  # fallback when no rate found

def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--hist", type=Path,
        default=Path("data/historical_ks.csv"),
        help="Your harvested starts from Step 5"
    )
    p.add_argument(
        "--sims", type=int, default=20000,
        help="Number of Monte-Carlo runs per start"
    )
    p.add_argument(
        "--line", type=float, default=6.5,
        help="Strike-out line L for computing P(K ≥ L)"
    )
    p.add_argument(
        "--out", type=Path,
        default=Path("data/historical_ks_sim.csv"),
        help="Where to write the simulated projections"
    )
    args = p.parse_args()

    df = pd.read_csv(args.hist)
    rows = []

    for _, r in df.iterrows():
        season   = int(r.season)
        pid      = int(r.pitcher_id)
        k_actual = int(r.k_actual)
        lineup   = [int(x) for x in str(r.lineup_ids).split(",")]

        # fetch pitcher rate, fallback if None
        raw_p = fetch_k_rate(pid, season, "pitcher")
        if raw_p is None:
            print(f"⚠️  Pitcher {pid} missing k_rate; using default {DEFAULT_K_RATE}")
            k_p = DEFAULT_K_RATE
        else:
            k_p = float(raw_p)

        # fetch batters rates, fallback
        ks_b = []
        for b in lineup:
            raw_b = fetch_k_rate(b, season, "batter")
            if raw_b is None:
                ks_b.append(DEFAULT_K_RATE)
            else:
                ks_b.append(float(raw_b))

        # build float array of probabilities
        pks_arr = np.array([k_p] + ks_b, dtype=float)

        # simulate: sim_many(pks_array, n_sims, line)
        sims = sim_many(pks_arr, args.sims, args.line)

        exp_ks = float(np.mean(sims))
        p_over = float(np.mean(sims >= args.line))

        rows.append({
            "season":     season,
            "side":       r.side,
            "pitcher_id": pid,
            "k_actual":   k_actual,
            "exp_ks":     exp_ks,
            "p_over":     p_over
        })

    out_df = pd.DataFrame(rows)
    args.out.parent.mkdir(exist_ok=True, parents=True)
    out_df.to_csv(args.out, index=False)
    print(f"✅ Saved {len(out_df)} simulated projections → {args.out}")

if __name__ == "__main__":
    main()
