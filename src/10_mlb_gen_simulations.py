#!/usr/bin/env python3
"""
10_mlb_gen_simulations.py
==========================
For every recorded start in data/historical_ks.csv, run your simulator to
produce:

  • exp_ks   : mean Ks from Monte Carlo
  • p_over   : P(K ≥ line) from those sims

If a pitcher’s k_rate is missing in your DB, fall back to:
  1) their avg exp_ks from data/historical_ks_sim.csv ÷ 9
  2) GLOBAL_DEFAULT

Usage:
  python src/10_mlb_gen_simulations.py \
    --sims 20000 \
    --line 6.5 \
    --fallback data/historical_ks_sim.csv
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

GLOBAL_DEFAULT = 0.252  # final fallback

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hist", type=Path,
                   default=Path("data/historical_ks.csv"),
                   help="Harvested starts from Step 5")
    p.add_argument("--fallback", type=Path,
                   default=Path("data/historical_ks_sim.csv"),
                   help="Historical sims CSV (pitcher_id, exp_ks)")
    p.add_argument("--sims", type=int, default=20000,
                   help="Number of Monte Carlo runs per start")
    p.add_argument("--line", type=float, default=6.5,
                   help="Strike-out line L for computing P(K ≥ L)")
    p.add_argument("--out", type=Path,
                   default=Path("data/historical_ks_sim.csv"),
                   help="Where to write the simulated projections")
    args = p.parse_args()

    # ── build per-pitcher fallback rates from your historical sims ──
    if args.fallback.exists():
        fb = pd.read_csv(args.fallback)
        if "pitcher_id" not in fb.columns or "exp_ks" not in fb.columns:
            raise KeyError(
                f"{args.fallback} must contain columns 'pitcher_id' and 'exp_ks'"
            )
        hist = fb.groupby("pitcher_id")["exp_ks"].mean().to_dict()
        # convert to per-batter k_rate
        hist_rate = {pid: val / 9.0 for pid, val in hist.items()}
    else:
        print(f"⚠️  No fallback file at {args.fallback}, using GLOBAL_DEFAULT only")
        hist_rate = {}

    # ── load harvested starts ─────────────────────────────────────────────
    df = pd.read_csv(args.hist)
    rows = []

    for _, r in df.iterrows():
        season   = int(r.season)
        pid      = int(r.pitcher_id)
        k_actual = int(r.k_actual)
        lineup   = [int(x) for x in str(r.lineup_ids).split(",")]

        # 1) try DB rate
        raw_p = fetch_k_rate(pid, season, "pitcher")
        if raw_p is not None:
            k_p = float(raw_p)
        # 2) then hist fallback
        elif pid in hist_rate:
            k_p = hist_rate[pid]
        # 3) else global default
            print(f"⚠️  Pitcher {pid} using hist fallback {k_p:.3f}")
        else:
            k_p = GLOBAL_DEFAULT

        # batters: DB rate or global default
        ks_b = []
        for b in lineup:
            rb = fetch_k_rate(b, season, "batter")
            ks_b.append(float(rb) if rb is not None else GLOBAL_DEFAULT)

        # assemble float array
        pks_arr = np.array([k_p] + ks_b, dtype=float)

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
