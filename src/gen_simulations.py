#!/usr/bin/env python3
""" 
Run Monte Carlo strikeout simulations over historical_ks.csv → historical_ks_sim.csv
Improvements:
  - Sequence plate appearances through the nine-man batting order
  - Use fixed outs threshold (line innings * 3)
  - Replace external sim_many with built-in sim_many that cycles batters
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from kpred_sim import fetch_k_rate

FALLBACK_DEFAULT = 0.252


def sim_game(pks: np.ndarray, max_outs: int) -> int:
    """
    Simulate a single game. Cycle through pks array in order,
    each PA always produces exactly one out (strikeout or not)
    until reaching max_outs; count strikeouts.
    """
    k_count = 0
    outs = 0
    idx = 0
    n_batters = len(pks)
    while outs < max_outs:
        # each plate appearance yields one out
        if np.random.rand() < pks[idx]:
            k_count += 1
        outs += 1
        idx = (idx + 1) % n_batters
    return k_count


def sim_many(pks: np.ndarray, sims: int, max_outs: int) -> np.ndarray:
    """
    Run multiple game simulations and return array of strikeout counts.
    """
    results = np.empty(sims, dtype=int)
    for i in range(sims):
        results[i] = sim_game(pks, max_outs)
    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hist", default=Path("../data/historical_ks.csv"))
    p.add_argument("--sims", type=int, default=20000)
    p.add_argument("--line", type=float, default=6.5,
                   help="Innings threshold for starter depth")
    p.add_argument("--out", default=Path("../data/historical_ks_sim.csv"))
    args = p.parse_args()

    df = pd.read_csv(args.hist)
    rows = []

    # convert innings to outs threshold
    max_outs = int(args.line * 3)

    for _, r in tqdm(df.iterrows(), total=len(df), desc="Simulating starts"):
        pid = int(r.pitcher_id)
        season = r.season
        lineup = [int(x) for x in r.lineup_ids.split(",")]

        # fetch k rates for pitcher and batters
        rp = fetch_k_rate(pid, season, "pitcher") or FALLBACK_DEFAULT
        batter_rates = [fetch_k_rate(b, season, "batter") or FALLBACK_DEFAULT
                        for b in lineup]
        # build probability array in order: starter then lineup
        pks = np.array([rp] + batter_rates, dtype=float)

        # run simulations cycling through batting order
        sim = sim_many(pks, args.sims, max_outs)

        rows.append({
            **r.to_dict(),
            "exp_ks": float(sim.mean()),
            "p_over": float((sim >= max_outs).mean()),
            "p10": float(np.percentile(sim, 10)),
            "p90": float(np.percentile(sim, 90)),
        })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"✅ Generated {len(out_df):,} simulation summaries → {args.out}")


if __name__ == "__main__":
    main()
