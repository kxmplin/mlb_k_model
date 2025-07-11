#!/usr/bin/env python3
""" 
Run Monte Carlo strikeout simulations over historical_ks.csv → historical_ks_sim.csv
Uses per-season DuckDB files (player_stats_2024.duckdb, player_stats_2025.duckdb) for K-rate lookups.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
import duckdb

# Fallback K-rate
FALLBACK_DEFAULT = 0.252

# Initialize DuckDB connection and attach season databases
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
# Paths to per-season DBs
DB_2024 = DATA_DIR / "player_stats_2024.duckdb"
DB_2025 = DATA_DIR / "player_stats_2025.duckdb"
# Output merged DB alias used here
con = duckdb.connect(database=':memory:')
# Attach each under alias
con.execute(f"ATTACH '{DB_2024}' AS db2024;")
con.execute(f"ATTACH '{DB_2025}' AS db2025;")


def get_rate(player_id: int, season: str) -> float:
    """
    Fetch k_rate for given player_id and season from the appropriate attached DB.
    Falls back to FALLBACK_DEFAULT if missing.
    """
    alias = 'db2024' if season == '2024' else 'db2025'
    try:
        q = ("SELECT k_rate FROM {alias}.player_stats "
             "WHERE player_id = ? LIMIT 1").format(alias=alias)
        res = con.execute(q, [player_id]).fetchone()
        return res[0] if res and res[0] is not None else FALLBACK_DEFAULT
    except Exception:
        return FALLBACK_DEFAULT


def sim_game(pks: np.ndarray, max_outs: int) -> int:
    """
    Simulate a single game: cycle through pks until outs reached.
    """
    k_count = 0
    outs = 0
    idx = 0
    n = len(pks)
    while outs < max_outs:
        if np.random.rand() < pks[idx]:
            k_count += 1
        outs += 1
        idx = (idx + 1) % n
    return k_count


def sim_many(pks: np.ndarray, sims: int, max_outs: int) -> np.ndarray:
    """
    Run multiple game simulations and return strikeout counts array.
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
    max_outs = int(args.line * 3)

    for _, r in tqdm(df.iterrows(), total=len(df), desc="Simulating starts"):
        season = r.season
        # pitcher first
        pid = int(r.pitcher_id)
        rp = get_rate(pid, season)
        # lineup
        lineup = [int(x) for x in r.lineup_ids.split(',')]
        batter_rates = [get_rate(b, season) for b in lineup]
        # build probs: pitcher then batters
        pks = np.array([rp] + batter_rates, dtype=float)
        sim = sim_many(pks, args.sims, max_outs)
        rows.append({
            **r.to_dict(),
            'exp_ks': float(sim.mean()),
            'p_over': float((sim >= max_outs).mean()),
            'p10': float(np.percentile(sim, 10)),
            'p90': float(np.percentile(sim, 90)),
        })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"✅ Generated {len(out_df):,} simulation summaries → {args.out}")

    # detach and close
    con.execute("DETACH db2024;")
    con.execute("DETACH db2025;")
    con.close()

if __name__ == "__main__":
    main()
