#!/usr/bin/env python3
"""
07_historical_projections.py
============================

For every historical start in data/historical_ks.duckdb, simulate the
expected strikeouts and P(K ≥ line), then save a CSV with actual vs.
predicted for calibration.

Usage:
    python src/07_historical_projections.py \
        --seasons 2024 2025 \
        --line 6.5 \
        --sims 2000 \
        --out data/historical_projections.csv
"""
import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

from k_pred_core import merge_prob, sim_many
from kpred_sim   import fetch_k_rate

# ─── CLI ───────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument(
    "--seasons", nargs="+", default=["2024", "2025"],
    help="Seasons to include in the historical dataset"
)
parser.add_argument(
    "--line", type=float, default=6.5,
    help="Strikeout line for probability (e.g. 6.5)"
)
parser.add_argument(
    "--sims", type=int, default=2000,
    help="Number of Monte Carlo simulations per start"
)
parser.add_argument(
    "--out", type=Path,
    default=Path("data/historical_projections.csv"),
    help="Path to write the output CSV"
)
args = parser.parse_args()

# ─── Load historical K dataset ─────────────────────────────────────────────
hist_db = Path("data/historical_ks.duckdb")
if not hist_db.exists():
    raise FileNotFoundError(f"{hist_db} not found – run Step 5-A first")

con = duckdb.connect(str(hist_db))
hist_df = con.execute("SELECT * FROM historical_ks").fetch_df()
con.close()

# ─── Simulate each start with progress bar ─────────────────────────────────
rows = []
for _, row in tqdm(hist_df.iterrows(),
                   total=len(hist_df),
                   desc="Simulating historical starts"):
    season       = str(row.season)
    game_pk      = row.game_pk
    date         = row.date
    pid          = int(row.pitcher_id)
    lineup_ids   = [int(x) for x in row.lineup_ids.split(",")]
    actual_k     = int(row.k_actual)

    # pitcher K-rate
    k_p = fetch_k_rate(pid, season, "pitching") or 0.20

    # batter K-rates & merge into matchup probabilities
    pks = []
    for bid in lineup_ids:
        k_b = fetch_k_rate(bid, season, "hitting") or 0.20
        pks.append(merge_prob(k_p, k_b))
    pks = np.array(pks)

    # Monte Carlo
    sims = sim_many(pks, args.sims)
    exp_k = sims.mean()
    p_over = (sims >= args.line).mean()

    rows.append({
        "season"        : season,
        "date"          : date,
        "game_pk"       : game_pk,
        "pitcher_id"    : pid,
        "lineup_ids"    : ",".join(map(str, lineup_ids)),
        "actual_k"      : actual_k,
        "exp_k"         : exp_k,
        f"p_k_≥{args.line}": p_over,
    })

# ─── Save CSV ──────────────────────────────────────────────────────────────
out_df = pd.DataFrame(rows)
args.out.parent.mkdir(parents=True, exist_ok=True)
out_df.to_csv(args.out, index=False)
print(f"✅ Saved {len(out_df)} historical projections → {args.out}")
