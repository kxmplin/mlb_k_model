#!/usr/bin/env python3
"""
10_mlb_calibrate.py
===================
Fit calibration models on your historical strike-out simulations.

Inputs:
  • data/historical_ks.csv          ← raw actual Ks per start
  • data/historical_ks_sim.csv      ← your Monte-Carlo outputs, one row per start, with
       season, side, pitcher_id, k_actual, exp_ks, p_over_L
Outputs (in models/):
  • mlb_exp_lin.pkl       ← LinearRegression mapping exp_ks → actual k_actual
  • mlb_p_over_L_iso.pkl  ← IsotonicRegression mapping p_over_L → Pr(actual ≥ L)
  • plots/cal_exp_ks.png, plots/cal_p_over_L.png
"""
import argparse
import pickle
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# requires scikit-learn
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.isotonic import IsotonicRegression
    from sklearn.calibration import calibration_curve
except ImportError as e:
    raise ImportError("Please pip install scikit-learn") from e

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ks_csv",   type=Path,
                   default=Path("data/historical_ks.csv"),
                   help="Actual Ks per start")
    p.add_argument("--sim_csv",  type=Path,
                   default=Path("data/historical_ks_sim.csv"),
                   help="Simulated exp_ks & p_over_L per start")
    p.add_argument("--line",     type=float, default=6.5,
                   help="Strike-out line L for P(K ≥ L)")
    p.add_argument("--outdir",   type=Path, default=Path("models"),
                   help="Where to write pickles and plots")
    args = p.parse_args()

    # load
    actual = pd.read_csv(args.ks_csv)
    sim    = pd.read_csv(args.sim_csv)
    df = actual.merge(sim, on=["season","side","pitcher_id"], how="inner")

    # drop missing
    df = df.dropna(subset=["exp_ks","k_actual","p_over"])

    # make sure our sim file uses p_over for this line
    # (if your sim uses a column named p_over_6_5 etc, rename it to p_over)
    # e.g. sim.rename(columns={f"p_over_{args.line}":"p_over"}, inplace=True)

    X = df[["exp_ks"]].values
    y = df["k_actual"].values

    # 1) linear calibration: k_actual ≃ a · exp_ks + b
    lin = LinearRegression().fit(X, y)
    print("Linear: actual ≃", lin.coef_[0], "× exp_ks +", lin.intercept_)

    # plot
    xs = np.linspace(df.exp_ks.min(), df.exp_ks.max(), 100)
    plt.scatter(df.exp_ks, df.k_actual, s=8, alpha=0.3)
    plt.plot(xs, lin.predict(xs.reshape(-1,1)), c="C1")
    plt.xlabel("simulated exp_ks")
    plt.ylabel("actual k_actual")
    plt.title("MLB Ks Calibration")
    args.outdir.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.outdir / "cal_exp_ks.png")
    plt.clf()

    # 2) isotonic for P(K ≥ L)
    y_bin = (df.k_actual >= args.line).astype(int)
    iso   = IsotonicRegression(out_of_bounds="clip")\
                .fit(df["p_over"], y_bin)

    prob_true, prob_pred = calibration_curve(y_bin, df["p_over"], n_bins=10)
    plt.plot(prob_pred, prob_true, marker="o")
    plt.plot([0,1],[0,1],"--", c="gray")
    plt.xlabel("simulated P(K≥%.1f)"%args.line)
    plt.ylabel("observed freq")
    plt.title(f"P(K≥{args.line}) Calibration")
    plt.savefig(args.outdir / "cal_p_over.png")
    plt.clf()

    # save models
    with open(args.outdir / "mlb_exp_lin.pkl", "wb") as f:
        pickle.dump(lin, f)
    with open(args.outdir / "mlb_p_over_iso.pkl", "wb") as f:
        pickle.dump(iso, f)

    print("✅ Saved calibration models & plots →", args.outdir)

if __name__ == "__main__":
    main()
