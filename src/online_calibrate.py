#!/usr/bin/env python3
"""
online_calibrate.py
-------------------
Merge cached predictions with actual results and retrain calibrators.

Inputs:
  • data/cached_predictions.csv  (from cache_predictions.py)
  • data/historical_ks.csv       (ground truth: k_actual per start)
Outputs:
  • models/mlb_exp_lin.pkl
  • models/mlb_p_over_iso.pkl
  • plots/cal_exp_ks_online.png
  • plots/cal_p_over_online.png
"""
import argparse
import pickle
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import calibration_curve

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pred",     default=Path("data/cached_predictions.csv"))
    p.add_argument("--truth",    default=Path("data/historical_ks.csv"))
    p.add_argument("--line",     type=float, default=6.5)
    p.add_argument("--models",   default=Path("models"))
    p.add_argument("--plots",    default=Path("plots"))
    args = p.parse_args()

    # load
    pred  = pd.read_csv(args.pred)
    truth = pd.read_csv(args.truth)

    # unify column names: historical_ks.csv has game_pk, predictions use game_id
    pred = pred.rename(columns={"game_id":"game_pk"})
    # merge on game_pk + side + pitcher_id
    df = pred.merge(
        truth[["game_pk","side","pitcher_id","k_actual"]],
        on=["game_pk","side","pitcher_id"],
        how="inner"
    ).dropna(subset=["exp_raw","k_actual","p_raw"])

    # make sure output dirs exist
    args.models.mkdir(exist_ok=True, parents=True)
    args.plots.mkdir(exist_ok=True, parents=True)

    # 1) linear calibration exp_raw → k_actual
    X = df[["exp_raw"]].values
    y = df["k_actual"].values
    lin = LinearRegression().fit(X, y)

    xs = np.linspace(df.exp_raw.min(), df.exp_raw.max(), 100)
    plt.scatter(df.exp_raw, df.k_actual, s=8, alpha=0.3)
    plt.plot(xs, lin.predict(xs.reshape(-1,1)), c="C1")
    plt.xlabel("simulated exp_raw")
    plt.ylabel("actual k_actual")
    plt.title("Online MLB Ks Calibration (exp)")
    plt.savefig(args.plots / "cal_exp_ks_online.png")
    plt.clf()

    # 2) isotonic for P(K ≥ line)
    y_bin = (df.k_actual >= args.line).astype(int)
    iso   = IsotonicRegression(out_of_bounds="clip").fit(df["p_raw"], y_bin)

    prob_true, prob_pred = calibration_curve(y_bin, df["p_raw"], n_bins=10)
    plt.plot(prob_pred, prob_true, marker="o")
    plt.plot([0,1],[0,1],"--", c="gray")
    plt.xlabel(f"simulated P(K≥{args.line})")
    plt.ylabel("observed freq")
    plt.title("Online MLB Ks Calibration (prob)")
    plt.savefig(args.plots / "cal_p_over_online.png")
    plt.clf()

    # save pickles
    with open(args.models / "mlb_exp_lin.pkl", "wb") as f:
        pickle.dump(lin, f)
    with open(args.models / "mlb_p_over_iso.pkl", "wb") as f:
        pickle.dump(iso, f)

    print("✅ Re‐trained calibrators →", args.models)

if __name__ == "__main__":
    main()
