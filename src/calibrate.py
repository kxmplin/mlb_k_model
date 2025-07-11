#!/usr/bin/env python3
"""
calibrate.py
------------
Fit a linear calibration between simulated K expectations (exp_ks)
and actual strikeouts (k_actual), then plot and save the calibration.
"""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

def main():
    p = argparse.ArgumentParser(
        description="Calibrate simulated K projections against actuals"
    )
    p.add_argument(
        "--sim", type=Path, default=Path("../data/historical_ks_sim.csv"),
        help="CSV with columns k_actual and exp_ks"
    )
    p.add_argument(
        "--outdir", type=Path, default=Path("models"),
        help="Directory to save calibration plot and JSON"
    )
    args = p.parse_args()

    # Load your simulation results
    df = pd.read_csv(args.sim)
    if not {"k_actual", "exp_ks"}.issubset(df.columns):
        raise ValueError("Input CSV must have 'k_actual' and 'exp_ks' columns")

    # Prepare X and y
    X = df[["exp_ks"]].to_numpy()        # shape (n_samples, 1)
    y = df["k_actual"].to_numpy()        # shape (n_samples,)

    # Fit linear model
    lr = LinearRegression()
    lr.fit(X, y)
    slope = lr.coef_[0]
    intercept = lr.intercept_
    print(f"Calibration result: k_actual ≈ {slope:.4f}·exp_ks + {intercept:.4f}")

    # Ensure output directory exists
    args.outdir.mkdir(parents=True, exist_ok=True)

    # Plot calibration
    plt.figure(figsize=(6, 6))
    plt.scatter(df["exp_ks"], df["k_actual"], alpha=0.3, label="Historical starts")
    # regression line
    xs = np.linspace(df["exp_ks"].min(), df["exp_ks"].max(), 100).reshape(-1, 1)
    ys = lr.predict(xs)
    plt.plot(xs.flatten(), ys, linewidth=2, label="Fit: slope×exp_ks + intercept")
    plt.xlabel("Simulated E[K]")
    plt.ylabel("Actual K")
    plt.title("Calibration of Simulated vs. Actual Strikeouts")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Save plot and JSON
    plot_path = args.outdir / "cal_exp_ks.png"
    json_path = args.outdir / "calibration.json"
    plt.savefig(plot_path)
    plt.close()
    print(f"✅  Saved plot → {plot_path}")

    # Save parameters
    params = {"slope": float(slope), "intercept": float(intercept)}
    with json_path.open("w") as f:
        json.dump(params, f, indent=2)
    print(f"✅  Saved parameters → {json_path}")

if __name__ == "__main__":
    main()
