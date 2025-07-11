#!/usr/bin/env python3
"""
Fit linear + isotonic calibrators on your historical sims.
"""
import argparse,pickle
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import calibration_curve

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--sim_csv", default=Path("../data/historical_ks_sim.csv"))
    p.add_argument("--line",    type=float, default=6.5)
    p.add_argument("--outdir",  default=Path("models"))
    args=p.parse_args()

    df=pd.read_csv(args.sim_csv).dropna(subset=["exp_ks","k_actual","p_over"])
    # linear
    X=df[["exp_ks"]]; y=df.k_actual
    lin=LinearRegression().fit(X,y)
    xs=np.linspace(X.min()[0],X.max()[0],100)
    plt.scatter(X,y,s=5,alpha=0.3)
    plt.plot(xs,lin.predict(xs.reshape(-1,1)))
    plt.savefig(args.outdir/"cal_exp_ks.png"); plt.clf()
    pickle.dump(lin,open(args.outdir/"mlb_exp_lin.pkl","wb"))

    # isotonic
    yb=(df.k_actual>=args.line).astype(int)
    iso=IsotonicRegression(out_of_bounds="clip").fit(df.p_over,yb)
    prob_true,prob_pred=calibration_curve(yb,df.p_over,n_bins=10)
    plt.plot(prob_pred,prob_true,"o--"); plt.savefig(args.outdir/"cal_p_over.png")
    pickle.dump(iso,open(args.outdir/"mlb_p_over_iso.pkl","wb"))

    print("✅ Calibrated models →",args.outdir)

if __name__=="__main__":
    main()
