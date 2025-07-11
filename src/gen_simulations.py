#!/usr/bin/env python3
"""
Run sim_many over historical_ks.csv → historical_ks_sim.csv
"""
import argparse
from pathlib import Path
import numpy as np, pandas as pd
from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

FALLBACK_DEFAULT = 0.252

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hist",     default=Path("../data/historical_ks.csv"))
    p.add_argument("--sims",     type=int, default=20000)
    p.add_argument("--line",     type=float,default=6.5)
    p.add_argument("--out",      default=Path("../data/historical_ks_sim.csv"))
    args = p.parse_args()

    df = pd.read_csv(args.hist)
    rows=[]
    for _,r in df.iterrows():
        pid=int(r.pitcher_id); season=r.season
        lineup=[int(x) for x in r.lineup_ids.split(",")]
        rp=fetch_k_rate(pid,season,"pitcher") or FALLBACK_DEFAULT
        ks_b=[fetch_k_rate(b,season,"batter") or FALLBACK_DEFAULT for b in lineup]
        pks=np.array([rp]+ks_b,dtype=float)
        sim=sim_many(pks,args.sims,args.line)
        rows.append({
          **r.to_dict(),
          "exp_ks": float(sim.mean()),
          "p_over": float((sim>=args.line).mean())
        })
    pd.DataFrame(rows).to_csv(args.out,index=False)
    print("✅ Generated sims →",args.out)

if __name__=="__main__":
    main()
