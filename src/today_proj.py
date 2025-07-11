#!/usr/bin/env python3
"""
On-the-day projection: uses your calibrated pickle models to
output exp_ks & p(K≥line) per upcoming start.
"""
import argparse,pickle
from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

DEFAULT=0.252

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--season", type=str, required=True)
    p.add_argument("--line",   type=float,default=6.5)
    p.add_argument("--sims",   type=int,  default=10000)
    args=p.parse_args()

    # load schedule + lineups via statsapi DuckDB
    con=duckdb.connect("../data/schedule.duckdb")
    sched=con.execute(f"""
      SELECT game_id, away_probable_pitcher AS away_pid,
             home_probable_pitcher AS home_pid,
             away_lineup, home_lineup
      FROM schedule WHERE official_date='{args.season}'
    """).fetchdf()

    # load calibrators
    lin   = pickle.load(open("models/mlb_exp_lin.pkl","rb"))
    iso   = pickle.load(open("models/mlb_p_over_iso.pkl","rb"))

    rows=[]
    for _,g in sched.iterrows():
      for side in ("away","home"):
        pid = g[f"{side}_pid"]
        lineup = list(map(int,g[f"{side}_lineup"].split(",")))
        rp=fetch_k_rate(pid,args.season,"pitcher") or DEFAULT
        ks_b=[fetch_k_rate(b,args.season,"batter") or DEFAULT for b in lineup]
        pks=np.array([rp]+ks_b,dtype=float)
        sim=sim_many(pks,args.sims,args.line)
        er=sim.mean(); pr=(sim>=args.line).mean()
        rows.append({
          "game_id":g.game_id,"side":side,"pitcher_id":pid,
          "exp_raw":round(er,2),"p_raw":round(pr,3),
          "exp_cal":round(float(lin.predict([[er]])),2),
          "p_cal":round(float(iso.predict([pr])[0]),3)
        })
    pd.DataFrame(rows).to_csv("../data/today_ks_proj.csv",index=False)
    print("✅ Today's projections → data/today_ks_proj.csv")

if __name__=="__main__":
    main()
