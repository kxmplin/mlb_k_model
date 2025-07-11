#!/usr/bin/env python3
"""
gen_simulations.py
Simulate historical starts using date-specific schedule files.
"""
import argparse
from pathlib import Path
import duckdb
import pandas as pd
from tqdm import tqdm
from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

def main():
    p=argparse.ArgumentParser()
    p.add_argument('date',help='YYYY-MM-DD')
    p.add_argument('--sims',type=int,default=10000)
    args=p.parse_args(); d=args.date; sims=args.sims
    base=Path(__file__).resolve().parent.parent
    sched_db=base/'data'/d/f'schedule_{d}.duckdb'
    con=duckdb.connect(str(sched_db))
    sched=con.execute('SELECT game_id,away_pid,home_pid,away_lineup,home_lineup FROM main.schedule',).fetchdf()
    con.close()
    out_rows=[]; total=len(sched)*2; pbar=tqdm(total=total,desc='Sims sides')
    for _,g in sched.iterrows():
        for side in ['away','home']:
            pid=int(g[f'{side}_pid']); lineup=list(map(int,g[f'{side}_lineup'].split(',')))
            rp=fetch_k_rate(pid,d[:4],'pitcher')
            bp=[fetch_k_rate(b,d[:4],'batter') for b in lineup]
            prates=[rp or 0.252]+[b or 0.252 for b in bp]
            outs=27;  # maybe parameterize
            res=sim_many(prates,outs,sims)
            out_rows.append({'game_id':g['game_id'],'side':side,'mean_k':res.mean(),'p_k':(res>=outs).mean()})
            pbar.update(1)
    pbar.close()
    df=pd.DataFrame(out_rows)
    df.to_csv(base/'data'/f'sim_results_{d}.csv',index=False)
    print(f"✅ Simulations saved to sim_results_{d}.csv")

if __name__=='__main__': main()
