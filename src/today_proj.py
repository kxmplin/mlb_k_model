#!/usr/bin/env python3
"""
today_proj.py
-------------
Project K strikeout totals for games on a given date, with a progress bar.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
import statsapi
from tqdm import tqdm
from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

DEFAULT = 0.252

def load_calibration(json_path: Path):
    with json_path.open('r') as f:
        params = json.load(f)
    return params.get('slope', 1.0), params.get('intercept', 0.0)

def fetch_schedule_from_db(duckdb_path: Path, date: str) -> pd.DataFrame:
    con = duckdb.connect(duckdb_path.as_posix())
    try:
        df = con.execute(
            """
            SELECT
              game_id,
              away_pid,
              home_pid,
              away_lineup,
              home_lineup
            FROM main.schedule
            WHERE official_date = ?
            """,
            [date]
        ).fetchdf()
    except duckdb.CatalogException:
        df = pd.DataFrame()
    finally:
        con.close()
    return df

def fetch_schedule_live(date: str) -> pd.DataFrame:
    rows = []
    resp = statsapi.get('schedule', {'sportId': 1, 'date': date})
    for day in resp.get('dates', []):
        for g in day['games']:
            state = g['status']['detailedState']
            if state not in ('Pre-Game', 'In Progress', 'Final'):
                continue
            pk = g['gamePk']
            box = statsapi.get('game_boxscore', {'gamePk': pk})
            rec = {'game_id': pk}
            for side in ('away', 'home'):
                pp = g['teams'][side].get('probablePitcher')
                rec[f'{side}_pid'] = pp.get('id') if pp else None
                order = box['teams'][side].get('battingOrder') or []
                def pid_from_raw(raw):
                    if isinstance(raw, int):
                        return raw
                    try:
                        return int(str(raw).replace('ID', ''))
                    except:
                        return None
                pids = [pid_from_raw(r) for r in order[:9] if pid_from_raw(r) is not None]
                if len(pids) < 9:
                    tmp = []
                    for pid_str, pdata in box['teams'][side].get('players', {}).items():
                        bo = pdata.get('battingOrder')
                        if bo:
                            try:
                                spot = int(str(bo).split('-')[0])
                                tmp.append((spot, int(pid_str.replace('ID', ''))))
                            except:
                                continue
                    tmp.sort()
                    pids = [pid for _, pid in tmp[:9]]
                rec[f'{side}_lineup'] = ",".join(str(pid) for pid in pids)
            rows.append(rec)
    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser(
        description='Project K totals for games on a given date.'
    )
    parser.add_argument(
        '--date', type=str, required=False,
        help='Date YYYY-MM-DD; defaults to today'
    )
    parser.add_argument(
        '--line', type=float, default=6.5,
        help='Innings threshold for probability'
    )
    parser.add_argument(
        '--sims', type=int, default=10000,
        help='Number of simulation trials'
    )
    args = parser.parse_args()
    proj_date = args.date or pd.Timestamp.now().date().isoformat()

    # Load schedule
    sched_db = Path(__file__).resolve().parent.parent / 'data' / 'schedule.duckdb'
    sched = fetch_schedule_from_db(sched_db, proj_date)
    if sched.empty:
        print(f"No schedule in DB for {proj_date}, fetching live.")
        sched = fetch_schedule_live(proj_date)
    else:
        print(f"Loaded schedule for {proj_date} from DB.")

    sched = sched.dropna(subset=['away_pid', 'home_pid', 'away_lineup', 'home_lineup'])
    if sched.empty:
        print(f"No valid games for {proj_date}. Exiting.")
        return

    # Load calibration
    slope, intercept = load_calibration(Path('models') / 'calibration.json')
    print(f"Using calibration: E[K]_cal = {slope:.4f} * E[K]_raw + {intercept:.4f}")

    outs_thresh = int(args.line * 3)
    results = []

    # Use tqdm to track progress across games and sides
    games = list(sched.itertuples(index=False))
    total = len(games) * 2  # away and home
    pbar = tqdm(total=total, desc='Simulating games', unit='side')

    for g in games:
        for side in ('away', 'home'):
            pid = int(getattr(g, f'{side}_pid'))
            lineup = [int(x) for x in getattr(g, f'{side}_lineup').split(',') if x]

            # fetch and simulate
            season = proj_date[:4]
            rp = fetch_k_rate(pid, season, 'pitcher') or DEFAULT
            batter_ps = [fetch_k_rate(b, season, 'batter') or DEFAULT for b in lineup]
            p_rates = np.array([rp] + batter_ps, dtype=float)
            sims = sim_many(p_rates, outs_thresh, args.sims)

            er_raw = float(sims.mean())
            pr_raw = float((sims >= outs_thresh).mean())
            er_cal = slope * er_raw + intercept

            results.append({
                'game_id': g.game_id,
                'side': side,
                'pitcher_id': pid,
                'exp_raw': round(er_raw, 2),
                'p_raw': round(pr_raw, 3),
                'exp_cal': round(er_cal, 2),
                'p_cal': round(pr_raw, 3),
            })
            pbar.update(1)

    pbar.close()

    # Save results
    out_path = Path(__file__).resolve().parent.parent / 'data' / 'today_ks_proj.csv'
    pd.DataFrame(results).to_csv(out_path, index=False)
    print(f"✅ Projections saved to {out_path}")

if __name__ == '__main__':
    main()
