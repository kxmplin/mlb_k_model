#!/usr/bin/env python3
"""
today_proj.py
-------------
Project strikeout totals for games on a given date and save
with date embedded in the filename.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
import statsapi
from tqdm import tqdm

from k_pred_core import sim_many
from kpred_sim import fetch_k_rate

DEFAULT = 0.252
# Project directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
MODEL_DIR = BASE_DIR / 'models'


def load_calibration(pkl_path: Path):
    """
    Load linear calibration model from pickle and return slope, intercept.
    """
    import pickle
    with pkl_path.open('rb') as f:
        model = pickle.load(f)
    slope = float(model.coef_[0])
    intercept = float(model.intercept_)
    return slope, intercept


def fetch_schedule_from_db(db_path: Path, date: str) -> pd.DataFrame:
    con = duckdb.connect(str(db_path))
    try:
        df = con.execute(
            """
            SELECT game_id, away_pid, home_pid, away_lineup, home_lineup
            FROM main.schedule
            WHERE official_date = ?
            """, [date]
        ).fetchdf()
    except Exception:
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
        description='Project K totals for games on a given date 🚀'
    )
    parser.add_argument(
        '--date', type=str, help='Date YYYY-MM-DD; defaults to today 📅', required=False
    )
    parser.add_argument(
        '--line', type=float, default=6.5, help='Innings threshold for probability ⚾'
    )
    parser.add_argument(
        '--sims', type=int, default=10000, help='Number of simulation trials 🎲'
    )
    args = parser.parse_args()
    proj_date = args.date or pd.Timestamp.now().date().isoformat()

    # Load schedule
    sched_db = DATA_DIR / 'schedule.duckdb'
    sched = fetch_schedule_from_db(sched_db, proj_date)
    if sched.empty:
        print(f"No schedule in DB for {proj_date}, fetching live 🛰️")
        sched = fetch_schedule_live(proj_date)
    sched = sched.dropna(subset=['away_pid', 'home_pid', 'away_lineup', 'home_lineup'])
    if sched.empty:
        print(f"No valid games for {proj_date}. Exiting ❌")
        return

    # Load calibration from pickle
    lin_pkl = MODEL_DIR / 'mlb_exp_lin.pkl'
    if not lin_pkl.exists():
        raise FileNotFoundError(f"Calibration pickle not found: {lin_pkl}")
    slope, intercept = load_calibration(lin_pkl)
    print(f"🔄 Using calibration: E[K]_cal = {slope:.4f} * E[K]_raw + {intercept:.4f} 📈")

    outs_thresh = int(args.line * 3)
    results = []

    # Progress bar over each side
    games = list(sched.itertuples(index=False))
    total_sides = len(games) * 2
    pbar = tqdm(total=total_sides, desc='⏱️ Simulating sides ⚾', unit='side')

    for g in games:
        for side in ('away', 'home'):
            pid = int(getattr(g, f'{side}_pid'))
            lineup = [int(x) for x in getattr(g, f'{side}_lineup').split(',') if x]
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

    # Save today's projections with date embedded
    out_path = DATA_DIR / f'today_ks_proj_{proj_date}.csv'
    pd.DataFrame(results).to_csv(out_path, index=False)
    print(f"✅ Projections saved to {out_path}")

if __name__ == '__main__':
    main()
