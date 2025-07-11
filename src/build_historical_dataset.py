#!/usr/bin/env python3
"""
Harvest every start (2024–25) into data/historical_ks.csv
Columns: season,side,pitcher_id,k_actual,lineup_ids
"""
import time
from pathlib import Path
import duckdb, pandas as pd, statsapi
from tqdm import tqdm

SEASONS = ["2024","2025"]
OUT_CSV = Path("../data/historical_ks.csv")

def outs_from_ip(ip):
    w, f = (ip.split(".") if "." in ip else (ip,0))
    return int(w)*3 + int(f)

def starting_pitcher(players):
    # same as before...
    ...

def extract_lineup(side):
    # same as before...
    ...

rows=[]
for yr in SEASONS:
    for d in tqdm(statsapi.get("schedule",{"sportId":1,"season":yr,"gameTypes":"R"})["dates"],desc=yr):
        for g in d["games"]:
            if g["status"]["detailedState"]!="Final":
                continue
            box = statsapi.get("game_boxscore",{"gamePk":g["gamePk"]})
            for side in ("away","home"):
                sp = starting_pitcher(box["teams"][side]["players"])
                if not sp: continue
                k_act = box["teams"][side]["players"][f"ID{sp}"]["stats"]["pitching"]["strikeOuts"]
                lineup = extract_lineup(box["teams"][side])
                if len(lineup)!=9: continue
                rows.append({
                  "season": yr,
                  "side": side,
                  "pitcher_id": sp,
                  "k_actual": k_act,
                  "lineup_ids": ",".join(map(str,lineup))
                })
            time.sleep(0.02)

pd.DataFrame(rows).to_csv(OUT_CSV,index=False)
print("✅ Saved",len(rows),"starts →",OUT_CSV)
