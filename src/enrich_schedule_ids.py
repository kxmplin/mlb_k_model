"""
enrich_schedule_ids.py
======================
Read schedule_<date>.duckdb, fill missing *prob_id* columns by
matching full names against players_<YEAR>, and write:
  • schedule_enriched_<date>.csv
  • schedule_enriched_<date>.duckdb          (table = schedule)
"""

import sys, datetime, pathlib, duckdb, pandas as pd, unicodedata, re

# ── date / file paths ────────────────────────────────────────────────────
if len(sys.argv) > 1:
    date_iso = sys.argv[1]
else:
    from zoneinfo import ZoneInfo
    date_iso = datetime.datetime.now(ZoneInfo("America/Detroit")).date().isoformat()

SEASON = date_iso[:4]
DATA   = pathlib.Path(__file__).resolve().parent.parent / "data"
CACHE  = DATA / "cache"
src_db = CACHE / f"schedule_{date_iso}.duckdb"
if not src_db.exists():
    sys.exit("Run cache_schedule.py first.")

dst_db = CACHE / f"schedule_enriched_{date_iso}.duckdb"
dst_csv= CACHE / f"schedule_enriched_{date_iso}.csv"

# ── load schedule DF ─────────────────────────────────────────────────────
dcon = duckdb.connect(src_db); df = dcon.table("schedule").df(); dcon.close()

# ── build name→ID map from players file(s) ───────────────────────────────
def _norm(n:str)->str: return re.sub(r"\s+"," ",unicodedata.normalize("NFKD",n)
                                    .encode("ascii","ignore").decode().lower().strip())
csv_p = DATA / f"players_{SEASON}.csv"
duck_p= DATA / f"players_{SEASON}.duckdb"
if csv_p.exists():
    pl = pd.read_csv(csv_p, dtype={"player_id":int})
elif duck_p.exists():
    pl = duckdb.connect(duck_p).execute("SELECT player_id, full_name FROM players").fetch_df()
else:
    pl = pd.DataFrame(columns=["player_id","full_name"])

name2id = {_norm(r.full_name): int(r.player_id) for _,r in pl.iterrows()}

def fill(col_name_id: str, col_name_name: str):
    mask = df[col_name_id].isna() & df[col_name_name].notna()
    df.loc[mask, col_name_id] = df.loc[mask, col_name_name].map(lambda n: name2id.get(_norm(n)))

fill("away_prob_id","away_prob_name")
fill("home_prob_id","home_prob_name")

# ── write outputs ────────────────────────────────────────────────────────
df.to_csv(dst_csv, index=False)
duckdb.connect(dst_db).execute("CREATE OR REPLACE TABLE schedule AS SELECT * FROM df").close()
print(f"✅ enriched file written → {dst_csv.name} & {dst_db.name}")
