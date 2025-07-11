import argparse, math
from pathlib import Path
import duckdb, numpy as np, pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from k_pred_core import merge_prob, sim_many
from kpred_sim   import fetch_k_rate           # uses the helper we just fixed

# ---------- CLI ----------
ap = argparse.ArgumentParser()
ap.add_argument("--season_min", default="2024")
ap.add_argument("--season_max", default="2025")
ap.add_argument("--sims", type=int, default=1000)
ap.add_argument("--line", type=float, default=6.5)
args = ap.parse_args()

# ---------- paths ----------
BASE  = Path(__file__).resolve().parent.parent
DATA  = BASE / "data"
plots = DATA / "plots"; plots.mkdir(exist_ok=True, parents=True)

# ---------- load historical starts ----------
hist = (
    duckdb.connect(DATA / "historical_ks.duckdb")
    .execute(
        "SELECT * FROM historical_ks WHERE season BETWEEN ? AND ?",
        [args.season_min, args.season_max],
    )
    .fetch_df()
)
print(f"📊  Evaluating {len(hist):,} starts…")

# ---------- cache per-season stats ----------
cache = {}
def season_df(s):
    if s not in cache:
        cache[s] = (
            duckdb.connect(DATA / f"player_stats_{s}.duckdb")
            .execute(
                "SELECT player_id, COALESCE(k_rate,0.20) AS k "
                "FROM player_stats"
            )
            .fetch_df()
            .set_index("player_id")
        )
    return cache[s]

# ---------- main loop ----------
rows = []
for _, row in tqdm(hist.iterrows(), total=len(hist)):
    df = season_df(row.season)

    k_p = df.at[row.pitcher_id, "k"] if row.pitcher_id in df.index \
          else fetch_k_rate(row.pitcher_id, row.season, "pitching") or 0.20

    ks_b = []
    for bid in map(int, row.lineup_ids.split(",")):
        ks_b.append(
            df.at[bid, "k"]
            if bid in df.index else
            fetch_k_rate(bid, row.season, "hitting") or 0.20
        )

    pks  = np.array([merge_prob(k_p, kb) for kb in ks_b])
    sims = sim_many(pks, args.sims)

    rows.append({
        "k_actual": row.k_actual,
        "k_pred"  : sims.mean(),
        "prob_over": (sims >= args.line).mean(),
    })

pred = pd.DataFrame(rows)
pred.to_csv(DATA / "eval_predictions.csv", index=False)
print("✅  eval_predictions.csv written")

# ---------- metrics ----------
rmse  = math.sqrt(np.mean((pred.k_pred - pred.k_actual)**2))
mae   = np.mean(np.abs(pred.k_pred - pred.k_actual))
brier = np.mean((pred.prob_over - (pred.k_actual >= args.line))**2)
print(f"RMSE {rmse:.2f}  •  MAE {mae:.2f}  •  Brier {brier:.3f}")

# ---------- plots ----------
plt.figure()
plt.scatter(pred.k_pred, pred.k_actual, s=6, alpha=0.4)
plt.plot([0,15],[0,15],'--')
plt.xlabel("Pred Ks"); plt.ylabel("Actual Ks")
plt.title("Ks: predicted vs actual")
plt.savefig(plots / "scatter_k.png", dpi=150)

plt.figure()
pred["bucket"] = pd.qcut(pred.prob_over, 10, labels=False)
cal = pred.groupby("bucket").apply(
        lambda g: pd.Series({
            "pred": g.prob_over.mean(),
            "emp" : (g.k_actual >= args.line).mean()}))
plt.plot(cal.pred, cal.emp, 'o-'); plt.plot([0,1],[0,1],'--')
plt.xlabel("Pred prob ≥ line"); plt.ylabel("Empirical hit-rate")
plt.title(f"Calibration (line {args.line})")
plt.savefig(plots / "calibration.png", dpi=150)

print(f"📉  Plots saved in {plots}/")
