import argparse, sys, duckdb, numpy as np, statsapi
from pathlib import Path
from k_pred_core import merge_prob, sim_many

# -------- helper: fetch season K-rate --------
def fetch_k_rate(pid: int, season: str, group: str) -> float | None:
    try:
        js = statsapi.get("stats",
                          {"playerId": pid, "stats": "season",
                           "group": group, "season": season})
        splits = js["stats"][0]["splits"]
        if not splits:
            return None
        stat = splits[0]["stat"]
        if group == "pitching":
            bf = stat.get("battersFaced") or 0
            k  = stat.get("strikeOuts")   or 0
            return k / bf if bf else None
        else:
            pa = stat.get("plateAppearances") or 0
            k  = stat.get("strikeOuts")       or 0
            return k / pa if pa else None
    except Exception:
        return None

# -------- run-once wrapper --------
def main() -> None:
    # ---------- CLI ----------
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2025")
    ap.add_argument("--pitcher", type=int, required=True)
    ap.add_argument("--batters", required=True,
                    help="comma-sep nine batter IDs")
    ap.add_argument("--sims", type=int, default=10_000)
    args = ap.parse_args()

    BASE = Path(__file__).resolve().parent.parent
    DATA = BASE / "data"
    con  = duckdb.connect(DATA / f"player_stats_{args.season}.duckdb")
    df   = con.execute(
            "SELECT player_id, COALESCE(k_rate,0.20) AS k "
            "FROM player_stats").fetch_df().set_index("player_id")

    # ----- helpers -----
    def upsert(pid, k_rate):
        if pid in df.index:
            return
        df.loc[pid] = k_rate
        con.execute("INSERT INTO player_stats (player_id, k_rate) VALUES (?, ?)",
                    [pid, k_rate])

    name_cache = {}
    def full_name(pid):
        if pid not in name_cache:
            try:
                name_cache[pid] = statsapi.get(
                    "people", {"personIds": pid})["people"][0]["fullName"]
            except Exception:
                name_cache[pid] = f"ID{pid}"
        return name_cache[pid]

    # ----- pitcher -----
    if args.pitcher in df.index:
        k_p = df.at[args.pitcher, "k"]
    else:
        k_p = fetch_k_rate(args.pitcher, args.season, "pitching") or 0.20
        upsert(args.pitcher, k_p)
        print(f"⚠️  Added pitcher {full_name(args.pitcher)} "
              f"({args.pitcher}) k_rate={k_p:.3f}")

    # ----- batters -----
    ids = [int(x) for x in args.batters.split(",")]
    if len(ids) != 9:
        sys.exit("Need exactly 9 batter IDs")

    ks_b = []
    for bid in ids:
        if bid in df.index:
            ks_b.append(df.at[bid, "k"])
        else:
            kr = fetch_k_rate(bid, args.season, "hitting") or 0.20
            upsert(bid, kr)
            print(f"⚠️  Added batter {full_name(bid)} "
                  f"({bid}) k_rate={kr:.3f}")
            ks_b.append(kr)
    ks_b = np.array(ks_b)

    # ----- Monte-Carlo -----
    pks = np.array([merge_prob(k_p, kb) for kb in ks_b])
    ks  = sim_many(pks, args.sims)

    print(f"\nPitcher  : {full_name(args.pitcher)}")
    print(f"Expected Ks (mean of {args.sims:,} sims): {ks.mean():.2f}")
    for line in (4.5, 5.5, 6.5, 7.5):
        print(f"P(K ≥ {line}) = {(ks >= line).mean():.3f}")

    con.close()

# -------- guard --------
if __name__ == "__main__":
    main()
