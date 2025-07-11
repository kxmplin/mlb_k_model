import argparse, math, numpy as np, statsapi
from k_pred_core import merge_prob, sim_many

def main() -> None:
    # ---------- CLI ----------
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025")
    parser.add_argument("--pitcher", type=int, required=True)
    parser.add_argument("--batters", required=True,
                        help="comma-sep nine batter IDs")
    parser.add_argument("--line", type=float, required=True)
    parser.add_argument("--sims", type=int, default=20_000)
    args = parser.parse_args()

    # --------- quick fetch helpers (inline) ----------
    def k_rate(pid, grp):
        try:
            js = statsapi.get("stats",
                    {"playerId": pid, "stats":"season",
                     "group":grp, "season": args.season})
            st = js["stats"][0]["splits"][0]["stat"]
            if grp=="pitching":
                bf = st.get("battersFaced") or 0
                return (st.get("strikeOuts") or 0)/bf if bf else .20
            else:
                pa = st.get("plateAppearances") or 0
                return (st.get("strikeOuts") or 0)/pa if pa else .20
        except Exception:
            return .20

    k_p   = k_rate(args.pitcher, "pitching")
    ks_b  = [k_rate(int(b), "hitting") for b in args.batters.split(",")]
    pks   = np.array([merge_prob(k_p, kb) for kb in ks_b])
    sims  = sim_many(pks, args.sims)

    print(f"Expected Ks: {sims.mean():.2f}")
    print(f"P(K ≥ {args.line}) = {(sims>=args.line).mean():.3f}")

if __name__ == "__main__":
    main()
