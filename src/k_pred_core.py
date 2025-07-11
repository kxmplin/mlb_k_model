"""
Minimal k-Pred port
-------------------
• merge_prob()  – same log-odds fusion we used earlier
• sim_game()    – one start, looping through the lineup
• sim_many()    – N Monte-Carlo games → array of K totals
"""

import math, random
import numpy as np

def merge_prob(k_pitcher: float, k_batter: float, league=0.20) -> float:
    """Combine pitcher & batter K% in log-odds space (same as kPred)."""
    def logit(p): return math.log(p / (1 - p))
    merged = logit(k_pitcher) + logit(k_batter) - logit(league)
    return 1 / (1 + math.exp(-merged))

def sim_game(pks: np.ndarray, outs_lambda: float = 18) -> int:
    """
    • outs ~ Poisson(outs_lambda)   (18 ≈ 6 IP)
    • plate appearances ≈ 1.15 × outs
    • walk through the 9-man lineup modulo 9
    """
    outs = np.random.poisson(outs_lambda)
    pas  = int(outs * 1.15)
    order_idx = np.random.randint(0, 9, pas)
    return np.random.binomial(1, pks[order_idx]).sum()

def sim_many(pks: np.ndarray, n: int = 10_000, outs_lambda: float = 18) -> np.ndarray:
    return np.fromiter((sim_game(pks, outs_lambda) for _ in range(n)), dtype=int, count=n)
