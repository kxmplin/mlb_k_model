import numpy as np
import math

def sim_game(pks: np.ndarray, outs_lambda: float) -> int:
    """
    Simulate one full 'start':
      • pks: array of strikeout probabilities per PA
      • outs_lambda: target total outs (line * 3)
    Returns total Ks recorded.
    """
    outs_target = int(outs_lambda * 3)
    outs = ks = 0
    while outs < outs_target:
        i = np.random.randint(len(pks))
        if np.random.rand() < pks[i]:
            ks += 1
        else:
            outs += 1
    return ks

def sim_many(pks: np.ndarray, n: int, outs_lambda: float) -> np.ndarray:
    """
    Run `sim_game` n times.
    """
    return np.fromiter(
        (sim_game(pks, outs_lambda) for _ in range(n)),
        dtype=int,
        count=n
    )
