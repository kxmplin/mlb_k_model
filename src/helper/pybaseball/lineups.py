"""
pybaseball.lineups – confirmed starting lineups via MLB StatsAPI
----------------------------------------------------------------
get_lineup(game_pk, side)  → list[int] (9 player IDs)

• Uses live feed: https://statsapi.mlb.com/api/v1.1/game/<gamePk>/feed/live
• Returns empty list if lineup not yet posted.
"""

import requests

def get_lineup(game_pk: int, side: str) -> list[int]:
    """
    Parameters
    ----------
    game_pk : int   MLB game ID
    side    : str   "home" | "away"

    Returns
    -------
    list[int]  length 9 (player IDs) or [] if unavailable
    """
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    data = requests.get(url, timeout=8).json()

    box = data["liveData"]["boxscore"]["teams"].get(side, {})
    order = box.get("battingOrder", [])
    if len(order) >= 9:
        return [int(pid) for pid in order[:9]]
    return []
