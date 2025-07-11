"""
pybaseball.probables  – Probable starters via MLB StatsAPI
==========================================================
schedule_and_probables(start_date, end_date=None, sport_id=1)
→ DataFrame with columns
       game_id
       away_player_id      away_probable_pitcher
       home_player_id      home_probable_pitcher
"""

from __future__ import annotations
import pandas as pd, statsapi
from typing import Dict, Any
from .utils import sanitize_date_range


def _probables_for_day(date_iso: str, sport_id: int = 1) -> Dict[int, Dict[str, Any]]:
    """
    Returns dict keyed by game_id with sub-dict:
      { 'away_player_id': …, 'away_probable_pitcher': …,
        'home_player_id': …, 'home_probable_pitcher': … }
    """
    result: dict[int, dict[str, Any]] = {}
    payload = statsapi.get(
        "schedule",
        {"sportId": sport_id, "date": date_iso, "hydrate": "probablePitchers"},
    )

    for d in payload.get("dates", []):
        for g in d["games"]:
            gid = g["gamePk"]
            entry = {
                "game_id": gid,
                "away_player_id": pd.NA,
                "away_probable_pitcher": "TBA",
                "home_player_id": pd.NA,
                "home_probable_pitcher": "TBA",
            }
            for side in ("away", "home"):
                prob = g.get("probablePitchers", {}).get(side)
                if prob:
                    entry[f"{side}_player_id"] = prob["id"]
                    entry[f"{side}_probable_pitcher"] = prob["fullName"]
            result[gid] = entry
    return result


def schedule_and_probables(start_date, end_date=None, sport_id: int = 1) -> pd.DataFrame:
    """
    Parameters
    ----------
    start_date, end_date : str | datetime-like
    sport_id             : 1 = MLB
    """
    # accept datetime.date objects
    if not isinstance(start_date, str):
        start_date = start_date.isoformat()
    if end_date is not None and not isinstance(end_date, str):
        end_date = end_date.isoformat()

    start, end = sanitize_date_range(start_date, end_date)
    rows: list[dict] = []
    for day in pd.date_range(start, end):
        rows.extend(_probables_for_day(day.date().isoformat(), sport_id).values())

    return pd.DataFrame(rows, columns=[
        "game_id",
        "away_player_id", "away_probable_pitcher",
        "home_player_id", "home_probable_pitcher",
    ])


__all__ = ["schedule_and_probables"]
