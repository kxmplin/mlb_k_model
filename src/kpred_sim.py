import duckdb

DB_PATH = "../data/player_stats.duckdb"  # adjust if your DB lives elsewhere

def fetch_k_rate(player_id: int, season: str, group: str) -> float | None:
    """
    Look up k_rate for pitcher or batter from your DuckDB.
    Tables: pitcher_stats(season, player_id, k_rate)
            batter_stats (same schema)
    """
    tbl = "pitcher_stats" if group == "pitcher" else "batter_stats"
    con = duckdb.connect(DB_PATH)
    q = f"""
      SELECT k_rate
      FROM {tbl}
      WHERE season = '{season}' AND player_id = {player_id}
      LIMIT 1
    """
    res = con.execute(q).fetchone()
    return res[0] if res else None
