import duckdb
from pathlib import Path

def fetch_k_rate(player_id: int, season: str, group: str) -> float | None:
    """
    Look up k_rate for pitcher or batter from per-season DuckDB.
    Falls back to combined player_stats.duckdb if per-season DB or table is missing.
    """
    # Determine per-season DB path
    season_db = Path(__file__).resolve().parent.parent / "data" / f"player_stats_{season}.duckdb"
    combined_db = Path(__file__).resolve().parent.parent / "data" / "player_stats.duckdb"
    # Choose DB: per-season if exists, else combined
    db_path = season_db if season_db.exists() else combined_db

    # Choose table
    tbl = "stats.pitcher_stats" if group == "pitcher" else "stats.batter_stats"
    # If per-season DB has no schema, try unqualified table name
    fallback_tbl = tbl

    con = duckdb.connect(db_path.as_posix())
    try:
        # First attempt: schema-qualified
        res = con.execute(
            f"SELECT k_rate FROM {tbl} WHERE season = ? AND player_id = ? LIMIT 1",
            [season, player_id]
        ).fetchone()
        if res:
            return res[0]
    except duckdb.CatalogException:
        # Try without schema prefix
        try:
            table_name = tbl.split(".", 1)[1]  # e.g., "pitcher_stats"
            res = con.execute(
                f"SELECT k_rate FROM {table_name} WHERE season = ? AND player_id = ? LIMIT 1",
                [season, player_id]
            ).fetchone()
            if res:
                return res[0]
        except duckdb.CatalogException:
            pass

    # As ultimate fallback, if combined has a default table
    if db_path == combined_db:
        # Try generic player_stats table
        try:
            res = con.execute(
                "SELECT k_rate FROM player_stats WHERE season = ? AND player_id = ? LIMIT 1",
                [season, player_id]
            ).fetchone()
            if res:
                return res[0]
        except duckdb.CatalogException:
            pass

    con.close()
    return None
