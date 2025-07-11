#!/usr/bin/env python3
"""
combine_player_stats.py
-----------------------

Combine per-season DuckDB files into a single player_stats.duckdb for simulations.

Usage (from project root):
  python src/combine_player_stats.py \
    2024 2025

This will look for data/player_stats_{season}.duckdb for each season
and merge their 'player_stats' tables into data/player_stats.duckdb.
"""
import sys
from pathlib import Path
import duckdb

def main():
    # Seasons passed as args
    seasons = sys.argv[1:]
    if not seasons:
        print("Usage: python combine_player_stats.py <season1> [<season2> ...]")
        sys.exit(1)

    # Project structure
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    out_db = data_dir / "player_stats.duckdb"

    # Initialize output DB
    con = duckdb.connect(str(out_db))
    con.execute("PRAGMA threads=4;")
    # Create combined table if not exists
    con.execute(
        "CREATE TABLE IF NOT EXISTS player_stats ("
        "season VARCHAR, player_id INTEGER, player_role VARCHAR,"
        "k_total INTEGER, opportunities INTEGER, k_rate DOUBLE)"
    )

    for season in seasons:
        input_db = data_dir / f"player_stats_{season}.duckdb"
        if not input_db.exists():
            print(f"⚠️  File not found: {input_db}")
            continue

        alias = f"db_{season}"
        con.execute(f"ATTACH '{input_db}' AS {alias}")
        # Check table presence
        exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema='{alias}' AND table_name='player_stats'"
        ).fetchone()[0]
        if not exists:
            print(f"⚠️  No 'player_stats' table in {input_db.name}")
            con.execute(f"DETACH {alias}")
            continue

        # Append records
        con.execute(
            f"INSERT INTO player_stats "
            f"SELECT * FROM {alias}.player_stats;"
        )
        print(f"✅  Appended stats from season {season}")
        con.execute(f"DETACH {alias}")

    # Final count
    total = con.execute("SELECT count(*) FROM player_stats").fetchone()[0]
    print(f"🏁 Total rows in merged 'player_stats': {total}")
    con.close()

if __name__ == '__main__':
    main()
