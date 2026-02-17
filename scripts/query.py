import os
import duckdb

db_path = os.getenv("DBT_DB_PATH", "nhlgame.duckdb")
con = duckdb.connect(db_path)


def print_results(title, query):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)
    results = con.execute(query).fetchall()
    description = con.execute(query).description
    headers = [d[0] for d in description]
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in results), default=0)) for i, h in enumerate(headers)]
    header_row = "  ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    print(header_row)
    print("-" * len(header_row))
    for row in results:
        print("  ".join(f"{str(v):<{col_widths[i]}}" for i, v in enumerate(row)))
    print(f"\n{len(results)} row(s)")


print_results(
    "stg_games: sample rows",
    """
    SELECT
        game_id,
        game_date,
        away_team_abbrev,
        away_score,
        home_score,
        home_team_abbrev,
        last_period_type,
        game_state
    FROM main.stg_games
    ORDER BY game_date DESC
    LIMIT 10
    """
)

print_results(
    "stg_games: summary",
    """
    SELECT
        count(*)                as total_games,
        min(game_date)          as earliest_game,
        max(game_date)          as latest_game,
        count(*) filter (where last_period_type = 'REG')    as reg,
        count(*) filter (where last_period_type = 'OT')     as ot,
        count(*) filter (where last_period_type = 'SO')     as so
    FROM main.stg_games
    """
)

print_results(
    "stg_plays: event type breakdown",
    """
    SELECT
        event_type,
        count(*)                                    as total_events,
        count(*) filter (where period = 1)          as p1,
        count(*) filter (where period = 2)          as p2,
        count(*) filter (where period = 3)          as p3
    FROM main.stg_plays
    GROUP BY event_type
    ORDER BY total_events DESC
    """
)
