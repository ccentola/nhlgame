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

print_results(
    "stg_players_history: sample rows",
    """
    SELECT
        player_id,
        team_abbrev,
        snapshot_date,
        full_name,
        position_code,
        player_group,
        sweater_number
    FROM main.stg_players_history
    ORDER BY snapshot_date DESC, team_abbrev, full_name
    LIMIT 20
    """
)

print_results(
    "stg_players_history: snapshot summary",
    """
    SELECT
        snapshot_date,
        team_abbrev,
        count(*) as player_count
    FROM main.stg_players_history
    GROUP BY snapshot_date, team_abbrev
    ORDER BY snapshot_date DESC, team_abbrev
    LIMIT 20
    """
)

print_results(
    "stg_players: latest snapshot sample",
    """
    SELECT
        player_id,
        team_abbrev,
        full_name,
        position_code,
        sweater_number,
        birth_country
    FROM main.stg_players
    ORDER BY team_abbrev, full_name
    LIMIT 20
    """
)

print_results(
    "stg_standings: current standings",
    """
    SELECT
        league_rank,
        team_abbrev,
        team_name,
        conference_abbrev,
        division_abbrev,
        games_played,
        wins,
        losses,
        ot_losses,
        points,
        round(point_pct, 3)     as point_pct,
        goal_differential,
        streak_code || streak_count::varchar as streak
    FROM main.stg_standings
    ORDER BY league_rank
    """
)

print_results(
    "stg_standings_history: snapshot count per date",
    """
    SELECT
        snapshot_date,
        count(*)                as teams
    FROM main.stg_standings_history
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    """
)
