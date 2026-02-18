{{ config(materialized='table') }}

with raw as (
    select
        split_part(filename, '/', 5)::date          as snapshot_date,
        unnest(standings)                           as team
    from read_json(
        's3://{{ var("minio_bucket") }}/standings/*/standings.json',
        format = 'auto',
        union_by_name = true
    )
)

select
    team['teamAbbrev']['default']                   as team_abbrev,
    snapshot_date,
    team['seasonId']::integer                       as season_id,
    team['teamName']['default']                     as team_name,
    team['teamCommonName']['default']               as team_common_name,
    team['conferenceName']                          as conference_name,
    team['conferenceAbbrev']                        as conference_abbrev,
    team['divisionName']                            as division_name,
    team['divisionAbbrev']                          as division_abbrev,
    team['leagueSequence']::integer                 as league_rank,
    team['conferenceSequence']::integer             as conference_rank,
    team['divisionSequence']::integer               as division_rank,
    team['gamesPlayed']::integer                    as games_played,
    team['wins']::integer                           as wins,
    team['losses']::integer                         as losses,
    team['otLosses']::integer                       as ot_losses,
    team['points']::integer                         as points,
    team['pointPctg']::double                       as point_pct,
    team['regulationWins']::integer                 as regulation_wins,
    team['regulationPlusOtWins']::integer           as regulation_ot_wins,
    team['goalFor']::integer                        as goals_for,
    team['goalAgainst']::integer                    as goals_against,
    team['goalDifferential']::integer               as goal_differential,
    team['homeWins']::integer                       as home_wins,
    team['homeLosses']::integer                     as home_losses,
    team['homeOtLosses']::integer                   as home_ot_losses,
    team['homePoints']::integer                     as home_points,
    team['roadWins']::integer                       as road_wins,
    team['roadLosses']::integer                     as road_losses,
    team['roadOtLosses']::integer                   as road_ot_losses,
    team['roadPoints']::integer                     as road_points,
    team['l10Wins']::integer                        as l10_wins,
    team['l10Losses']::integer                      as l10_losses,
    team['l10OtLosses']::integer                    as l10_ot_losses,
    team['l10Points']::integer                      as l10_points,
    team['streakCode']                              as streak_code,
    team['streakCount']::integer                    as streak_count,
    team['wildcardSequence']::integer               as wildcard_sequence,
    team['teamLogo']                                as team_logo_url
from raw
