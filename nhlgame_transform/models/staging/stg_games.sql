{{ config(materialized='table') }}

with raw as (
    select *
    from read_json(
        's3://nhl-raw/play-by-play/*/play-by-play.json',
        format = 'auto',
        union_by_name = true
    )
)

select
    id                                              as game_id,
    season,
    gameType                                        as game_type,
    gameDate::date                                  as game_date,
    startTimeUTC::timestamptz                       as start_time_utc,
    gameState                                       as game_state,
    gameScheduleState                               as game_schedule_state,

    -- venue
    venue['default']                                as venue,
    venueLocation['default']                        as venue_location,

    -- away team
    awayTeam['id']::integer                         as away_team_id,
    awayTeam['abbrev']                              as away_team_abbrev,
    awayTeam['commonName']['default']               as away_team_name,
    awayTeam['score']::integer                      as away_score,
    awayTeam['sog']::integer                        as away_sog,

    -- home team
    homeTeam['id']::integer                         as home_team_id,
    homeTeam['abbrev']                              as home_team_abbrev,
    homeTeam['commonName']['default']               as home_team_name,
    homeTeam['score']::integer                      as home_score,
    homeTeam['sog']::integer                        as home_sog,

    -- outcome
    gameOutcome['lastPeriodType']                   as last_period_type,
    regPeriods                                      as reg_periods,
    otInUse                                         as ot_in_use,
    shootoutInUse                                   as shootout_in_use,

    -- derived
    away_score > home_score                         as away_win,
    home_score > away_score                         as home_win,
    abs(home_score - away_score)                    as goal_diff

from raw
