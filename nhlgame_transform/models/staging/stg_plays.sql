{{ config(materialized='table') }}

with raw as (
    SELECT
        filename,
        unnest(plays) as play
    FROM read_json(
        's3://nhl-raw/play-by-play/*/play-by-play.json',
        format = 'auto',
        union_by_name = true
    )
),
parsed as (
    SELECT
        -- game context from filename: play-by-play/{game_id}/play-by-play.json
        split_part(replace(filename, 's3://nhl-raw/', ''), '/', 2)::integer as game_id,
        (play->>'eventId')::integer                         as event_id,
        (play->'periodDescriptor'->>'number')::integer      as period,
        (play->'periodDescriptor'->>'periodType')           as period_type,
        play->>'timeInPeriod'                               as time_in_period,
        play->>'timeRemaining'                              as time_remaining,
        play->>'typeDescKey'                                as event_type,
        (play->>'sortOrder')::integer                       as sort_order,
        play->>'situationCode'                              as situation_code,
        play->'details'                                     as details
    FROM raw
)

SELECT * FROM parsed
