{{ config(materialized='table') }}

with forwards as (
    select
        split_part(filename, '/', 5)                as team_abbrev,
        split_part(filename, '/', 6)::date          as snapshot_date,
        unnest(forwards)                            as player,
        'forward'                                   as player_group
    from read_json(
        's3://{{ var("minio_bucket") }}/rosters/*/*/roster.json',
        format = 'auto',
        union_by_name = true
    )
),

defensemen as (
    select
        split_part(filename, '/', 5)                as team_abbrev,
        split_part(filename, '/', 6)::date          as snapshot_date,
        unnest(defensemen)                          as player,
        'defenseman'                                as player_group
    from read_json(
        's3://{{ var("minio_bucket") }}/rosters/*/*/roster.json',
        format = 'auto',
        union_by_name = true
    )
),

goalies as (
    select
        split_part(filename, '/', 5)                as team_abbrev,
        split_part(filename, '/', 6)::date          as snapshot_date,
        unnest(goalies)                             as player,
        'goalie'                                    as player_group
    from read_json(
        's3://{{ var("minio_bucket") }}/rosters/*/*/roster.json',
        format = 'auto',
        union_by_name = true
    )
),

unioned as (
    select * from forwards
    union all
    select * from defensemen
    union all
    select * from goalies
)

select
    player['id']::integer                           as player_id,
    team_abbrev,
    snapshot_date,
    player_group,
    player['firstName']['default']                  as first_name,
    player['lastName']['default']                   as last_name,
    player['firstName']['default']
        || ' ' ||
    player['lastName']['default']                   as full_name,
    player['sweaterNumber']::integer                as sweater_number,
    player['positionCode']                          as position_code,
    player['shootsCatches']                         as shoots_catches,
    player['heightInInches']::integer               as height_inches,
    player['weightInPounds']::integer               as weight_pounds,
    player['birthDate']::date                       as birth_date,
    player['birthCity']['default']                  as birth_city,
    player['birthCountry']                          as birth_country,
    player['headshot']                              as headshot_url
from unioned
