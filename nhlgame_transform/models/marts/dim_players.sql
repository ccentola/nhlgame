{{ config(materialized='table') }}

with players as (
    select * from {{ ref('stg_players') }}
),

enriched as (
    select
        player_id,
        team_abbrev,
        full_name,
        first_name,
        last_name,
        position_code,
        player_group,
        sweater_number,
        shoots_catches,
        height_inches,
        weight_pounds,
        birth_date,
        birth_city,
        birth_country,
        headshot_url,

        -- derived
        date_diff('year', birth_date, current_date)         as age,
        case
            when position_code = 'G'                        then 'Goalie'
            when position_code in ('L', 'R', 'C')          then 'Forward'
            when position_code = 'D'                        then 'Defenseman'
            else 'Unknown'
        end                                                 as position_group,
        case
            when height_inches is not null
                then (height_inches / 12)::integer
                    || '''' ||
                    (height_inches % 12)::integer
                    || '"'
        end                                                 as height_display

    from players
)

select * from enriched
