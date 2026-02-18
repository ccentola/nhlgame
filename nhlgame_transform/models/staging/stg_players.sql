{{ config(materialized='view') }}

with latest_snapshot as (
    select
        player_id,
        max(snapshot_date)                          as latest_date
    from {{ ref('stg_players_history') }}
    group by player_id
)

select h.*
from {{ ref('stg_players_history') }} h
inner join latest_snapshot s
    on h.player_id = s.player_id
    and h.snapshot_date = s.latest_date
