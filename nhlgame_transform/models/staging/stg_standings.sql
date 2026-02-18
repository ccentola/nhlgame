{{ config(materialized='view') }}

with latest_snapshot as (
    select
        team_abbrev,
        max(snapshot_date)                          as latest_date
    from {{ ref('stg_standings_history') }}
    group by team_abbrev
)

select h.*
from {{ ref('stg_standings_history') }} h
inner join latest_snapshot s
    on h.team_abbrev = s.team_abbrev
    and h.snapshot_date = s.latest_date
