{{ config(materialized='table') }}

select
    team_abbrev,
    team_name,
    team_common_name,
    conference_name,
    conference_abbrev,
    division_name,
    division_abbrev,
    team_logo_url
from {{ ref('stg_standings') }}
