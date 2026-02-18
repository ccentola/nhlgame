{{ config(materialized='table') }}

with plays as (
    select * from {{ ref('stg_plays') }}
),

games as (
    select * from {{ ref('stg_games') }}
),

players as (
    select * from {{ ref('dim_players') }}
),

teams as (
    select * from {{ ref('dim_teams') }}
),

enriched as (
    select
        -- event identity
        p.game_id,
        p.event_id,
        p.event_type,
        p.sort_order,

        -- game context
        g.game_date,
        g.season,
        g.home_team_abbrev,
        g.away_team_abbrev,
        g.home_score                                        as final_home_score,
        g.away_score                                        as final_away_score,
        g.last_period_type,

        -- period context
        p.period,
        p.period_type,
        p.time_in_period,
        p.time_remaining,
        p.situation_code,

        -- event team
        (p.details->>'eventOwnerTeamId')::integer          as event_team_id,

        -- shot/goal location
        (p.details->>'xCoord')::integer                    as x_coord,
        (p.details->>'yCoord')::integer                    as y_coord,
        p.details->>'zoneCode'                             as zone_code,
        p.details->>'shotType'                             as shot_type,

        -- goal details
        (p.details->>'scoringPlayerId')::integer           as scoring_player_id,
        (p.details->>'assist1PlayerId')::integer           as assist1_player_id,
        (p.details->>'assist2PlayerId')::integer           as assist2_player_id,
        (p.details->>'scoringPlayerTotal')::integer        as scoring_player_total,
        (p.details->>'assist1PlayerTotal')::integer        as assist1_player_total,
        (p.details->>'assist2PlayerTotal')::integer        as assist2_player_total,
        (p.details->>'awayScore')::integer                 as away_score_at_goal,
        (p.details->>'homeScore')::integer                 as home_score_at_goal,
        (p.details->>'goalieInNetId')::integer             as goalie_player_id,
        p.details->>'highlightClipSharingUrl'              as highlight_url,

        -- shot details (non-goal)
        (p.details->>'shootingPlayerId')::integer          as shooting_player_id,
        (p.details->>'blockingPlayerId')::integer          as blocking_player_id,

        -- faceoff details
        (p.details->>'winningPlayerId')::integer           as faceoff_winning_player_id,
        (p.details->>'losingPlayerId')::integer            as faceoff_losing_player_id,

        -- hit details
        (p.details->>'hittingPlayerId')::integer           as hitting_player_id,
        (p.details->>'hitteePlayerId')::integer            as hittee_player_id,

        -- penalty details
        (p.details->>'committedByPlayerId')::integer       as penalty_committed_by_player_id,
        (p.details->>'drawnByPlayerId')::integer           as penalty_drawn_by_player_id,
        p.details->>'descKey'                              as penalty_desc,
        (p.details->>'duration')::integer                  as penalty_duration_minutes,

        -- giveaway/takeaway
        (p.details->>'playerId')::integer                  as player_id

    from plays p
    left join games g
        on p.game_id = g.game_id
),

-- join player names for key roles
with_players as (
    select
        e.*,

        -- goal scorer
        scorer.full_name                                    as scoring_player_name,
        assist1.full_name                                   as assist1_player_name,
        assist2.full_name                                   as assist2_player_name,
        goalie.full_name                                    as goalie_player_name,

        -- shots
        shooter.full_name                                   as shooting_player_name,

        -- faceoffs
        faceoff_winner.full_name                            as faceoff_winning_player_name,
        faceoff_loser.full_name                             as faceoff_losing_player_name,

        -- hits
        hitter.full_name                                    as hitting_player_name,
        hittee.full_name                                    as hittee_player_name,

        -- penalties
        penalty_by.full_name                               as penalty_committed_by_player_name,
        penalty_drawn.full_name                            as penalty_drawn_by_player_name,

        -- giveaway/takeaway
        gtplayer.full_name                                  as player_name

    from enriched e
    left join players scorer
        on e.scoring_player_id = scorer.player_id
    left join players assist1
        on e.assist1_player_id = assist1.player_id
    left join players assist2
        on e.assist2_player_id = assist2.player_id
    left join players goalie
        on e.goalie_player_id = goalie.player_id
    left join players shooter
        on e.shooting_player_id = shooter.player_id
    left join players faceoff_winner
        on e.faceoff_winning_player_id = faceoff_winner.player_id
    left join players faceoff_loser
        on e.faceoff_losing_player_id = faceoff_loser.player_id
    left join players hitter
        on e.hitting_player_id = hitter.player_id
    left join players hittee
        on e.hittee_player_id = hittee.player_id
    left join players penalty_by
        on e.penalty_committed_by_player_id = penalty_by.player_id
    left join players penalty_drawn
        on e.penalty_drawn_by_player_id = penalty_drawn.player_id
    left join players gtplayer
        on e.player_id = gtplayer.player_id
)

select * from with_players
