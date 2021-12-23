drop view public.v_game_team;
drop view public.v_game_entity;
drop view public.v_game_entity_state_final

create view public.v_game_entity_state_final
as
SELECT game_entity_state.*, mvp.mvp, mvp.mvp_details
FROM game_entity_state
LEFT JOIN mvp on game_entity_state.id = mvp.game_entity_state_id
WHERE game_entity_state.is_final = true;

alter table public.v_game_entity_state_final
    owner to postgres;

create view public.v_game_entity
as
SELECT ge.id,
       ge.ipl_id,
       ge.entity_type,
       ge.entity_desc,
       ge.entity_level,
       ge.category,
       ge.battlesuit,
       ge.game_team_id,
       ge.end_code,
       ge.end_time,
       ge."position",
       ge.player_id,
       ge.start_time,
       vgesf.score,
       vgesf.is_eliminated
FROM game_entity ge
         LEFT JOIN v_game_entity_state_final vgesf ON ge.id = vgesf.entity_id;

alter table public.v_game_entity
    owner to postgres;

create view public.v_game_team
as
SELECT game_team.id,
       game_team.team_index,
       game_team.team_desc,
       game_team.color_enum,
       game_team.color_desc,
       game_team.game_id,
       game_team.ui_color,
       game_team.is_eliminated,
       game_team.opp_eliminated,
       game_team.elim_bonus,
       sum(vge.score)                        AS raw_score,
       sum(vge.score) + game_team.elim_bonus AS score
FROM game_team
         LEFT JOIN v_game_entity vge ON game_team.id = vge.game_team_id
GROUP BY game_team.id;

alter table public.v_game_team
    owner to postgres;
