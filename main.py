from datetime import datetime, time, date
import os
import pickle
    
from collection.schemas import *
from collection.pages import SeasonPage, CalendarPage, GamePage, TeamPage
from collection.browser import BrowserConnection

import asyncio
from db.queries.core import AsyncCore as AC
from db.models import *

   
async def start_fill_database(season: Season):
    await AC.Season.insert_season(season_id=season.id,
                        start_date=season.start_date,
                        end_date=season.end_date)
    for team in season.teams:
        await AC.Team.insert_team(team_id=team.id,
                                  name=team.name)
        await AC.SeasonTeam.insert_season_team(season_id=season.id,
                                               team_id=team.id,
                                               season_team_id=team.season_team_id)
        for player in team.players:
            await AC.Player.insert_player(player_id=player.id,
                                          first_name=player.first_name,
                                          last_name=player.last_name,
                                          birth_date=player.birth_date)
            await AC.PlayerStat.insert_player_stat(player_id=player.id,
                                                   amplua=player.role,
                                                   season_id=season.id,
                                                   number=player.number,
                                                   growth=player.growth,
                                                   weight=player.weight,
                                                   transfer_value=player.transfer_value)
            await AC.TeamPlayer.insert_team_player_for_team_id(team_id=team.id,
                                                   season_id=season.id,
                                                   player_id=player.id)
        
        if team.coach is None: continue    
        await AC.Coach.insert_coach(coach_id=team.coach.id,
                                    first_name=team.coach.first_name,
                                    middle_name=team.coach.middle_name,
                                    last_name=team.coach.last_name,
                                    birth_date=team.coach.birth_date)
        await AC.TeamCoach.insert_team_coach_for_team_id(team_id=team.id,
                                             season_id=season.id,
                                             coach_id=team.coach.id)
        
    for game in season.games:
        
        # если когда либо присутствовали другие тренера на игре
        # в дальнейшем добавить обработку тренеров при просмотре игры
        left_coach_id = None
        if game.left_coach_id:
            left_coach_id = game.left_coach_id.id
            await AC.Coach.insert_coach(coach_id=left_coach_id)
            await AC.TeamCoach.insert_team_coach_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    coach_id=left_coach_id,
                                                                    is_active=False)
        right_coach_id = None
        if game.right_coach_id:
            right_coach_id = game.right_coach_id.id
            await AC.Coach.insert_coach(coach_id=right_coach_id)
            await AC.TeamCoach.insert_team_coach_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    coach_id=right_coach_id,
                                                                    is_active=False)
        
        game_id = await AC.Game.insert_game(season_game_id=game.id,
                                            season_id=season.id,
                                            left_season_team_id=game.left_season_team_id,
                                            right_season_team_id=game.right_season_team_id,
                                            left_coach_id=left_coach_id,
                                            right_coach_id=right_coach_id,
                                            game_status_id=game.is_played,
                                            tour_number=game.tour_number,
                                            start_date=game.date,
                                            start_time=game.time,
                                            min=game.cur_min,
                                            plus_min=game.cur_plus_min,)
        # если рефери указан для игры
        if game.referee:
            await AC.Referee.insert_referee(referee_id=game.referee.id,
                                            first_name=game.referee.first_name,
                                            last_name=game.referee.last_name)
            await AC.RefereeGame.insert_referee_game(referee_id=game.referee.id,
                                                    game_id=game_id)
        
        for left_team_goal in game.left_team_goals:
            
            player_id = left_team_goal.player_id.id
            player_sub_id = None if left_team_goal.player_sub_id is None else left_team_goal.player_sub_id.id
            
            # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
            if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
                await AC.Player.insert_player(player_id=player_sub_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_sub_id,
                                                                    is_active=False)
            
            await AC.Goal.insert_goal_for_season_team_id(game_id=game_id,
                                                         season_id=season.id,
                                                         season_team_id=game.left_season_team_id,
                                                         player_id=left_team_goal.player_id.id,
                                                         player_sub_id=player_sub_id,
                                                         goal_type_name=left_team_goal.type,
                                                         min=left_team_goal.min,
                                                         plus_min=left_team_goal.plus_min)
        
        for right_team_goal in game.right_team_goals:
            
            player_id = right_team_goal.player_id.id
            player_sub_id = None if right_team_goal.player_sub_id is None else right_team_goal.player_sub_id.id
            
            # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
            if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
                await AC.Player.insert_player(player_id=player_sub_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_sub_id,
                                                                    is_active=False)
            
            await AC.Goal.insert_goal_for_season_team_id(game_id=game_id,
                                                         season_id=season.id,
                                                         season_team_id=game.right_season_team_id,
                                                         player_id=right_team_goal.player_id.id,
                                                         player_sub_id=player_sub_id,
                                                         goal_type_name=right_team_goal.type,
                                                         min=right_team_goal.min,
                                                         plus_min=right_team_goal.plus_min)
        
        for team_penalty in game.left_team_penalties:
            player_id = team_penalty.player_id.id
             # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
            
            await AC.Penalty.insert_penalty_for_season_team_id(game_id=game_id,
                                                               season_id=season.id,
                                                               season_team_id=game.left_season_team_id,
                                                               player_id=player_id,
                                                               penalty_type_name=team_penalty.type,
                                                               min=team_penalty.min,
                                                               plus_min=team_penalty.plus_min)
            
        for team_penalty in game.right_team_penalties:
            player_id = team_penalty.player_id.id
             # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
            
            await AC.Penalty.insert_penalty_for_season_team_id(game_id=game_id,
                                                               season_id=season.id,
                                                               season_team_id=game.right_season_team_id,
                                                               player_id=player_id,
                                                               penalty_type_name=team_penalty.type,
                                                               min=team_penalty.min,
                                                               plus_min=team_penalty.plus_min)
        
        for lineup in game.left_team_lineup:
            player_id = lineup.player_id.id
             # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
            if lineup.saves:
                await AC.Save.insert_save_for_season_team_id(game_id=game_id,
                                                             season_id=season.id,
                                                             season_team_id=game.left_season_team_id,
                                                             player_id=player_id,
                                                             count=lineup.saves)
                
            await AC.Lineup.insert_lineup_for_season_team_id(game_id=game_id,
                                                             season_id=season.id,
                                                             season_team_id=game.left_season_team_id,
                                                             player_id=player_id,
                                                             min_in=lineup.min_in,
                                                             plus_min_in=lineup.plus_min_in,
                                                             min_out=lineup.min_out,
                                                             plus_min_out=lineup.plus_min_out)
            
        for lineup in game.right_team_lineup:
            player_id = lineup.player_id.id
             # добавляем id игрока если его нет в собранных данных о составе
            if not await AC.Player.is_player_id_exist(player_id):
                await AC.Player.insert_player(player_id=player_id)
                await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_id,
                                                                    is_active=False)
                
            if lineup.saves:
                await AC.Save.insert_save_for_season_team_id(game_id=game_id,
                                                             season_id=season.id,
                                                             season_team_id=game.right_season_team_id,
                                                             player_id=player_id,
                                                             count=lineup.saves)
                
            await AC.Lineup.insert_lineup_for_season_team_id(game_id=game_id,
                                                             season_id=season.id,
                                                             season_team_id=game.right_season_team_id,
                                                             player_id=player_id,
                                                             min_in=lineup.min_in,
                                                             plus_min_in=lineup.plus_min_in,
                                                             min_out=lineup.min_out,
                                                             plus_min_out=lineup.plus_min_out)
            
        for gamestat in game.game_stats:
            await AC.GameStat.insert_game_stat_for_season_team_id(game_id=game_id,
                                                                  season_id=season.id,
                                                                  season_team_id=game.left_season_team_id,
                                                                  stat_name=gamestat.stat_name,
                                                                  count=gamestat.left_team_stat,
                                                                  min=game.cur_min,
                                                                  plus_min=game.cur_plus_min)
            await AC.GameStat.insert_game_stat_for_season_team_id(game_id=game_id,
                                                                  season_id=season.id,
                                                                  season_team_id=game.right_season_team_id,
                                                                  stat_name=gamestat.stat_name,
                                                                  count=gamestat.right_team_stat,
                                                                  min=game.cur_min,
                                                                  plus_min=game.cur_plus_min)

def get_filled_schemas_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

async def process_file(file, path):
    with open(os.path.join(path, file), "rb") as f:
        loaded_season: Season = pickle.load(f)
    await start_fill_database(loaded_season)

async def main():
    filled_schemas_path = './collection/filled_schemas'
    files = list(get_filled_schemas_files(filled_schemas_path))
    
    for file in files:
        print(f"Processing {file}...")
        await process_file(file, filled_schemas_path)
        input('Для продолжения нажмите любую кнопку...')

if __name__ == "__main__":
    if os.name == 'nt':
        from asyncio import WindowsSelectorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())