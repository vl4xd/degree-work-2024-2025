from datetime import datetime, time, date
import os
import pickle
    
from collection.schemas import *
from collection.pages import PlayerPage
from collection.browser import BrowserConnection

import asyncio
from db.queries.core import AsyncCore as AC
from db.models import *


# добавить кнопку сообщить о неполных данных в интерфейс клиентского приложения
UNKNOWN_SEASON_PLAYER_STAT: dict[str, set] = {}
   
async def start_fill_database(season: Season):
    # инициализируем ключ словаря UNKNOWN_SEASON_PLAYER_STAT для данного сезона
    UNKNOWN_SEASON_PLAYER_STAT[season.id] = set()
    
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
            await AC.TeamPlayer.insert_team_player_for_team_id(team_id=team.id,
                                                   season_id=season.id,
                                                   player_id=player.id)
            
            # проверка для исключения дублирования
            if await AC.PlayerStat.is_player_stat_exist(player_id=player.id, season_id=season.id): continue
            await AC.PlayerStat.insert_player_stat(player_id=player.id,
                                                   amplua=player.role,
                                                   season_id=season.id,
                                                   number=player.number,
                                                   growth=player.growth,
                                                   weight=player.weight,
                                                   transfer_value=player.transfer_value)
        
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
        
        # если когда либо присутствовали необработанные в момент просмотра команды тренера на игре
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
            #if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
                                
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
                
                
            #if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
            if player_sub_id:
                await AC.Player.insert_player(player_id=player_sub_id)
                
                player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_sub_id,
                                                                    is_active=False)
                if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_sub_id)
                
            
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
            #if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
            
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
                
            #if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
            if player_sub_id:
                await AC.Player.insert_player(player_id=player_sub_id)
                
                player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                    season_id=season.id,
                                                                    player_id=player_sub_id,
                                                                    is_active=False)
                if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_sub_id)
            
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
            # if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
            
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
            #if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
            
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
            
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
            #if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
                
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
            #if not await AC.Player.is_player_id_exist(player_id):
            await AC.Player.insert_player(player_id=player_id)
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                season_id=season.id,
                                                                player_id=player_id,
                                                                is_active=False)
            
            if not player_has_stat: UNKNOWN_SEASON_PLAYER_STAT[season.id].add(player_id)
                
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

    print(UNKNOWN_SEASON_PLAYER_STAT)


def get_filled_schemas_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file


async def process_file(file, path):
    with open(os.path.join(path, file), "rb") as f:
        loaded_season: Season = pickle.load(f)
    await start_fill_database(loaded_season)


async def load_unknown_season_player():
    '''Обработка выявленных игроков со страниц матчей поскольку полный сбор данных об игроках ведется через состав команд'''
    with BrowserConnection() as br:
        for season_id in UNKNOWN_SEASON_PLAYER_STAT.keys():
            print(f'\nseason: {season_id}\nplayers: {UNKNOWN_SEASON_PLAYER_STAT[season_id]}')
            for player_id in UNKNOWN_SEASON_PLAYER_STAT[season_id]:
                pp = PlayerPage(br, PlayerPage.get_player_page_link(season_id=season_id, player_id=player_id))
                player_info: Player = pp.get_info()
                await AC.Player.update_player_data(player_id=player_id,
                                                    first_name=player_info.first_name,
                                                    last_name=player_info.last_name,
                                                    birth_date=player_info.birth_date)
                if await AC.PlayerStat.is_player_stat_exist(player_id=player_id, season_id=season_id): continue
                await AC.PlayerStat.insert_player_stat(player_id=player_id,
                                                        amplua=player_info.role,
                                                        season_id=season_id,
                                                        number=player_info.number,
                                                        growth=player_info.growth,
                                                        weight=player_info.weight,
                                                        transfer_value=player_info.transfer_value)


async def main():
    filled_schemas_path = './collection/filled_schemas'
    files = list(get_filled_schemas_files(filled_schemas_path))
    
    for file in files:
        print(f"Processing {file}...")
        await process_file(file, filled_schemas_path)
        # input('Для продолжения нажмите любую кнопку...')
    
    await load_unknown_season_player()


if __name__ == "__main__":
    if os.name == 'nt':
        from asyncio import WindowsSelectorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
                
    # asyncio.run(AC.PlayerStat.is_player_stat_exist())
    # print(UNKNOWN_SEASON_PLAYER_STAT)
    
    
    