import sys
import os
# Получаем абсолютный путь к директории текущего скрипта (collection/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на уровень выше (в корень проекта)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# Добавляем корень проекта в пути поиска модулей
sys.path.append(project_root)

from collection.browser import BrowserConnection
from collection.pages import *
from db.queries.core import AsyncCore as AC
import asyncio




async def check_season_id_in_db(first_season:str = '2016/2017') -> list[str]:
    """Проерка рассмотренных сезонов (сохраненных в БД)

    Args:
        first_season (str, optional): _description_. Начальный сезон (по умолчанию'2016/2017').

    Returns:
        list[str]: Не рассмотренные сезоны
    """
    
    # Список доступных сезонов
    all_season_list = []
    with BrowserConnection() as br:
        season_page = SeasonPage(br)
        # Получение списка доступных сезонов
        all_season_list = season_page.get_season_list_options()
        
    # Список с рассматриваемыми сезонами
    considered_season_list = []
    for season in all_season_list:
        if season == first_season:
            considered_season_list.append(season)
            break
        considered_season_list.append(season)
    
    # Список с сезонами ожидающими сбор данных
    pending_season_list = []
    with BrowserConnection() as br:
        for season in considered_season_list:
                season_page = SeasonPage(br)
                season_page.go_to_season(season)
                season_id = season_page.get_info(only_info=True).id
                if not await AC.Season.is_season_id_exist(season_id=season_id):
                    pending_season_list.append(season_id)
    return pending_season_list
    

async def insert_season_team_into_db(season_id: str, team: Team):
    await AC.Team.insert_team(team_id=team.id,
                                name=team.name)
    await AC.SeasonTeam.insert_season_team(season_id=season_id,
                                            team_id=team.id,
                                            season_team_id=team.season_team_id)
    for player in team.players:
        await AC.Player.insert_player(player_id=player.id,
                                        first_name=player.first_name,
                                        last_name=player.last_name,
                                        birth_date=player.birth_date)
        await AC.TeamPlayer.insert_team_player_for_team_id(team_id=team.id,
                                                season_id=season_id,
                                                player_id=player.id)
        
        # проверка для исключения дублирования
        if await AC.PlayerStat.is_player_stat_exist(player_id=player.id, season_id=season_id): continue
        await AC.PlayerStat.insert_player_stat(player_id=player.id,
                                                amplua=player.role,
                                                season_id=season_id,
                                                number=player.number,
                                                growth=player.growth,
                                                weight=player.weight,
                                                transfer_value=player.transfer_value)
    
    if team.coach:    
        await AC.Coach.insert_coach(coach_id=team.coach.id,
                                    first_name=team.coach.first_name,
                                    middle_name=team.coach.middle_name,
                                    last_name=team.coach.last_name,
                                    birth_date=team.coach.birth_date)
        await AC.TeamCoach.insert_team_coach_for_team_id(team_id=team.id,
                                                season_id=season_id,
                                                coach_id=team.coach.id)
        

async def insert_unknown_season_player_into_db(season_id: str, unknown_season_player: set):
    '''Обработка выявленных игроков со страниц матчей поскольку полный сбор данных об игроках ведется через состав команд'''
    with BrowserConnection() as br:
        print(f'\nseason: {season_id}\nplayers: {unknown_season_player}')
        for player_id in unknown_season_player:
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


async def insert_game_coach_into_db(season_id: str, game: Game):
    # если присутствовали необработанные в момент просмотра состава команд тренера
    # в дальнейшем добавить обработку тренеров при просмотре игры
    left_coach_id = None
    if game.left_coach_id:
        left_coach_id = game.left_coach_id.id
        await AC.Coach.insert_coach(coach_id=left_coach_id)
        await AC.TeamCoach.insert_team_coach_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                season_id=season_id,
                                                                coach_id=left_coach_id,
                                                                is_active=False)
    right_coach_id = None
    if game.right_coach_id:
        right_coach_id = game.right_coach_id.id
        await AC.Coach.insert_coach(coach_id=right_coach_id)
        await AC.TeamCoach.insert_team_coach_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                season_id=season_id,
                                                                coach_id=right_coach_id,
                                                                is_active=False)
    
    return left_coach_id, right_coach_id


async def insert_game_referee_into_db(game_id: int, game: Game):
    # если рефери указан
    if game.referee:
        await AC.Referee.insert_referee(referee_id=game.referee.id,
                                        first_name=game.referee.first_name,
                                        last_name=game.referee.last_name)
        await AC.RefereeGame.insert_referee_game(referee_id=game.referee.id,
                                                game_id=game_id)
        

async def insert_game_goal_into_db(season_id: str, game_id: int, game: Game, unknown_season_player: set):
    for left_team_goal in game.left_team_goals:
            
        player_id = left_team_goal.player_id.id
        player_sub_id = None if left_team_goal.player_sub_id is None else left_team_goal.player_sub_id.id
        
        # добавляем id игрока если его нет в собранных данных о составе
        #if not await AC.Player.is_player_id_exist(player_id):
        await AC.Player.insert_player(player_id=player_id)
                            
        player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        if not player_has_stat: unknown_season_player.add(player_id)
            
            
        #if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
        if player_sub_id:
            await AC.Player.insert_player(player_id=player_sub_id)
            
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                                season_id=season_id,
                                                                player_id=player_sub_id,
                                                                is_active=False)
            if not player_has_stat: unknown_season_player.add(player_sub_id)
            
        
        await AC.Goal.insert_goal_for_season_team_id(game_id=game_id,
                                                        season_id=season_id,
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
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        if not player_has_stat: unknown_season_player.add(player_id)
            
        #if player_sub_id and not await AC.Player.is_player_id_exist(player_sub_id):
        if player_sub_id:
            await AC.Player.insert_player(player_id=player_sub_id)
            
            player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.right_season_team_id,
                                                                season_id=season_id,
                                                                player_id=player_sub_id,
                                                                is_active=False)
            if not player_has_stat: unknown_season_player.add(player_sub_id)
        
        await AC.Goal.insert_goal_for_season_team_id(game_id=game_id,
                                                        season_id=season_id,
                                                        season_team_id=game.right_season_team_id,
                                                        player_id=right_team_goal.player_id.id,
                                                        player_sub_id=player_sub_id,
                                                        goal_type_name=right_team_goal.type,
                                                        min=right_team_goal.min,
                                                        plus_min=right_team_goal.plus_min)
        
        
async def insert_game_penalty_into_db(season_id: str, game_id: int, game: Game, unknown_season_player: set):
    for team_penalty in game.left_team_penalties:
        player_id = team_penalty.player_id.id
        # добавляем id игрока если его нет в собранных данных о составе
        # if not await AC.Player.is_player_id_exist(player_id):
        await AC.Player.insert_player(player_id=player_id)
        player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        
        if not player_has_stat: unknown_season_player.add(player_id)
        
        await AC.Penalty.insert_penalty_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
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
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        
        if not player_has_stat: unknown_season_player.add(player_id)
        
        await AC.Penalty.insert_penalty_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
                                                            season_team_id=game.right_season_team_id,
                                                            player_id=player_id,
                                                            penalty_type_name=team_penalty.type,
                                                            min=team_penalty.min,
                                                            plus_min=team_penalty.plus_min)


async def insert_game_lineup_into_db(season_id: str, game_id: int, game: Game, unknown_season_player: set):
    for lineup in game.left_team_lineup:
        player_id = lineup.player_id.id
        # добавляем id игрока если его нет в собранных данных о составе
        #if not await AC.Player.is_player_id_exist(player_id):
        await AC.Player.insert_player(player_id=player_id)
        player_has_stat = await AC.TeamPlayer.insert_team_player_for_season_team_id(season_team_id=game.left_season_team_id,
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        
        if not player_has_stat: unknown_season_player.add(player_id)
            
        if lineup.saves:
            await AC.Save.insert_save_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
                                                            season_team_id=game.left_season_team_id,
                                                            player_id=player_id,
                                                            count=lineup.saves)
            
        await AC.Lineup.insert_lineup_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
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
                                                            season_id=season_id,
                                                            player_id=player_id,
                                                            is_active=False)
        
        if not player_has_stat: unknown_season_player.add(player_id)
            
        if lineup.saves:
            await AC.Save.insert_save_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
                                                            season_team_id=game.right_season_team_id,
                                                            player_id=player_id,
                                                            count=lineup.saves)
            
        await AC.Lineup.insert_lineup_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
                                                            season_team_id=game.right_season_team_id,
                                                            player_id=player_id,
                                                            min_in=lineup.min_in,
                                                            plus_min_in=lineup.plus_min_in,
                                                            min_out=lineup.min_out,
                                                            plus_min_out=lineup.plus_min_out)


async def insert_game_stat_into_db(season_id: str, game_id: int, game: Game):
    for gamestat in game.game_stats:
        await AC.GameStat.insert_game_stat_for_season_team_id(game_id=game_id,
                                                                season_id=season_id,
                                                                season_team_id=game.left_season_team_id,
                                                                stat_name=gamestat.stat_name,
                                                                count=gamestat.left_team_stat,
                                                                min=game.cur_min,
                                                                plus_min=game.cur_plus_min)
        await AC.GameStat.insert_game_stat_for_season_team_id(game_id=game_id,
                                                                season_id=season_id,
                                                                season_team_id=game.right_season_team_id,
                                                                stat_name=gamestat.stat_name,
                                                                count=gamestat.right_team_stat,
                                                                min=game.cur_min,
                                                                plus_min=game.cur_plus_min)

        
async def insert_season_game_into_db(season_id: str, game: Game):
    
    
    # Список уникальных идентификаторов игроков, необработанных в составах команд
    unknown_season_player: set = ()
    
    left_coach_id, right_coach_id = insert_game_coach_into_db(season_id=season_id, game=game)
    
    
    game_id = await AC.Game.insert_game(season_game_id=game.id,
                                        season_id=season_id,
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
    
    insert_game_referee_into_db(game_id=game_id, game=game)
    
    insert_game_goal_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
    
    insert_game_penalty_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
    
    insert_game_lineup_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
        
    insert_game_stat_into_db(season_id=season_id, game_id=game_id, game=game)        
    
    insert_unknown_season_player_into_db(season_id=season_id, unknown_season_player=unknown_season_player)


async def insert_season_into_db(season_id: str):
    """Занесения всей информации сезона в базу данных

    Args:
        season_id (str): Идентификатор сезона
    """
    
    # Полная ссылка на рассматриваемый сезон
    season_page_link = SeasonPage.get_page_link(season_id=season_id)
    
    count_attempt = 0
    max_count_attempt = 50
    try:
        while count_attempt < max_count_attempt:
            with BrowserConnection() as br:
                season_page = SeasonPage(br, page_href=season_page_link)
                season: Season = season_page.get_info()
    except Exception as e:
        print(f'\nПопытка#{count_attempt}\nИсключение: {e}\n')
        count_attempt += 1
    
    await AC.Season.insert_season(season_id=season.id,
                        start_date=season.start_date,
                        end_date=season.end_date)
    
    for team in season.teams:
        await insert_season_team_into_db(season_id=season.id, team=team)
    
    for game in season.games:
        await insert_season_game_into_db(season_id=season.id, game=game)  


async def check_active_game_in_db():
    # Индетификатор текущего сезона
    current_season_id = await AC.Season.get_current_season_id()
    # Лист идентификторов прошедших (необработанных) и текущих игры
    active_season_game_id = await AC.Game.get_active_season_game_id(season_id=current_season_id)
    print(active_season_game_id)
    return active_season_game_id
    

asyncio.run()