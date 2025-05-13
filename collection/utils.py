import sys
import os
# Получаем абсолютный путь к директории текущего скрипта (collection/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на уровень выше (в корень проекта)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# Добавляем корень проекта в пути поиска модулей
sys.path.append(project_root)

import pandas as pd
from collection.browser import BrowserConnection, AsyncBrowserConnection
from collection.pages import *
from db.queries.core import AsyncCore as AC
import asyncio


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
    print(f'\nseason: {season_id}\nplayers: {unknown_season_player}')
    if len(unknown_season_player) == 0: return
    with BrowserConnection() as br:
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
            
        # Учтено обновление данных
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
        
        # Учтено обновление данных
        await AC.Lineup.insert_lineup_for_season_team_id(game_id=game_id,
                                                            season_id=season_id,
                                                            season_team_id=game.right_season_team_id,
                                                            player_id=player_id,
                                                            min_in=lineup.min_in,
                                                            plus_min_in=lineup.plus_min_in,
                                                            min_out=lineup.min_out,
                                                            plus_min_out=lineup.plus_min_out)


async def insert_game_stat_into_db(season_id: str, game_id: int, game: Game):
    
    # Добавить временное указание (предыдущие как None)
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
    unknown_season_player: set = set()
    
    left_coach_id, right_coach_id = await insert_game_coach_into_db(season_id=season_id, game=game)
    
    
    # Учтено обновление данных
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
    
    await insert_game_referee_into_db(game_id=game_id, game=game)
    
    await insert_game_goal_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
    
    await insert_game_penalty_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
    
    await insert_game_lineup_into_db(season_id=season_id, game_id=game_id, game=game, unknown_season_player=unknown_season_player)
        
    await insert_game_stat_into_db(season_id=season_id, game_id=game_id, game=game)        
    
    await insert_unknown_season_player_into_db(season_id=season_id, unknown_season_player=unknown_season_player)
    
    return game_id


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
            async with AsyncBrowserConnection() as br:
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
    current_season_id: str = await AC.Season.get_current_season_id()
    # Лист идентификторов прошедших (необработанных) и текущих игры
    active_season_game_id: list[str] = await AC.Game.get_active_season_game_id_for_collection(season_id=current_season_id)
    return current_season_id, active_season_game_id
    
    
async def add_time_event(time_events: set, 
                   min_column_name: str, 
                   plus_min_column_name: str, 
                   df: pd.DataFrame):
    """Добавляет во временное множество событий время события

    Args:
        time_events (set): Временное множество событий
        min_column_name (str): Название атрибута набора данных, указывающая на время события 
        plus_min_column_name (str): Название атрибута набора данных, указывающая на дополнительное время события 
        df (pd.DataFrame): Набор данных
    """
    for _, row in df.iterrows():
        if not pd.isna(row[min_column_name]):
            time_events.add((row[min_column_name], row[plus_min_column_name]))  


async def simulate_match(game_id: int, time_events: set = None, is_event_exist: bool = False):
    res_df = pd.DataFrame(columns=['min',
                                'plus_min',
                                'left_coach_id',
                                'right_coach_id',
                                'referee_id',
                                'left_num_v',
                                'left_num_z',
                                'left_num_p',
                                'left_num_n',
                                'left_num_u',
                                'right_num_v',
                                'right_num_z',
                                'right_num_p',
                                'right_num_n',
                                'right_num_u',
                                'left_num_y',
                                'left_num_y2r',
                                'right_num_y',
                                'right_num_y2r',
                                'right_num_goal_g',
                                'right_num_goal_p',
                                'right_num_goal_a',
                                'left_num_goal_g',
                                'left_num_goal_p',
                                'left_num_goal_a',
                                'left_total_transfer_value',
                                'right_total_transfer_value',
                                'left_avg_transfer_value',
                                'right_avg_transfer_value',
                                'left_goal_score',
                                'right_goal_score',
                                'left_avg_time_player_in_game',
                                'right_avg_time_player_in_game',
                                'left_right_transfer_value_div',
                                'right_left_transfer_value_div',
                                'res_event'
                                   ])
    
    df_game = await AC.TableToDataFrame.get_game_df(game_id=game_id)
    df_referee_game = await AC.TableToDataFrame.get_referee_game_df(game_id=game_id)
    df_goal = await AC.TableToDataFrame.get_goal_df(game_id=game_id)
    df_goal_type = await AC.TableToDataFrame.get_goal_type_df()
    df_lineup = await AC.TableToDataFrame.get_lineup_df(game_id=game_id)
    df_penalty = await AC.TableToDataFrame.get_penalty_df(game_id=game_id)
    df_penalty_type = await AC.TableToDataFrame.get_penalty_type_df()
    game_player_stat_amplua = await AC.TableToDataFrame.get_lineup_player_stat_for_game(game_id=game_id) # Получаем информацию о игроках, учавствующих в данной игре (амплуа)
    
    # Объединяем главного судью с матчем
    df_game = df_game.join(df_referee_game.set_index('game_id'), 'game_id')
    if not is_event_exist: time_events = set() # Время возникновения какого либо события в матче
    game_id = int(df_game['game_id'].item()) # Уникальный идентификатор иргы
    left_team_id = str(df_game['left_team_id'].item()) # Уникальный идентификатор левой команды
    right_team_id = str(df_game['right_team_id'].item()) # Уникальный идентификатор правой команды
    left_coach_id = int(df_game['left_coach_id'].item()) # Уникальный идентификатор тренера левой команды
    right_coach_id = int(df_game['right_coach_id'].item()) # Уникальный идентификатор тренера правой команды
    referee_id = int(df_game['referee_id'].item()) # Уникальный идентификатор главного сузьи
    # Находим голы, забитые левой командой
    left_goals_df = df_goal.loc[(df_goal['game_id']==game_id) & (df_goal['team_id']==left_team_id)]
    left_goals_df['plus_min'] = left_goals_df['plus_min'].fillna(0) # Заполняем добавленной время гола (None - без добавленного времени)
    left_goals_df.sort_values(by=['min', 'plus_min'], ascending=[True, True]) # Сортируем по времени забитых голов
    left_goals_df = left_goals_df.join(df_goal_type.set_index('goal_type_id'), 'goal_type_id') # Объединяем голы с типом гола
    if not is_event_exist: await add_time_event(time_events, 'min', 'plus_min', left_goals_df) # Добавляем время событий "гол" во временное множество
    
    # Находим наказания, полученные левой командой
    left_penalty_df = df_penalty.loc[(df_penalty['game_id']==game_id) & (df_penalty['team_id']==left_team_id)]
    left_penalty_df = left_penalty_df.join(df_penalty_type.set_index('penalty_type_id'), 'penalty_type_id') # Объединяем наказание с типом наказания
    left_penalty_df['plus_min'] = left_penalty_df['plus_min'].fillna(0) # Заполняем добавленной время наказания (None - без добавленного времени)
    left_penalty_df.sort_values(by=['min', 'plus_min'], ascending=[True, True]) # Сортируем по времени полученных наказаний
    if not is_event_exist: await add_time_event(time_events, 'min', 'plus_min', left_penalty_df) # Добавляем время событий "гол" во временное множество
    
    # Находим состав игроков на игру левой команды
    left_lineup_df = df_lineup.loc[(df_lineup['game_id']==game_id) & (df_lineup['team_id']==left_team_id)] 
    left_lineup_df['min_in'] = left_lineup_df['min_in'].fillna(0) # Заполняем время появления на поле (None - в стартовом составе)
    left_lineup_df['plus_min_in'] = left_lineup_df['plus_min_in'].fillna(0) # Заполняем добавленной время появление на поле (None - в стартовом составе)
    mask_left_lineup = left_lineup_df['min_out'].notna() # Создаем маску для заполнения добавленного времни ухода с поля тех, кто был заменен
    left_lineup_df.loc[mask_left_lineup, 'plus_min_out'] = (
        left_lineup_df.loc[mask_left_lineup, 'plus_min_out'].fillna(0) # Заполняем добавочное время ухода по маске
    )
    left_lineup_df = left_lineup_df.join(game_player_stat_amplua.set_index('player_id'), 'player_id') # Объединяем амплуа с игроками левой команды 
    # Назначим время выхода из игры игрокам, которые получили наказания: yellow2 или red (в составах при получении красной или двойной желтой время ухода с поля не обозначено)
    for __, left_penalty_row in left_penalty_df.loc[(left_penalty_df['name']=='yellow2') | (left_penalty_df['name']=='red')].iterrows():
        player_penalty_id = left_penalty_row['player_id']
        
        mask = left_lineup_df['player_id'] == player_penalty_id
        left_lineup_df.loc[mask, ['min_out', 'plus_min_out']] = [
            left_penalty_row['min'],
            left_penalty_row['plus_min']
        ]
    if not is_event_exist: await add_time_event(time_events, 'min_out', 'plus_min_out', left_lineup_df)

    # Аналогичные действия для правой команды
    right_goals_df = df_goal.loc[(df_goal['game_id']==game_id) & (df_goal['team_id']==right_team_id)]
    right_goals_df['plus_min'] = right_goals_df['plus_min'].fillna(0)
    right_goals_df.sort_values(by=['min', 'plus_min'], ascending=[True, True])
    right_goals_df = right_goals_df.join(df_goal_type.set_index('goal_type_id'), 'goal_type_id')
    if not is_event_exist: await add_time_event(time_events, 'min', 'plus_min', right_goals_df) 
    
    right_penalty_df = df_penalty.loc[(df_penalty['game_id']==game_id) & (df_penalty['team_id']==right_team_id)]
    right_penalty_df = right_penalty_df.join(df_penalty_type.set_index('penalty_type_id'), 'penalty_type_id')
    right_penalty_df['plus_min'] = right_penalty_df['plus_min'].fillna(0)
    right_penalty_df.sort_values(by=['min', 'plus_min'], ascending=[True, True])
    if not is_event_exist: await add_time_event(time_events, 'min', 'plus_min', right_penalty_df) 
    
    right_lineup_df = df_lineup.loc[(df_lineup['game_id']==game_id) & (df_lineup['team_id']==right_team_id)]
    right_lineup_df['min_in'] = right_lineup_df['min_in'].fillna(0)
    right_lineup_df['plus_min_in'] = right_lineup_df['plus_min_in'].fillna(0)
    mask_right_lineup = right_lineup_df['min_out'].notna()
    right_lineup_df.loc[mask_right_lineup, 'plus_min_out'] = (
        right_lineup_df.loc[mask_right_lineup, 'plus_min_out'].fillna(0)
    )
    right_lineup_df = right_lineup_df.join(game_player_stat_amplua.set_index('player_id'), 'player_id')
    # Назначим время выхода из игры игрокам, которые получили наказания: yellow2 или red
    for _, right_penalty_row in right_penalty_df.loc[(right_penalty_df['name']=='yellow2') | (right_penalty_df['name']=='red')].iterrows():
        player_penalty_id = right_penalty_row['player_id']
        
        mask = right_lineup_df['player_id'] == player_penalty_id
        right_lineup_df.loc[mask, ['min_out', 'plus_min_out']] = [
            right_penalty_row['min'],
            right_penalty_row['plus_min']
        ]
    if not is_event_exist: await add_time_event(time_events, 'min_out', 'plus_min_out', right_lineup_df)
    
    
    if not is_event_exist: time_events.add((0,0)) # Добавим для каждого матча начальное состояние 0 0
    # Проходим по списку событий матча в которых произошли какие либо изменения
    for time_event in time_events:
        
        min = int(time_event[0]) # Время событя
        plus_min = int(time_event[1]) # Добавочное время события
        
        left_goal_score = len(left_goals_df.loc[((left_goals_df['min'] < min) | ((left_goals_df['min'] == min) & (left_goals_df['plus_min'] <= plus_min)))]) # Вычисляем количество голов, забитых левой командой к событию
        right_goal_score = len(right_goals_df.loc[((right_goals_df['min'] < min) | ((right_goals_df['min'] == min) & (right_goals_df['plus_min'] <= plus_min)))]) # Вычисляем количество голов, забитых правой командой к событию
        
        # Вычисляем количество желтых карточек, полученных левой командой к событию
        left_num_y = len(left_penalty_df.loc[((left_penalty_df['min'] < min) | ((left_penalty_df['min'] == min) & (left_penalty_df['plus_min'] <= plus_min))) & (left_penalty_df['name'] == 'yellow')]) 
        # Вычисляем количество красных или двойных желтых карточек, полученных левой командой к событию
        left_num_y2r = len(left_penalty_df.loc[((left_penalty_df['min'] < min) | ((left_penalty_df['min'] == min) & (left_penalty_df['plus_min'] <= plus_min))) & ((left_penalty_df['name'] == 'yellow2') | (left_penalty_df['name'] == 'red'))])
        
        # Вычисляем количество желтых карточек, полученных правой командой к событию
        right_num_y = len(right_penalty_df.loc[((right_penalty_df['min'] < min) | ((right_penalty_df['min'] == min) & (right_penalty_df['plus_min'] <= plus_min))) & (right_penalty_df['name'] == 'yellow')])
        # Вычисляем количество красных или двойных желтых карточек, полученных правой командой к событию
        right_num_y2r = len(right_penalty_df.loc[((right_penalty_df['min'] < min) | ((right_penalty_df['min'] == min) & (right_penalty_df['plus_min'] <= plus_min))) & ((right_penalty_df['name'] == 'yellow2') | (right_penalty_df['name'] == 'red'))])
        
        # Текущий состав к событию
        cur_left_lineup = left_lineup_df.loc[
            ((left_lineup_df['min_in'] < min) | ((left_lineup_df['min_in'] == min) & (left_lineup_df['plus_min_in'] <= plus_min))) &
            (
                ((pd.isna(left_lineup_df['min_out'])) | (pd.isna(left_lineup_df['plus_min_out']))) |
                ((left_lineup_df['min_out'] > min) | ((left_lineup_df['min_out'] == min) & (left_lineup_df['plus_min_out'] > plus_min)))
            )
            ]
        
        left_num_v = len(cur_left_lineup.loc[cur_left_lineup['name'] == 'вратарь']) # Кол-во вратарей в текущем составе левой команды
        left_num_z = len(cur_left_lineup.loc[cur_left_lineup['name'] == 'защитник']) # Кол-во защитников в текущем составе левой команды
        left_num_p = len(cur_left_lineup.loc[cur_left_lineup['name'] == 'полузащитник']) # Кол-во полузащитников в текущем составе левой команды
        left_num_n = len(cur_left_lineup.loc[cur_left_lineup['name'] == 'нападающий']) # Кол-во нападающих в текущем составе левой команды
        left_num_u = len(cur_left_lineup.loc[cur_left_lineup['name'] == 'неизвестно']) # Кол-во игроков с неизвестных амплуа в текущем составе левой команды
        left_num_goal_g = len(left_goals_df.loc[((left_goals_df['min'] < min) | ((left_goals_df['min'] == min) & (left_goals_df['plus_min'] <= plus_min))) & (left_goals_df['name'] == 'гол')]) # Кол-во голов левой команды
        left_num_goal_p = len(left_goals_df.loc[((left_goals_df['min'] < min) | ((left_goals_df['min'] == min) & (left_goals_df['plus_min'] <= plus_min))) & (left_goals_df['name'] == 'пенальти')]) # Кол-во пенальти левой команды
        left_num_goal_a = len(left_goals_df.loc[((left_goals_df['min'] < min) | ((left_goals_df['min'] == min) & (left_goals_df['plus_min'] <= plus_min))) & (left_goals_df['name'] == 'автогол')]) # Кол-во автоголов левой команды
        left_total_transfer_value = cur_left_lineup['transfer_value'].sum() # Сумма стоимости игроков левой команды в текущем составе
        left_avg_transfer_value = cur_left_lineup['transfer_value'].mean() # Средняя стоимость игроков левой команды в текущем составе
        left_avg_time_player_in_game = cur_left_lineup.apply(lambda row: min - row['min_in'], axis=1).mean() # Средняя продолжительность нахождения на поле игроков в текущем составе (без учета дополнительного времени)
        
        # Аналогичные действия для правой команды
        cur_right_lineup = right_lineup_df.loc[
            ((right_lineup_df['min_in'] < min) | ((right_lineup_df['min_in'] == min) & (right_lineup_df['plus_min_in'] <= plus_min))) &
            (
                ((pd.isna(right_lineup_df['min_out'])) | (pd.isna(right_lineup_df['plus_min_out']))) |
                ((right_lineup_df['min_out'] > min) | ((right_lineup_df['min_out'] == min) & (right_lineup_df['plus_min_out'] > plus_min)))
            )
            ]
        
        right_num_v = len(cur_right_lineup.loc[cur_right_lineup['name'] == 'вратарь'])
        right_num_z = len(cur_right_lineup.loc[cur_right_lineup['name'] == 'защитник'])
        right_num_p = len(cur_right_lineup.loc[cur_right_lineup['name'] == 'полузащитник'])
        right_num_n = len(cur_right_lineup.loc[cur_right_lineup['name'] == 'нападающий'])
        right_num_u = len(cur_right_lineup.loc[cur_right_lineup['name'] == 'неизвестно'])
        right_num_goal_g = len(right_goals_df.loc[((right_goals_df['min'] < min) | ((right_goals_df['min'] == min) & (right_goals_df['plus_min'] <= plus_min))) & (right_goals_df['name'] == 'гол')])
        right_num_goal_p = len(right_goals_df.loc[((right_goals_df['min'] < min) | ((right_goals_df['min'] == min) & (right_goals_df['plus_min'] <= plus_min))) & (right_goals_df['name'] == 'пенальти')])
        right_num_goal_a = len(right_goals_df.loc[((right_goals_df['min'] < min) | ((right_goals_df['min'] == min) & (right_goals_df['plus_min'] <= plus_min))) & (right_goals_df['name'] == 'автогол')])
        right_total_transfer_value = cur_right_lineup['transfer_value'].sum()
        right_avg_transfer_value = cur_right_lineup['transfer_value'].mean()
        right_avg_time_player_in_game = cur_right_lineup.apply(lambda row: min - row['min_in'], axis=1).mean()            
        
        
        left_right_transfer_value_div = left_total_transfer_value / right_total_transfer_value if right_total_transfer_value > 0 else 9999 # Разница в стоимости левой команды
        right_left_transfer_value_div = right_total_transfer_value / left_total_transfer_value if left_total_transfer_value > 0 else 9999 # Разница в стоимости левой команды
        
        
        if left_goal_score > right_goal_score: res_event = 1
        elif left_goal_score < right_goal_score: res_event = 2
        else: res_event = 0
            
        
        # Запись события
        new_row = {
            'game_id':[game_id],        
            'min':[min],
            'plus_min':[plus_min],
            'left_coach_id':[left_coach_id],
            'right_coach_id':[right_coach_id],
            'referee_id':[referee_id],
            'left_num_v':[left_num_v],
            'left_num_z':[left_num_z],
            'left_num_p':[left_num_p],
            'left_num_n':[left_num_n],
            'left_num_u':[left_num_u],
            'right_num_v':[right_num_v],
            'right_num_z':[right_num_z],
            'right_num_p':[right_num_p],
            'right_num_n':[right_num_n],
            'right_num_u':[right_num_u],
            'left_num_y':[left_num_y],
            'left_num_y2r':[left_num_y2r],
            'right_num_y':[right_num_y],
            'right_num_y2r':[right_num_y2r],
            'right_num_goal_g':[right_num_goal_g],
            'right_num_goal_p':[right_num_goal_p],
            'right_num_goal_a':[right_num_goal_a],
            'left_num_goal_g':[left_num_goal_g],
            'left_num_goal_p':[left_num_goal_p],
            'left_num_goal_a':[left_num_goal_a],
            'left_total_transfer_value':[left_total_transfer_value],
            'right_total_transfer_value':[right_total_transfer_value],
            'left_avg_transfer_value':[left_avg_transfer_value],
            'right_avg_transfer_value':[right_avg_transfer_value],
            'left_goal_score':[left_goal_score],
            'right_goal_score':[right_goal_score],
            'left_avg_time_player_in_game':[left_avg_time_player_in_game],
            'right_avg_time_player_in_game':[right_avg_time_player_in_game],
            'left_right_transfer_value_div':[left_right_transfer_value_div],
            'right_left_transfer_value_div':[right_left_transfer_value_div],
            'res_event':[res_event],
        }
        new_row_df = pd.DataFrame(new_row)
        print(new_row_df)
        # Добавление события в результирующий набор данных
        res_df = pd.concat([res_df, new_row_df], ignore_index=True)
    
    for _, row in res_df.iterrows():
        await AC.PredictionDrawLeftRight.insert_prediction_draw_left_right(
            game_id=int(row['game_id']),
            min=int(row['min']),
            plus_min=int(row['plus_min']),
            left_coach_id=int(row['left_coach_id']),
            right_coach_id=int(row['right_coach_id']),
            referee_id=int(row['referee_id']),
            left_num_v=int(row['left_num_v']),
            left_num_z=int(row['left_num_z']),
            left_num_p=int(row['left_num_p']),
            left_num_n=int(row['left_num_n']),
            left_num_u=int(row['left_num_u']),
            right_num_v=int(row['right_num_v']),
            right_num_z=int(row['right_num_z']),
            right_num_p=int(row['right_num_p']),
            right_num_n=int(row['right_num_n']),
            right_num_u=int(row['right_num_u']),
            left_num_y=int(row['left_num_y']),
            left_num_y2r=int(row['left_num_y2r']),
            right_num_y=int(row['right_num_y']),
            right_num_y2r=int(row['right_num_y2r']),
            right_num_goal_g=int(row['right_num_goal_g']),
            right_num_goal_p=int(row['right_num_goal_p']),
            right_num_goal_a=int(row['right_num_goal_a']),
            left_num_goal_g=int(row['left_num_goal_g']),
            left_num_goal_p=int(row['left_num_goal_p']),
            left_num_goal_a=int(row['left_num_goal_a']),
            left_total_transfer_value=float(row['left_total_transfer_value']),
            right_total_transfer_value=float(row['right_total_transfer_value']),
            left_avg_transfer_value=float(row['left_avg_transfer_value']),
            right_avg_transfer_value=float(row['right_avg_transfer_value']),
            left_goal_score=int(row['left_goal_score']),
            right_goal_score=int(row['right_goal_score']),
            left_avg_time_player_in_game=float(row['left_avg_time_player_in_game']),
            right_avg_time_player_in_game=float(row['right_avg_time_player_in_game']),
            left_right_transfer_value_div=float(row['left_right_transfer_value_div']),
            right_left_transfer_value_div=float(row['right_left_transfer_value_div']),
            res_event=int(row['res_event']),
        )

    
async def insert_active_game_info_db(season_id: str, season_game_id: str):
    
    game_page_link = GamePage.get_page_link(season_id=season_id, season_game_id=season_game_id)
        
    async with AsyncBrowserConnection() as br:
        game_page = GamePage(br, game_page_link)
        game: Game = game_page.get_info()
        
    game_status_id_played = 1 #
    game_status_id_pause = 2 #
    game_status_id_in_play = 3
    game_status_id_played_not_predicted = 5 #
    
    
    if AC.GAME_STATUS_DICT[game_status_id_played] != 'окончен': raise Exception('Идентифифактор оконченного матча был изменен')
    if AC.GAME_STATUS_DICT[game_status_id_pause] != 'перерыв': raise Exception('Идентифифактор матча на перерыве был изменен')
    if AC.GAME_STATUS_DICT[game_status_id_in_play] != 'игра': raise Exception('Идентифифактор матча в игре был изменен')
    if AC.GAME_STATUS_DICT[game_status_id_played_not_predicted] != 'окончен, не спрогнозирован': raise Exception('Идентификатор не спрогнозированного матча был изменен')
    
    
    game.id = season_game_id
    game.left_season_team_id = await AC.SeasonTeam.get_left_season_team_id_by_season_id_season_game_id(season_id=season_id, season_game_id=season_game_id)
    game.right_season_team_id = await AC.SeasonTeam.get_right_season_team_id_by_season_id_season_game_id(season_id=season_id, season_game_id=season_game_id)
    
    if game.is_played == game_status_id_played:
        game.is_played = game_status_id_played_not_predicted
    game_id = await insert_season_game_into_db(season_id=season_id, game=game)
    
    # Если игра на перерыве - заканчиваем обработку
    if game.is_played == game_status_id_pause:
        return
    
    if game.is_played == game_status_id_in_play:
        time_events = ((game.cur_min, game.cur_plus_min))
        await simulate_match(game_id=game_id, time_events=time_events, is_event_exist=True)
    
    if game.is_played == game_status_id_played_not_predicted:
        await simulate_match(game_id=game_id)
        

#season_id: str, active_season_game_id: list[str]
async def manage_active_game():
    season_id, active_season_game_id = await check_active_game_in_db()
    print(active_season_game_id)
    for season_game_id in active_season_game_id:
        task = asyncio.create_task(insert_active_game_info_db(season_id=season_id, season_game_id=season_game_id))
        await asyncio.gather(task)
    return


asyncio.run(manage_active_game())