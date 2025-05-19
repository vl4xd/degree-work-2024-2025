from datetime import datetime, time, date, timezone, timedelta

from sqlalchemy import Integer, or_, and_, func, insert, select, text, update
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError
import asyncio

import pandas as pd
from ..database import async_session_factory, sync_session_factory
from ..schemasDto import (SeasonDto, SeasonAddDto,
                          GameAddDto, GameDto,
                          SortSeasonGameDto,
                          SeasonTeamAddDto,
                          PredictionDrawLeftRightDto, GamePredictionDrowLeftRightDto)
from collection.pages import (SeasonPage, GamePage, TeamPage)


class SyncCore:
    
    @staticmethod
    def get_team_id_list() -> list[str]:
        with sync_session_factory() as session:
            query = text('''
                         SELECT team_id
                         FROM team
                         ''')
            return session.scalars(query).all()
    
    
    @staticmethod
    def get_table_names() -> list[str]:
        with sync_session_factory() as session:
            query = text('''
                         SELECT tablename 
                         FROM pg_catalog.pg_tables
                         WHERE schemaname='public' AND tablename!='alembic_version'
                         ''')
            return session.scalars(query).all()
      
      
    @staticmethod
    def get_amplua_id_for_name(name: str) -> int:
        try:
            with sync_session_factory() as session:
                query = text('''SELECT amplua_id FROM amplua WHERE name=:name
                            ''')
                query = query.bindparams(
                    name=name
                )
                res = session.execute(query)
                amplua = res.one_or_none()
                existed_amplua_id = None if amplua is None else amplua.amplua_id
                return existed_amplua_id
        except Exception as e:
            session.rollback()
            raise
            
    
    @staticmethod
    def get_lineup_player_stat_for_game(game_id: int) -> pd.DataFrame:
        '''Сводная таблица о статистике игровок, учавствующих в игре'''
        
        # https://stackoverflow.com/questions/2281551/tsql-left-join-and-only-last-row-from-right
        with sync_session_factory() as session:
            query = text('''SELECT
                            lineup.player_id,
                            player_stat.transfer_value,
                            player_stat.amplua_id,
                            amplua.name
                            FROM game
                            LEFT JOIN lineup ON game.game_id=lineup.game_id
                            LEFT JOIN player_stat 
                            ON lineup.player_id=player_stat.player_id AND 
                            game.season_id=player_stat.season_id AND 
                            player_stat.player_stat_id=(
                                SELECT MAX(player_stat_id) FROM player_stat WHERE player_stat.player_id=lineup.player_id AND player_stat.season_id=game.season_id
                            )
                            LEFT JOIN amplua ON player_stat.amplua_id=amplua.amplua_id
                            WHERE game.game_id=:game_id
                         ''')
            query = query.bindparams(
                game_id=game_id
            )
            df = pd.read_sql(query, session.connection())
            
        df['transfer_value'] = df['transfer_value'].fillna(0)
        unkwn_amplua_name = 'неизвестно'
        unkwn_amplua_id = SyncCore.get_amplua_id_for_name(unkwn_amplua_name)
        df['amplua_id'] = df['amplua_id'].fillna(unkwn_amplua_id)
        df['name'] = df['name'].fillna(unkwn_amplua_name)
        
        return df
            


class AsyncCore:
    '''Класс для асинхронного обращения к базе данных'''
    
    UNDEFINED_GAME_STATUS_ID = 4
    GAME_STATUS_DICT = {
        0: 'не начался',
        1: 'окончен',
        2: 'перерыв',
        3: 'игра',
        UNDEFINED_GAME_STATUS_ID: 'не определен',
        5: 'окончен, не спрогнозирован'
    }
    
    @staticmethod
    async def get_moscow_datetime_now() -> datetime:
        utc_datetime_now = datetime.now(timezone.utc)
        moscow_datetime_now = utc_datetime_now + timedelta(hours=3) # МСК (UTC+3)
        return moscow_datetime_now.replace(tzinfo=None) # убираем часовой пояс
    
    async def check_sort_type(sort_type: str):
        if sort_type.lower() == 'desc' or sort_type.lower() == 'asc':
            return True
        return False
    
    class Player:
        
        @staticmethod
        async def is_player_id_exist(player_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM player
                                 WHERE player_id=:player_id
                                 ''')
                    query = query.bindparams(
                        player_id=player_id
                    )
                    res = await session.execute(query)                    
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_player(
            player_id: str,
            first_name: str | None = None,
            last_name: str | None = None,
            birth_date: date | None = None
        ):
            if await AsyncCore.Player.is_player_id_exist(player_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO player (player_id, first_name, last_name, birth_date)
                                VALUES (
                                    :player_id,
                                    :first_name,
                                    :last_name,
                                    :birth_date
                                    )''')
                    stmt = stmt.bindparams(
                        player_id=player_id,
                        first_name=first_name,
                        last_name=last_name,
                        birth_date=birth_date,
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def update_player_data(
            player_id: str,
            first_name: str | None = None,
            last_name: str | None = None,
            birth_date: date | None = None
        ):
            # выход если игрока не существует
            if not await AsyncCore.Player.is_player_id_exist(player_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                UPDATE player
                                SET first_name=:first_name, last_name=:last_name, birth_date=:birth_date
                                WHERE player_id=:player_id
                                ''')
                    stmt = stmt.bindparams(
                        first_name=first_name,
                        last_name=last_name,
                        birth_date=birth_date,
                        player_id=player_id
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
    
    class Coach:
        '''Подкласс для взаимодействия с таблицей Coach'''
        @staticmethod
        async def is_coach_id_exist(coach_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM coach
                                 WHERE coach_id=:coach_id
                                 ''')
                    query = query.bindparams(
                        coach_id=coach_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def insert_coach(
            coach_id: str,
            first_name: str | None = None,
            middle_name: str | None = None,
            last_name: str | None = None,
            birth_date: date | None = None):
            
            if await AsyncCore.Coach.is_coach_id_exist(coach_id):
                return
                
            
            async with async_session_factory() as session:
                try:   
                    stmt = text('''
                                INSERT INTO coach (coach_id, first_name, middle_name, last_name, birth_date)
                                VALUES (
                                    :coach_id,
                                    :first_name,
                                    :middle_name,
                                    :last_name,
                                    :birth_date
                                    )''')
                    stmt = stmt.bindparams(
                        coach_id=coach_id,
                        first_name=first_name,
                        middle_name=middle_name,
                        last_name=last_name,
                        birth_date=birth_date,
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
    
    class Amplua:
        
        UNDEFINED_AMPLUA_NAME = 'неизвестно'
        
        @staticmethod
        async def is_amplua_name_exist(name: str) -> int | None:
            
            if name is None: name = AsyncCore.Amplua.UNDEFINED_AMPLUA_NAME
            
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM amplua
                                 WHERE name=:name
                                 ''')
                    query = query.bindparams(
                        name=name
                    )
                    res = await session.execute(query)
                    amplua = res.one_or_none()
                    existed_amplua_id =  None if amplua is None else amplua.amplua_id
                    return existed_amplua_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_amplua(name: str) -> int:
            
            if name is None: name = AsyncCore.Amplua.UNDEFINED_AMPLUA_NAME
            
            amplua_id = await AsyncCore.Amplua.is_amplua_name_exist(name)
            if amplua_id is not None:
                return amplua_id
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO amplua (name)
                                VALUES (:name)
                                RETURNING amplua_id''')
                    stmt = stmt.bindparams(
                        name=name,
                    )
                    res = await session.execute(stmt)
                    amplua_id = res.scalar()
                    await session.commit()
                    return amplua_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Season:
        
        @staticmethod
        async def get_season_list() -> list[SeasonAddDto]:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season''')
                    res = await session.execute(query)
                    seasons = res.all()
                    
                    season_list = [
                        SeasonAddDto(
                            season_id=row.season_id,
                            start_date=row.start_date,
                            end_date=row.end_date,
                            season_url=SeasonPage.get_page_link(row.season_id)
                        )
                        for row in seasons
                    ]
                    return season_list
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def get_current_season_id() -> str | None:
            
            current_datetime = await AsyncCore.get_moscow_datetime_now()
            current_date = current_datetime.date()
            
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season
                                 WHERE start_date <= :current_date AND end_date >= :current_date
                                 ''')
                    query = query.bindparams(
                        current_date=current_date
                    )
                    res = await session.execute(query)
                    season = res.one_or_none()
                    existed_season_id =  None if season is None else season.season_id
                    return existed_season_id
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def is_season_id_exist(season_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season
                                 WHERE season_id=:season_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_season(season_id: str, 
                                start_date: date | None, 
                                end_date: date | None):
            if await AsyncCore.Season.is_season_id_exist(season_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO season (season_id, start_date, end_date)
                                VALUES (
                                    :season_id,
                                    :start_date,
                                    :end_date
                                    )''')
                    stmt = stmt.bindparams(
                        season_id=season_id,
                        start_date=start_date,
                        end_date=end_date
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class PlayerStat:
        
        @staticmethod
        async def is_player_stat_exist(player_id: str,
                                       season_id: str) -> bool:
            '''
            Метод для проверки ранее добавленной записи player_stat для сезона
            
            НЕ ИСПОЛЬЗОВАТЬ! Применяется только для стартового заполнения
            '''
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM player_stat
                                 WHERE player_id=:player_id AND season_id=:season_id
                                 ''')
                    query = query.bindparams(
                        player_id=player_id,
                        season_id=season_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
            
            
        
        @staticmethod
        async def insert_player_stat(player_id: str,
                                     amplua: str,
                                     season_id: str,
                                     number: int,
                                     growth: int,
                                     weight: int,
                                     transfer_value: int):
            amplua_id = await AsyncCore.Amplua.insert_amplua(amplua)
            
            async with async_session_factory() as session:                
                try:
                    stmt = text('''
                                INSERT INTO player_stat (player_id, amplua_id, season_id, number, growth, weight, transfer_value, created_at)
                                VALUES (
                                    :player_id,
                                    :amplua_id,
                                    :season_id,
                                    :number,
                                    :growth,
                                    :weight,
                                    :transfer_value,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        player_id=player_id,
                        amplua_id=amplua_id,
                        season_id=season_id,
                        number=number,
                        growth=growth,
                        weight=weight,
                        transfer_value=transfer_value,
                        created_at=created_at,
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Team:
                
        @staticmethod
        async def is_team_id_exist(team_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM team
                                 WHERE team_id=:team_id
                                 ''')
                    query = query.bindparams(
                        team_id=team_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def insert_team(team_id: str,
                              name: str):
            if await AsyncCore.Team.is_team_id_exist(team_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO team (team_id, name)
                                VALUES (
                                    :team_id,
                                    :name
                                    )''')
                    stmt = stmt.bindparams(
                        team_id=team_id,
                        name=name
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
    
    class SeasonTeam:
        
        @staticmethod
        async def get_season_team_list(season_id: str) -> list[SeasonTeamAddDto]:
            async with async_session_factory() as session:
                try:
                    base_query = text('''
                                 SELECT 
                                    season_team.season_id AS season_id,
                                    season_team.team_id AS team_id,
                                    season_team.season_team_id AS season_team_id,
                                    team.name AS name
                                 FROM season_team
                                 LEFT JOIN team ON season_team.team_id=team.team_id
                                 ''')
                    
                    conditions = []
                    params = {}
                    
                    if season_id:
                        conditions.append(text("season_id=:season_id"))
                        params['season_id'] = season_id
                    
                    if conditions:
                        where_clause = and_(*conditions)
                        query = text(f'{base_query}' + ' WHERE ' + str(where_clause))
                    else:
                        query = base_query
                    
                    res = await session.execute(query, params)

                    season_teams = res.all()
                    
                    season_team_list = [
                        SeasonTeamAddDto(
                            season_id=row.season_id,
                            team_id=row.team_id,
                            season_team_id=row.season_team_id,
                            name=row.name,
                            season_team_url=TeamPage.get_page_link(row.season_id, row.season_team_id)
                        )
                        for row in season_teams
                    ]
                    return season_team_list
                except Exception as e:
                    await session.rollback()
                    raise
                
        
        @staticmethod
        async def is_season_id_team_id_exist(season_id: str,
                                             team_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season_team
                                 WHERE season_id=:season_id AND team_id=:team_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id,
                        team_id=team_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_season_team(season_id: str,
                                     team_id: str,
                                     season_team_id: str):
            if await AsyncCore.SeasonTeam.is_season_id_team_id_exist(season_id, team_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO season_team (season_id, team_id, season_team_id)
                                VALUES (
                                    :season_id,
                                    :team_id,
                                    :season_team_id
                                    )''')
                    stmt = stmt.bindparams(
                        season_id=season_id,
                        team_id=team_id,
                        season_team_id=season_team_id
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def get_team_id_by_season_id_season_team_id(season_id: str,
                              season_team_id: str) -> str | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season_team
                                 WHERE season_id=:season_id AND season_team_id=:season_team_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id,
                        season_team_id=season_team_id
                    )
                    res = await session.execute(query)
                    team = res.one_or_none()
                    team_id = None if team is None else team.team_id
                    return team_id
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def get_season_team_id_by_season_id_team_id(season_id: str,
                                     team_id: str) -> str | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM season_team
                                 WHERE season_id=:season_id AND team_id=:team_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id,
                        team_id=team_id
                    )
                    res = await session.execute(query)
                    season_team = res.one_or_none()
                    season_team_id = None if season_team is None else season_team.season_team_id
                    return season_team_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def get_left_season_team_id_by_season_id_season_game_id(season_id: str,
                                                        season_game_id: str) -> str | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT left_team_id FROM game
                                 WHERE season_id=:season_id AND season_game_id=:season_game_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id,
                        season_game_id=season_game_id
                    )
                    res = await session.execute(query)
                    team_id = res.one_or_none()[0]
                    season_team_id = await AsyncCore.SeasonTeam.get_season_team_id_by_season_id_team_id(season_id=season_id, team_id=team_id)
                    return season_team_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        
        @staticmethod
        async def get_right_season_team_id_by_season_id_season_game_id(season_id: str,
                                                        season_game_id: str) -> str | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT right_team_id FROM game
                                 WHERE season_id=:season_id AND season_game_id=:season_game_id
                                 ''')
                    query = query.bindparams(
                        season_id=season_id,
                        season_game_id=season_game_id
                    )
                    res = await session.execute(query)
                    team_id = res.one_or_none()[0]
                    season_team_id = await AsyncCore.SeasonTeam.get_season_team_id_by_season_id_team_id(season_id=season_id, team_id=team_id)
                    return season_team_id
                except Exception as e:
                    await session.rollback()
                    raise
        
                
    class TeamPlayer:
        
        @staticmethod
        async def is_team_id_season_id_player_id_exist(team_id: str,
                                                       season_id: str,
                                                       player_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM team_player
                                 WHERE team_id=:team_id AND season_id=:season_id AND player_id=:player_id
                                 ''')
                    query = query.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        player_id=player_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_team_player_for_team_id(team_id: str,
                                     season_id: str,
                                     player_id: str,
                                     is_active: bool = True):
            if await AsyncCore.TeamPlayer.is_team_id_season_id_player_id_exist(team_id, season_id, player_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO team_player (team_id, season_id, player_id, is_active, created_at, updated_at)
                                VALUES (
                                    :team_id,
                                    :season_id,
                                    :player_id,
                                    :is_active,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        player_id=player_id,
                        is_active=is_active,
                        created_at=created_at,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_team_player_for_season_team_id(season_team_id: str,
                                                        season_id: str,
                                                        player_id: str,
                                                        is_active: bool = True) -> bool:
            '''
            Возвращает True если запись была в противном случае False
            
            (для проверки наличия player_stat поскольку если игрок не был в записи команды, значит его статистика не была собрана)
            '''
            
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.TeamPlayer.is_team_id_season_id_player_id_exist(team_id, season_id, player_id):
                return True
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO team_player (team_id, season_id, player_id, is_active, created_at, updated_at)
                                VALUES (
                                    :team_id,
                                    :season_id,
                                    :player_id,
                                    :is_active,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        player_id=player_id,
                        is_active=is_active,
                        created_at=created_at,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                    return False
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def set_is_active_false(team_id: str,
                                      season_id: str):
            '''
            Функция для установки у всех игроков команды в сезоне is_active=False.
            
            Для следующей итерации по собранным данным об активных игроках и установки is_active=True.'''
            pass
        
    class TeamCoach:
        
        @staticmethod
        async def is_team_id_season_id_coach_id_exist(team_id: str,
                                                       season_id: str,
                                                       coach_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM team_coach
                                 WHERE team_id=:team_id AND season_id=:season_id AND coach_id=:coach_id
                                 ''')
                    query = query.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        coach_id=coach_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_team_coach_for_team_id(team_id: str,
                                     season_id: str,
                                     coach_id: str,
                                     is_active: bool = True):
            if await AsyncCore.TeamCoach.is_team_id_season_id_coach_id_exist(team_id, season_id, coach_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO team_coach (team_id, season_id, coach_id, is_active, created_at, updated_at)
                                VALUES (
                                    :team_id,
                                    :season_id,
                                    :coach_id,
                                    :is_active,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        coach_id=coach_id,
                        is_active=is_active,
                        created_at=created_at,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
            
        @staticmethod
        async def insert_team_coach_for_season_team_id(season_team_id: str,
                                     season_id: str,
                                     coach_id: str,
                                     is_active: bool = True):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.TeamCoach.is_team_id_season_id_coach_id_exist(team_id, season_id, coach_id):
                return
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO team_coach (team_id, season_id, coach_id, is_active, created_at, updated_at)
                                VALUES (
                                    :team_id,
                                    :season_id,
                                    :coach_id,
                                    :is_active,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        team_id=team_id,
                        season_id=season_id,
                        coach_id=coach_id,
                        is_active=is_active,
                        created_at=created_at,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def set_is_active_false(team_id: str,
                                      season_id: str):
            '''
            Функция для установки у всех игроков команды в сезоне is_active=False.
            
            Для следующей итерации по собранным данным об активных игроках и установки is_active=True.'''
            pass
        
    class GameStatus:
        
        @staticmethod
        async def is_game_status_id_exist(game_status_id: int) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM game_status
                                 WHERE game_status_id=:game_status_id
                                 ''')
                    query = query.bindparams(
                        game_status_id=game_status_id,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_game_status(game_status_id: int) -> int:
            try:
                name = AsyncCore.GAME_STATUS_DICT[game_status_id]
            except KeyError:
                game_status_id = AsyncCore.UNDEFINED_GAME_STATUS_ID
                name = AsyncCore.GAME_STATUS_DICT[game_status_id]
            
            if await AsyncCore.GameStatus.is_game_status_id_exist(game_status_id):
                return game_status_id
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO game_status (game_status_id, name)
                                VALUES (
                                    :game_status_id,
                                    :name
                                    )''')
                    
                    stmt = stmt.bindparams(
                        game_status_id=game_status_id,
                        name=name
                    )
                    await session.execute(stmt)
                    await session.commit()
                    return game_status_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Game:
        
        async def get_game_list(season_id: str,
                                sort_type: str,
                                game_statuses: list[int],
                                left_team_id: str,
                                right_team_id: str,
                                from_start_date: date,
                                to_start_date: date,
                                limit,
                                offset) -> list[GameAddDto]:
            async with async_session_factory() as session:
                try:
                    base_query = text('''
                                 SELECT * FROM game''')
                    
                    conditions = []
                    params = {}
                    
                    if season_id:
                        conditions.append(text("season_id=:season_id"))
                        params['season_id'] = season_id

                    if len(game_statuses) > 0:
                        game_status_conditions = []
                        for game_status_id in game_statuses:
                            game_status_conditions.append(text(f"game_status_id = {game_status_id}"))
                        conditions.append(or_(*game_status_conditions))

                    if left_team_id:
                        conditions.append(text("left_team_id=:left_team_id"))
                        params['left_team_id'] = left_team_id

                    if right_team_id:
                        conditions.append(text("right_team_id=:right_team_id"))
                        params['right_team_id'] = right_team_id

                    if from_start_date:
                        conditions.append(text("start_date>=:from_start_date"))
                        params['from_start_date'] = from_start_date

                    if to_start_date:
                        conditions.append(text("start_date<=:to_start_date"))
                        params['to_start_date'] = to_start_date
                    
                    if conditions:
                        where_clause = and_(*conditions)
                        query = text('SELECT * FROM game WHERE ' + str(where_clause))
                    else:
                        query = base_query
                    
                    if await AsyncCore.check_sort_type(sort_type):
                        query = text(str(query) + ' ORDER BY start_date ' + sort_type)
                        
                    query = text(str(query) + ' LIMIT :limit OFFSET :offset')
                    query = query.bindparams(
                        limit=limit,
                        offset=offset
                    )
                    
                    res = await session.execute(query, params)
                    
                    games = res.all()
                    
                    game_list = [
                        GameAddDto(
                            game_id=int(row.game_id),
                            season_game_id=row.season_game_id,
                            season_id=row.season_id,
                            left_team_id=row.left_team_id,
                            right_team_id=row.right_team_id,
                            game_status_id=int(row.game_status_id),
                            left_coach_id=row.left_coach_id,
                            right_coach_id=row.right_coach_id,
                            tour_number=int(row.tour_number),
                            start_date=row.start_date,
                            start_time=row.start_time,
                            min=row.min,
                            plus_min=row.plus_min,
                            created_at=row.created_at,
                            updated_at=row.updated_at,
                            game_url=GamePage.get_page_link(row.season_id, row.season_game_id)
                        )
                        for row in games
                    ]
                    return game_list
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        async def set_game_status_id_played_by_game_id(game_id: int):
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                UPDATE game
                                SET 
                                game_status_id=:game_status_id,  
                                updated_at=:updated_at
                                WHERE game_id=:game_id
                                ''')
                    
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    game_status_id = 1
                    if AsyncCore.GAME_STATUS_DICT[game_status_id] != 'окончен': raise Exception('Идентифифактор оконченного матча был изменен')
                    
                    stmt = stmt.bindparams(
                        game_status_id=game_status_id,
                        updated_at=updated_at,
                        game_id=game_id
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        async def get_game_status_id_by_game_id(game_id: int) -> int:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT game_status_id FROM game
                                 WHERE game_id=:game_id
                                 ''')
                    query = query.bindparams(
                        game_id=game_id
                    )
                    res = await session.execute(query)
                    return res.scalars().one()
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        async def get_active_season_game_id_for_collection(season_id: str) -> list[str]:
            
            current_datetime = await AsyncCore.get_moscow_datetime_now()
            current_date = current_datetime.date()
            current_time = current_datetime.time()
            
            game_status_id_played = 1
            game_status_id_played_not_predicted = 5
            
            if AsyncCore.GAME_STATUS_DICT[game_status_id_played_not_predicted] != 'окончен, не спрогнозирован': raise Exception('Идентификатор не спрогнозированного матча был изменен')
            if AsyncCore.GAME_STATUS_DICT[game_status_id_played] != 'окончен': raise Exception('Идентифифактор оконченного матча был изменен')
            
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT season_game_id FROM game
                                 WHERE 
                                 game_status_id NOT IN (:game_status_id_played, :game_status_id_played_not_predicted) AND 
                                 (start_date < :current_date OR (start_date = :current_date AND start_time <= :current_time)) AND
                                 season_id = :season_id
                                 ''')
                    query = query.bindparams(
                        game_status_id_played=game_status_id_played,
                        game_status_id_played_not_predicted=game_status_id_played_not_predicted,
                        current_date=current_date,
                        current_time=current_time,
                        season_id=season_id
                    )
                    res = await session.execute(query)
                    return res.scalars().all()
                except Exception as e:
                    await session.rollback()
                    raise
                
        
        async def get_active_season_game_id_for_prediction(season_id: str) -> list[str]:
            
            current_datetime = await AsyncCore.get_moscow_datetime_now()
            current_date = current_datetime.date()
            current_time = current_datetime.time()
            
            game_status_id_in_play = 3
            game_status_id_played_not_predicted = 5
            
            if AsyncCore.GAME_STATUS_DICT[game_status_id_in_play] != 'игра': raise Exception('Идентифифактор активного матча был изменен')
            if AsyncCore.GAME_STATUS_DICT[game_status_id_played_not_predicted] != 'окончен, не спрогнозирован': raise Exception('Идентификатор не спрогнозированного матча был изменен')
            
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT game_id FROM game
                                 WHERE 
                                 game_status_id IN (:game_status_id_in_play, :game_status_id_played_not_predicted) AND 
                                 (start_date < :current_date OR (start_date = :current_date AND start_time <= :current_time)) AND
                                 season_id = :season_id
                                 ''')
                    query = query.bindparams(
                        game_status_id_in_play=game_status_id_in_play,
                        game_status_id_played_not_predicted=game_status_id_played_not_predicted,
                        current_date=current_date,
                        current_time=current_time,
                        season_id=season_id
                    )
                    res = await session.execute(query)
                    return res.scalars().all()
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def is_season_game_id_season_id_exist(season_game_id: str, season_id: str) -> int | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM game
                                 WHERE season_game_id=:season_game_id AND season_id=:season_id
                                 ''')
                    query = query.bindparams(
                        season_game_id=season_game_id,
                        season_id=season_id
                    )
                    res = await session.execute(query)
                    game = res.one_or_none()
                    existed_game_id =  None if game is None else game.game_id
                    return existed_game_id
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def update_game(season_game_id: str, 
                              season_id: str,
                              game_status_id: int,
                              min: int,
                              plus_min: int,
                              left_coach_id: str,
                              right_coach_id: str):
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                UPDATE game
                                SET 
                                game_status_id=:game_status_id, 
                                min=:min, 
                                plus_min=:plus_min, 
                                updated_at=:updated_at,
                                left_coach_id=:left_coach_id, 
                                right_coach_id=:right_coach_id 
                                WHERE season_game_id=:season_game_id AND season_id=:season_id
                                ''')
                    
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        game_status_id=game_status_id,
                        min=min,
                        plus_min=plus_min,
                        season_game_id=season_game_id,
                        season_id=season_id,
                        updated_at=updated_at,
                        left_coach_id=left_coach_id,
                        right_coach_id=right_coach_id
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def insert_game(season_game_id: str, 
                              season_id: str,
                              left_season_team_id: str,
                              right_season_team_id: str,
                              left_coach_id: str,
                              right_coach_id: str,
                              game_status_id: int,
                              tour_number: int,
                              start_date: date,
                              start_time: time,
                              min: int,
                              plus_min: int,) -> int | None:
            game_id = await AsyncCore.Game.is_season_game_id_season_id_exist(season_game_id, season_id)
            
            left_team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, left_season_team_id)
            right_team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, right_season_team_id)
            
            if not await AsyncCore.GameStatus.is_game_status_id_exist(game_status_id):
                game_status_id = await AsyncCore.GameStatus.insert_game_status(game_status_id)
            
            if game_id is not None:
                await AsyncCore.Game.update_game(season_game_id=season_game_id,
                                           season_id=season_id,
                                           game_status_id=game_status_id,
                                           min=min,
                                           plus_min=plus_min,
                                           left_coach_id=left_coach_id,
                                           right_coach_id=right_coach_id)
                return game_id
            
            if left_team_id is None:
                print(f'Команда {left_team_id=} не найдена в таблице SeasonTeam')
                return None
            if right_team_id is None:
                print(f'Команда {right_team_id=} не найдена в таблице SeasonTeam')
                return None
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO game (
                                    season_game_id, 
                                    season_id, 
                                    left_team_id, 
                                    right_team_id, 
                                    game_status_id, 
                                    tour_number, 
                                    start_date,
                                    start_time,
                                    min,
                                    plus_min,
                                    created_at,
                                    updated_at,
                                    left_coach_id,
                                    right_coach_id
                                    )
                                VALUES (
                                    :season_game_id, 
                                    :season_id, 
                                    :left_team_id, 
                                    :right_team_id, 
                                    :game_status_id, 
                                    :tour_number, 
                                    :start_date,
                                    :start_time,
                                    :min,
                                    :plus_min,
                                    :created_at,
                                    :updated_at,
                                    :left_coach_id,
                                    :right_coach_id
                                    )
                                RETURNING game_id''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        season_game_id=season_game_id, 
                        season_id=season_id, 
                        left_team_id=left_team_id, 
                        right_team_id=right_team_id, 
                        game_status_id=game_status_id, 
                        tour_number=tour_number, 
                        start_date=start_date,
                        start_time=start_time,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at,
                        updated_at=updated_at,
                        left_coach_id=left_coach_id,
                        right_coach_id=right_coach_id
                    )
                    res = await session.execute(stmt)
                    game_id = res.scalar()
                    await session.commit()
                    return game_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class GoalType:
        
        @staticmethod
        async def is_goal_type_name_exist(name: str) -> int | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM goal_type
                                 WHERE name=:name
                                 ''')
                    query = query.bindparams(
                        name=name
                    )
                    res = await session.execute(query)
                    goal_type = res.one_or_none()
                    existed_goal_type_id =  None if goal_type is None else goal_type.goal_type_id
                    return existed_goal_type_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_goal_type(name: str) -> int:
            goal_type_id = await AsyncCore.GoalType.is_goal_type_name_exist(name)
            if goal_type_id is not None:
                return goal_type_id
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO goal_type (name)
                                VALUES (:name)
                                RETURNING goal_type_id''')
                    stmt = stmt.bindparams(
                        name=name,
                    )
                    res = await session.execute(stmt)
                    goal_type_id = res.scalar()
                    await session.commit()
                    return goal_type_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Goal:
        
        @staticmethod
        async def is_goal_exist(game_id: int,
                                team_id: str,
                                player_id: str,
                                player_sub_id: str | None,
                                goal_type_name: str,
                                min: int,
                                plus_min: int) -> bool:
            async with async_session_factory() as session:
                # SQL выражение NULL = NULL возвращает NULL (фактически False). Для player_sub_id и plus_min нужно использовать IS NOT DISTINCT FROM
                try:
                    query = text('''
                                 SELECT * FROM goal
                                 WHERE 
                                 game_id=:game_id AND
                                 team_id=:team_id AND
                                 player_id=:player_id AND
                                 player_sub_id IS NOT DISTINCT FROM :player_sub_id AND
                                 goal_type_id=:goal_type_id AND
                                 min=:min AND
                                 plus_min IS NOT DISTINCT FROM :plus_min
                                 ''')
                    
                    goal_type_id = await AsyncCore.GoalType.insert_goal_type(goal_type_name)
                    
                    query = query.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        player_sub_id=player_sub_id,
                        goal_type_id=goal_type_id,
                        min=min,
                        plus_min=plus_min,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def insert_goal_for_team_id(game_id: int,
                              team_id: str,
                              player_id: str,
                              player_sub_id: str | None,
                              goal_type_name: str,
                              min: int,
                              plus_min: int):  
            
            if await AsyncCore.Goal.is_goal_exist(game_id, team_id, player_id, player_sub_id, goal_type_name, min, plus_min):
                return
                   
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO goal (
                                    game_id,
                                    team_id,
                                    player_id,
                                    player_sub_id,
                                    goal_type_id,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :player_sub_id,
                                    :goal_type_id,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    goal_type_id = await AsyncCore.GoalType.insert_goal_type(goal_type_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        player_sub_id=player_sub_id,
                        goal_type_id=goal_type_id,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_goal_for_season_team_id(game_id: int,
                                                 season_id: str,
                                                 season_team_id: str,
                                                 player_id: str,
                                                 player_sub_id: str | None,
                                                 goal_type_name: str,
                                                 min: int,
                                                 plus_min: int):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.Goal.is_goal_exist(game_id, team_id, player_id, player_sub_id, goal_type_name, min, plus_min):
                return
                  
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO goal (
                                    game_id,
                                    team_id,
                                    player_id,
                                    player_sub_id,
                                    goal_type_id,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :player_sub_id,
                                    :goal_type_id,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    goal_type_id = await AsyncCore.GoalType.insert_goal_type(goal_type_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        player_sub_id=player_sub_id,
                        goal_type_id=goal_type_id,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Referee:
        
        @staticmethod
        async def is_referee_id_exist(referee_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM referee
                                 WHERE referee_id=:referee_id
                                 ''')
                    query = query.bindparams(
                        referee_id=referee_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def insert_referee(
            referee_id: str,
            first_name: str | None,
            last_name: str | None):
            
            if await AsyncCore.Referee.is_referee_id_exist(referee_id):
                return
                
            
            async with async_session_factory() as session:
                try:   
                    stmt = text('''
                                INSERT INTO referee (referee_id, first_name, last_name)
                                VALUES (
                                    :referee_id,
                                    :first_name,
                                    :last_name)''')
                    stmt = stmt.bindparams(
                        referee_id=referee_id,
                        first_name=first_name,
                        last_name=last_name,
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class RefereeGame:
        
        @staticmethod
        async def is_referee_game_exist(referee_id: str,
                                        game_id: int) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM referee_game
                                 WHERE referee_id=:referee_id AND game_id=:game_id
                                 ''')
                    query = query.bindparams(
                        referee_id=referee_id,
                        game_id=game_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def insert_referee_game(referee_id: str,
                                      game_id: int):
            
            if await AsyncCore.RefereeGame.is_referee_game_exist(referee_id, game_id):
                return
                
            
            async with async_session_factory() as session:
                try:   
                    stmt = text('''
                                INSERT INTO referee_game (referee_id, game_id)
                                VALUES (
                                    :referee_id,
                                    :game_id)''')
                    stmt = stmt.bindparams(
                        referee_id=referee_id,
                        game_id=game_id,
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class PenaltyType:
        
        @staticmethod
        async def is_penalty_type_name_exist(name: str) -> int | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM penalty_type
                                 WHERE name=:name
                                 ''')
                    query = query.bindparams(
                        name=name
                    )
                    res = await session.execute(query)
                    penalty_type = res.one_or_none()
                    existed_penalty_type_id =  None if penalty_type is None else penalty_type.penalty_type_id
                    return existed_penalty_type_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_penalty_type(name: str) -> int:
            penalty_type_id = await AsyncCore.PenaltyType.is_penalty_type_name_exist(name)
            if penalty_type_id is not None:
                return penalty_type_id
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO penalty_type (name)
                                VALUES (:name)
                                RETURNING penalty_type_id''')
                    stmt = stmt.bindparams(
                        name=name,
                    )
                    res = await session.execute(stmt)
                    penalty_type_id = res.scalar()
                    await session.commit()
                    return penalty_type_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Penalty:
        
        @staticmethod
        async def is_penalty_exist(game_id: int,
                                   team_id: str,
                                   player_id: str,
                                   penalty_type_name: str,) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM penalty
                                 WHERE game_id=:game_id AND team_id=:team_id AND player_id=:player_id AND penalty_type_id=:penalty_type_id
                                 ''')
                    
                    penalty_type_id = await AsyncCore.PenaltyType.insert_penalty_type(penalty_type_name)
                    
                    query = query.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        penalty_type_id=penalty_type_id
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def insert_penalty_for_team_id(game_id: int,
                              team_id: str,
                              player_id: str,
                              penalty_type_name: str,
                              min: int,
                              plus_min: int):
            if await AsyncCore.Penalty.is_penalty_exist(game_id, team_id, player_id, penalty_type_name):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO penalty (
                                    game_id,
                                    team_id,
                                    player_id,
                                    penalty_type_id,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :penalty_type_id,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    penalty_type_id = await AsyncCore.PenaltyType.insert_penalty_type(penalty_type_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        penalty_type_id=penalty_type_id,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_penalty_for_season_team_id(game_id: int,
                                                    season_id: str,
                              season_team_id: str,
                              player_id: str,
                              penalty_type_name: str,
                              min: int,
                              plus_min: int):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.Penalty.is_penalty_exist(game_id, team_id, player_id, penalty_type_name):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO penalty (
                                    game_id,
                                    team_id,
                                    player_id,
                                    penalty_type_id,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :penalty_type_id,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    penalty_type_id = await AsyncCore.PenaltyType.insert_penalty_type(penalty_type_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        penalty_type_id=penalty_type_id,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
    
    class Lineup:
        
        @staticmethod
        async def is_lineup_exist(game_id: int,
                                  team_id: str,
                                  player_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM lineup
                                 WHERE game_id=:game_id AND team_id=:team_id AND player_id=:player_id
                                 ''')
                    
                    query = query.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_lineup_for_team_id(game_id: int,
                              team_id: str,
                              player_id: str,
                              min_in: int,
                              plus_min_in: int,
                              min_out: int,
                              plus_min_out: int):
            if await AsyncCore.Lineup.is_lineup_exist(game_id, team_id, player_id):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO lineup (
                                    game_id,
                                    team_id,
                                    player_id,
                                    min_in,
                                    plus_min_in,
                                    min_out,
                                    plus_min_out,
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :min_in,
                                    :plus_min_in,
                                    :min_out,
                                    :plus_min_out,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        min_in=min_in,
                        plus_min_in=plus_min_in,
                        min_out=min_out,
                        plus_min_out=plus_min_out,
                        created_at=created_at,
                        updated_at=updated_at
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_lineup_for_season_team_id(game_id: int,
                                                   season_id: str,
                              season_team_id: str,
                              player_id: str,
                              min_in: int,
                              plus_min_in: int,
                              min_out: int,
                              plus_min_out: int):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.Lineup.is_lineup_exist(game_id, team_id, player_id):
                await AsyncCore.Lineup.update_lineup_for_team_id(game_id=game_id,
                                                                 team_id=team_id,
                                                                 player_id=player_id,
                                                                 min_in=min_in,
                                                                 plus_min_in=plus_min_in,
                                                                 min_out=min_out,
                                                                 plus_min_out=plus_min_out)
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO lineup (
                                    game_id,
                                    team_id,
                                    player_id,
                                    min_in,
                                    plus_min_in,
                                    min_out,
                                    plus_min_out,
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :min_in,
                                    :plus_min_in,
                                    :min_out,
                                    :plus_min_out,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        min_in=min_in,
                        plus_min_in=plus_min_in,
                        min_out=min_out,
                        plus_min_out=plus_min_out,
                        created_at=created_at,
                        updated_at=updated_at
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def update_lineup_for_team_id(game_id: int,
                                            team_id: str,
                                            player_id: str,
                                            min_in: int,
                                            plus_min_in: int,
                                            min_out: int,
                                            plus_min_out: int):
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                UPDATE lineup
                                SET min_in=:min_in, plus_min_in=:plus_min_in, min_out=:min_out, plus_min_out=:plus_min_out, updated_at=:updated_at
                                WHERE game_id=:game_id AND team_id=:team_id AND player_id=:player_id
                                ''')
                    
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        min_in=min_in,
                        plus_min_in=plus_min_in,
                        min_out=min_out,
                        plus_min_out=plus_min_out,
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
                
    class Save:
        @staticmethod
        async def is_save_exist(game_id: int,
                                  team_id: str,
                                  player_id: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM save
                                 WHERE game_id=:game_id AND team_id=:team_id AND player_id=:player_id
                                 ''')
                    
                    query = query.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_save_for_team_id(game_id: int,
                              team_id: str,
                              player_id: str,
                              count: int):
            if await AsyncCore.Save.is_save_exist(game_id, team_id, player_id):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO save (
                                    game_id,
                                    team_id,
                                    player_id,
                                    count,
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :count,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        count=count,
                        created_at=created_at,
                        updated_at=updated_at
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_save_for_season_team_id(game_id: int,
                                                 season_id: str,
                              season_team_id: str,
                              player_id: str,
                              count: int):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.Save.is_save_exist(game_id, team_id, player_id):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO save (
                                    game_id,
                                    team_id,
                                    player_id,
                                    count,
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :player_id,
                                    :count,
                                    :created_at,
                                    :updated_at
                                    )''')
                    
                    datetime_now = await AsyncCore.get_moscow_datetime_now()
                    created_at, updated_at  = datetime_now, datetime_now
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        count=count,
                        created_at=created_at,
                        updated_at=updated_at
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class Stat:
        
        @staticmethod
        async def is_name_exist(name: str) -> int | None:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM stat
                                 WHERE name=:name
                                 ''')
                    query = query.bindparams(
                        name=name
                    )
                    res = await session.execute(query)
                    stat = res.one_or_none()
                    existed_stat_id =  None if stat is None else stat.stat_id
                    return existed_stat_id
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_stat(name: str) -> int:
            stat_id = await AsyncCore.Stat.is_name_exist(name)
            if stat_id is not None:
                return stat_id
            
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO stat (name)
                                VALUES (:name)
                                RETURNING stat_id''')
                    stmt = stmt.bindparams(
                        name=name,
                    )
                    res = await session.execute(stmt)
                    stat_id = res.scalar()
                    await session.commit()
                    return stat_id
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
    class GameStat:
        
        @staticmethod
        async def is_game_stat_exist(game_id: int,
                                     team_id: str,
                                     stat_name: str) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM game_stat
                                 WHERE game_id=:game_id AND team_id=:team_id AND stat_id=:stat_id
                                 ''')
                    
                    stat_id = await AsyncCore.Stat.insert_stat(stat_name)
                    
                    query = query.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        stat_id=stat_id,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_game_stat_for_team_id(game_id: int,
                                   team_id: str,
                                   stat_name: str,
                                   count: int,
                                   min: int,
                                   plus_min: int):
            
            if await AsyncCore.GameStat.is_game_stat_exist(game_id, team_id, stat_name):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO game_stat (
                                    game_id,
                                    team_id,
                                    stat_id,
                                    count,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :stat_id,
                                    :count,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    stat_id = await AsyncCore.Stat.insert_stat(stat_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        stat_id=stat_id,
                        count=count,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at,
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def insert_game_stat_for_season_team_id(game_id: int,
                                                      season_id: str,
                                   season_team_id: str,
                                   stat_name: str,
                                   count: int,
                                   min: int,
                                   plus_min: int):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id_by_season_id_season_team_id(season_id, season_team_id)
            
            if await AsyncCore.GameStat.is_game_stat_exist(game_id, team_id, stat_name):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO game_stat (
                                    game_id,
                                    team_id,
                                    stat_id,
                                    count,
                                    min,
                                    plus_min,
                                    created_at
                                    )
                                VALUES (
                                    :game_id,
                                    :team_id,
                                    :stat_id,
                                    :count,
                                    :min,
                                    :plus_min,
                                    :created_at
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    stat_id = await AsyncCore.Stat.insert_stat(stat_name)
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        team_id=team_id,
                        stat_id=stat_id,
                        count=count,
                        min=min,
                        plus_min=plus_min,
                        created_at=created_at,
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
             
             
    class TableToDataFrame:
        
        @staticmethod
        async def get_game_df(game_id: int) -> pd.DataFrame:
            async with async_session_factory() as session:
                query = text('SELECT * FROM game WHERE game_id=:game_id')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                return pd.DataFrame(rows)
        
        @staticmethod
        async def get_referee_game_df(game_id: int) -> pd.DataFrame:        
            async with async_session_factory() as session:
                query = text('SELECT * FROM referee_game WHERE game_id=:game_id')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                return pd.DataFrame(rows)
        
        @staticmethod
        async def get_goal_df(game_id: int) -> pd.DataFrame:       
            async with async_session_factory() as session:
                columns_query = text('SELECT * FROM goal LIMIT 0')
                columns_result = await session.execute(columns_query)
                columns = columns_result.keys()
                
                
                query = text('SELECT * FROM goal WHERE game_id=:game_id')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                # return pd.DataFrame(rows)
                print(f'Колонки для пустого датафрейма голы {columns}')
                if rows:
                    return pd.DataFrame(rows)
                else:
                    return pd.DataFrame(columns=columns)
        
        @staticmethod
        async def get_goal_type_df() -> pd.DataFrame:
            async with async_session_factory() as session:
                query = text('SELECT * FROM goal_type')
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                return pd.DataFrame(rows)
        
        @staticmethod
        async def get_lineup_df(game_id: int) -> pd.DataFrame:       
            async with async_session_factory() as session:
                query = text('SELECT * FROM lineup WHERE game_id=:game_id')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                return pd.DataFrame(rows)
        
        @staticmethod
        async def get_penalty_df(game_id: int) -> pd.DataFrame:        
            async with async_session_factory() as session:
                # Запрос для получения столбцов таблицы penalty
                columns_query = text('SELECT * FROM penalty LIMIT 0')
                columns_result = await session.execute(columns_query)
                columns = columns_result.keys()
                
                
                query = text('SELECT * FROM penalty WHERE game_id=:game_id')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                # return pd.DataFrame(rows)
                if rows:
                    return pd.DataFrame(rows)
                else:
                    return pd.DataFrame(columns=columns)
        
        
        @staticmethod
        async def get_penalty_type_df() -> pd.DataFrame:
            async with async_session_factory() as session:
                query = text('SELECT * FROM penalty_type')
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                # Конвертируем в DataFrame
                return pd.DataFrame(rows)
        
        
        @staticmethod
        async def get_lineup_player_stat_for_game(game_id: int) -> pd.DataFrame:
            '''Сводная таблица о статистике игровок, учавствующих в игре'''
            
            # https://stackoverflow.com/questions/2281551/tsql-left-join-and-only-last-row-from-right
            async with async_session_factory() as session:
                query = text('''SELECT
                                lineup.player_id,
                                player_stat.transfer_value,
                                player_stat.amplua_id,
                                amplua.name
                                FROM game
                                LEFT JOIN lineup ON game.game_id=lineup.game_id
                                LEFT JOIN player_stat 
                                ON lineup.player_id=player_stat.player_id AND 
                                game.season_id=player_stat.season_id AND 
                                player_stat.player_stat_id=(
                                    SELECT MAX(player_stat_id) FROM player_stat WHERE player_stat.player_id=lineup.player_id AND player_stat.season_id=game.season_id
                                )
                                LEFT JOIN amplua ON player_stat.amplua_id=amplua.amplua_id
                                WHERE game.game_id=:game_id
                            ''')
                query = query.bindparams(
                    game_id=game_id
                )
                # Выполняем через async session
                result = await session.execute(query)
                rows = result.mappings().all()
                df = pd.DataFrame(rows)
                df['transfer_value'] = df['transfer_value'].fillna(0)
                unkwn_amplua_name = 'неизвестно'
                unkwn_amplua_id = SyncCore.get_amplua_id_for_name(unkwn_amplua_name)
                df['amplua_id'] = df['amplua_id'].fillna(unkwn_amplua_id)
                df['name'] = df['name'].fillna(unkwn_amplua_name)
                # Конвертируем в DataFrame
                return df
                
             
    class PredictionDrawLeftRight:
        
        @staticmethod
        async def get_game_prediction(game_id: int, sort_type: str) -> GamePredictionDrowLeftRightDto:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM prediction_draw_left_right
                                 WHERE 
                                 game_id=:game_id AND
                                 res_p IS NOT NULL
                                 ''')
                    
                    if await AsyncCore.check_sort_type(sort_type):
                        query = text(str(query) + ' ORDER BY min ' + sort_type + ',')
                        query = text(str(query) + ' plus_min ' + sort_type)
                    
                    params = {'game_id': game_id}
                    
                    res = await session.execute(query, params)
                    predictions_res = res.all()
                    
                    prediction_list = [
                        PredictionDrawLeftRightDto(
                            prediction_id=row.prediction_id,
                            min=row.min,
                            plus_min=row.plus_min, 
                            left_coach_id=row.left_coach_id,
                            right_coach_id=row.right_coach_id,
                            referee_id=row.referee_id,
                            left_num_v=row.left_num_v,
                            left_num_z=row.left_num_z,
                            left_num_p=row.left_num_p,
                            left_num_n=row.left_num_n,
                            left_num_u=row.left_num_u,
                            right_num_v=row.right_num_v,
                            right_num_z=row.right_num_z,
                            right_num_p=row.right_num_p,
                            right_num_n=row.right_num_n,
                            right_num_u=row.right_num_u,
                            left_num_y=row.left_num_y,
                            left_num_y2r=row.left_num_y2r,
                            right_num_y=row.right_num_y, 
                            right_num_y2r=row.right_num_y2r, 
                            right_num_goal_g=row.right_num_goal_g, 
                            right_num_goal_p=row.right_num_goal_p, 
                            right_num_goal_a=row.right_num_goal_a, 
                            left_num_goal_g=row.left_num_goal_g, 
                            left_num_goal_p=row.left_num_goal_p, 
                            left_num_goal_a=row.left_num_goal_a, 
                            left_total_transfer_value=row.left_total_transfer_value, 
                            right_total_transfer_value=row.right_total_transfer_value, 
                            left_avg_transfer_value=row.left_avg_transfer_value, 
                            right_avg_transfer_value=row.right_avg_transfer_value, 
                            left_goal_score=row.left_goal_score, 
                            right_goal_score=row.right_goal_score, 
                            left_avg_time_player_in_game=row.left_avg_time_player_in_game, 
                            right_avg_time_player_in_game=row.right_avg_time_player_in_game, 
                            left_right_transfer_value_div=row.left_right_transfer_value_div, 
                            right_left_transfer_value_div=row.right_left_transfer_value_div, 
                            res_event=row.res_event,
                            draw_p=row.draw_p, 
                            left_p=row.left_p, 
                            right_p=row.right_p, 
                            res_p=row.res_p,
                            res=row.res,
                            created_at=row.created_at,
                            updated_at=row.updated_at
                        )
                        for row in predictions_res
                    ]
                    
                    return GamePredictionDrowLeftRightDto(game_id=game_id, prediction_list=prediction_list)
                except Exception as e:
                    await session.rollback()
                    raise
        
        
        @staticmethod
        async def is_prediction_draw_left_right_exist(game_id: int,
                                                      min: int,
                                                      plus_min: int) -> bool:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT * FROM prediction_draw_left_right
                                 WHERE game_id=:game_id AND min=:min AND plus_min=:plus_min
                                 ''')
                    
                    query = query.bindparams(
                        game_id=game_id,
                        min=min,
                        plus_min=plus_min,
                    )
                    res = await session.execute(query)
                    is_exist = False if res.one_or_none() is None else True
                    return is_exist
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def get_unpredicted_prediction_id(game_id: int) -> list[int]:
            async with async_session_factory() as session:
                try:
                    query = text('''
                                 SELECT prediction_id FROM prediction_draw_left_right
                                 WHERE game_id=:game_id AND res_p IS NULL
                                 ''')
                    query = query.bindparams(
                        game_id=game_id
                    )
                    res = await session.execute(query)
                    return res.scalars().all()
                except Exception as e:
                    await session.rollback()
                    raise
             
        @staticmethod
        async def get_attributes_prediction(prediction_id: int):
            async with async_session_factory() as session:
                try:
                    query = text('''
                                SELECT 
                                left_coach_id,
                                right_coach_id,
                                referee_id,
                                left_num_v,
                                left_num_z,
                                left_num_p,
                                left_num_n,
                                left_num_u,
                                right_num_v,
                                right_num_z,
                                right_num_p,
                                right_num_n,
                                right_num_u,
                                left_num_y,
                                left_num_y2r,
                                right_num_y,
                                right_num_y2r,
                                right_num_goal_g,
                                right_num_goal_p,
                                right_num_goal_a,
                                left_num_goal_g,
                                left_num_goal_p,
                                left_num_goal_a,
                                left_total_transfer_value,
                                right_total_transfer_value,
                                left_avg_transfer_value,
                                right_avg_transfer_value,
                                left_goal_score,
                                right_goal_score,
                                left_avg_time_player_in_game,
                                right_avg_time_player_in_game,
                                left_right_transfer_value_div,
                                right_left_transfer_value_div,
                                res_event
                                FROM prediction_draw_left_right
                                WHERE prediction_id=:prediction_id
                                 ''')
                    query = query.bindparams(
                        prediction_id=prediction_id
                    )
                    res = await session.execute(query)
                    return res.all()[0]
                except Exception as e:
                    await session.rollback()
                    raise
                
        @staticmethod
        async def get_attributes_train(game_id: int):
            async with async_session_factory() as session:
                try:
                    query = text('''
                                SELECT 
                                left_coach_id,
                                right_coach_id,
                                referee_id,
                                left_num_v,
                                left_num_z,
                                left_num_p,
                                left_num_n,
                                left_num_u,
                                right_num_v,
                                right_num_z,
                                right_num_p,
                                right_num_n,
                                right_num_u,
                                left_num_y,
                                left_num_y2r,
                                right_num_y,
                                right_num_y2r,
                                right_num_goal_g,
                                right_num_goal_p,
                                right_num_goal_a,
                                left_num_goal_g,
                                left_num_goal_p,
                                left_num_goal_a,
                                left_total_transfer_value,
                                right_total_transfer_value,
                                left_avg_transfer_value,
                                right_avg_transfer_value,
                                left_goal_score,
                                right_goal_score,
                                left_avg_time_player_in_game,
                                right_avg_time_player_in_game,
                                left_right_transfer_value_div,
                                right_left_transfer_value_div,
                                res_event,
                                res
                                FROM prediction_draw_left_right
                                WHERE game_id=:game_id
                                 ''')
                    query = query.bindparams(
                        game_id=game_id
                    )
                    res = await session.execute(query)
                    return res.all()
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def update_prediction(prediction_id: int, 
                                    draw_p: float,
                                    left_p: float,
                                    right_p: float,
                                    res_p: int):
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                UPDATE prediction_draw_left_right
                                SET updated_at=:updated_at, draw_p=:draw_p, left_p=:left_p, right_p=:right_p, res_p=:res_p
                                WHERE prediction_id=:prediction_id
                                ''')
                    
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        updated_at=updated_at,
                        draw_p=draw_p,
                        left_p=left_p,
                        right_p=right_p,
                        res_p=res_p,
                        prediction_id=prediction_id
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def set_res(game_id: int):
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                WITH max_scores AS (
                                    SELECT
                                    MAX(left_goal_score) AS max_left_goal_score,
                                    MAX(right_goal_score) AS max_right_goal_score
                                    FROM prediction_draw_left_right
                                    WHERE game_id=:game_id
                                )
                                UPDATE prediction_draw_left_right
                                SET
                                updated_at=:updated_at,
                                res = CASE
                                WHEN (SELECT max_left_goal_score FROM max_scores) = (SELECT max_right_goal_score FROM max_scores) THEN 0
                                WHEN (SELECT max_left_goal_score FROM max_scores) > (SELECT max_right_goal_score FROM max_scores) THEN 1
                                WHEN (SELECT max_left_goal_score FROM max_scores) < (SELECT max_right_goal_score FROM max_scores) THEN 2
                                END
                                WHERE game_id=:game_id
                                ''')
                    
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        updated_at=updated_at
                    )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
        
        @staticmethod
        async def insert_prediction_draw_left_right(game_id: int,
                                                    min: int,
                                                    plus_min: int,
                                                    left_coach_id: int,
                                                    right_coach_id: int,
                                                    referee_id: int,
                                                    left_num_v: int,
                                                    left_num_z: int,
                                                    left_num_p: int,
                                                    left_num_n: int,
                                                    left_num_u: int,
                                                    right_num_v: int,
                                                    right_num_z: int,
                                                    right_num_p: int,
                                                    right_num_n: int,
                                                    right_num_u: int,
                                                    left_num_y: int,
                                                    left_num_y2r: int,
                                                    right_num_y: int,
                                                    right_num_y2r: int,
                                                    right_num_goal_g: int,
                                                    right_num_goal_p: int,
                                                    right_num_goal_a: int,
                                                    left_num_goal_g: int,
                                                    left_num_goal_p: int,
                                                    left_num_goal_a: int,
                                                    left_total_transfer_value: float,
                                                    right_total_transfer_value: float,
                                                    left_avg_transfer_value: float,
                                                    right_avg_transfer_value: float,
                                                    left_goal_score: int,
                                                    right_goal_score: int,
                                                    left_avg_time_player_in_game: float,
                                                    right_avg_time_player_in_game: float,
                                                    left_right_transfer_value_div: float,
                                                    right_left_transfer_value_div: float,
                                                    res_event: int,
                                                    draw_p: float = None,
                                                    left_p: float = None,
                                                    right_p: float = None,
                                                    res_p: int = None,
                                                    res: int = None):
                        
            if await AsyncCore.PredictionDrawLeftRight.is_prediction_draw_left_right_exist(game_id, min, plus_min):
                return
                     
            async with async_session_factory() as session:
                try:
                    stmt = text('''
                                INSERT INTO prediction_draw_left_right (
                                    game_id,
                                    min,
                                    plus_min,
                                    left_coach_id,
                                    right_coach_id,
                                    referee_id,
                                    left_num_v,
                                    left_num_z,
                                    left_num_p,
                                    left_num_n,
                                    left_num_u,
                                    right_num_v,
                                    right_num_z,
                                    right_num_p,
                                    right_num_n,
                                    right_num_u,
                                    left_num_y,
                                    left_num_y2r,
                                    right_num_y,
                                    right_num_y2r,
                                    right_num_goal_g,
                                    right_num_goal_p,
                                    right_num_goal_a,
                                    left_num_goal_g,
                                    left_num_goal_p,
                                    left_num_goal_a,
                                    left_total_transfer_value,
                                    right_total_transfer_value,
                                    left_avg_transfer_value,
                                    right_avg_transfer_value,
                                    left_goal_score,
                                    right_goal_score,
                                    left_avg_time_player_in_game,
                                    right_avg_time_player_in_game,
                                    left_right_transfer_value_div,
                                    right_left_transfer_value_div,
                                    res_event,
                                    created_at,
                                    updated_at,
                                    draw_p,
                                    left_p,
                                    right_p,
                                    res_p,
                                    res
                                    )
                                VALUES (
                                    :game_id,
                                    :min,
                                    :plus_min,
                                    :left_coach_id,
                                    :right_coach_id,
                                    :referee_id,
                                    :left_num_v,
                                    :left_num_z,
                                    :left_num_p,
                                    :left_num_n,
                                    :left_num_u,
                                    :right_num_v,
                                    :right_num_z,
                                    :right_num_p,
                                    :right_num_n,
                                    :right_num_u,
                                    :left_num_y,
                                    :left_num_y2r,
                                    :right_num_y,
                                    :right_num_y2r,
                                    :right_num_goal_g,
                                    :right_num_goal_p,
                                    :right_num_goal_a,
                                    :left_num_goal_g,
                                    :left_num_goal_p,
                                    :left_num_goal_a,
                                    :left_total_transfer_value,
                                    :right_total_transfer_value,
                                    :left_avg_transfer_value,
                                    :right_avg_transfer_value,
                                    :left_goal_score,
                                    :right_goal_score,
                                    :left_avg_time_player_in_game,
                                    :right_avg_time_player_in_game,
                                    :left_right_transfer_value_div,
                                    :right_left_transfer_value_div,
                                    :res_event,
                                    :created_at,
                                    :updated_at,
                                    :draw_p,
                                    :left_p,
                                    :right_p,
                                    :res_p,
                                    :res
                                    )''')
                    
                    created_at = await AsyncCore.get_moscow_datetime_now()
                    updated_at = await AsyncCore.get_moscow_datetime_now()
                    
                    stmt = stmt.bindparams(
                        game_id=game_id,
                        min=min,
                        plus_min=plus_min,
                        left_coach_id=left_coach_id,
                        right_coach_id=right_coach_id,
                        referee_id=referee_id,
                        left_num_v=left_num_v,
                        left_num_z=left_num_z,
                        left_num_p=left_num_p,
                        left_num_n=left_num_n,
                        left_num_u=left_num_u,
                        right_num_v=right_num_v,
                        right_num_z=right_num_z,
                        right_num_p=right_num_p,
                        right_num_n=right_num_n,
                        right_num_u=right_num_u,
                        left_num_y=left_num_y,
                        left_num_y2r=left_num_y2r,
                        right_num_y=right_num_y,
                        right_num_y2r=right_num_y2r,
                        right_num_goal_g=right_num_goal_g,
                        right_num_goal_p=right_num_goal_p,
                        right_num_goal_a=right_num_goal_a,
                        left_num_goal_g=left_num_goal_g,
                        left_num_goal_p=left_num_goal_p,
                        left_num_goal_a=left_num_goal_a,
                        left_total_transfer_value=left_total_transfer_value,
                        right_total_transfer_value=right_total_transfer_value,
                        left_avg_transfer_value=left_avg_transfer_value,
                        right_avg_transfer_value=right_avg_transfer_value,
                        left_goal_score=left_goal_score,
                        right_goal_score=right_goal_score,
                        left_avg_time_player_in_game=left_avg_time_player_in_game,
                        right_avg_time_player_in_game=right_avg_time_player_in_game,
                        left_right_transfer_value_div=left_right_transfer_value_div,
                        right_left_transfer_value_div=right_left_transfer_value_div,
                        res_event=res_event,
                        created_at=created_at,
                        updated_at=updated_at,
                        draw_p=draw_p,
                        left_p=left_p,
                        right_p=right_p,
                        res_p=res_p,
                        res=res
                        )
                    await session.execute(stmt)
                    await session.commit()
                except IntegrityError as e:
                    await session.rollback() # откатываем транзакцию
                    raise
                except Exception as e:
                    await session.rollback()
                    raise
             