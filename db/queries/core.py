from datetime import datetime, time, date, timezone, timedelta

from sqlalchemy import Integer, and_, func, insert, select, text, update
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError

import pandas as pd
from ..database import async_session_factory, sync_session_factory


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
        
        with sync_session_factory() as session:
            query = text('''SELECT
                            lineup.player_id,
                            player_stat.transfer_value,
                            player_stat.amplua_id,
                            amplua.name
                            FROM game
                            LEFT JOIN lineup ON game.game_id=lineup.game_id
                            LEFT JOIN player_stat ON lineup.player_id=player_stat.player_id AND game.season_id=player_stat.season_id
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
    
    @staticmethod
    async def get_moscow_datetime_now() -> datetime:
        utc_datetime_now = datetime.now(timezone.utc)
        moscow_datetime_now = utc_datetime_now + timedelta(hours=3) # МСК (UTC+3)
        return moscow_datetime_now.replace(tzinfo=None) # убираем часовой пояс
    
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
                                       season_id: str):
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
        async def get_team_id(season_id: str,
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
        async def get_season_team_id(season_id: str,
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
                                                        is_active: bool = True):
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
        
        UNDEFINED_GAME_STATUS_ID = 4
        GAME_STATUS_DICT = {
            0: 'не начался',
            1: 'окончен',
            2: 'перерыв',
            3: 'игра',
            UNDEFINED_GAME_STATUS_ID: 'не определен',
        }
        
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
                name = AsyncCore.GameStatus.GAME_STATUS_DICT[game_status_id]
            except KeyError:
                game_status_id = AsyncCore.GameStatus.UNDEFINED_GAME_STATUS_ID
                name = AsyncCore.GameStatus.GAME_STATUS_DICT[game_status_id]
            
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
            if game_id is not None:
                return game_id
            
            left_team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, left_season_team_id)
            right_team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, right_season_team_id)
            
            if not await AsyncCore.GameStatus.is_game_status_id_exist(game_status_id):
                game_status_id = await AsyncCore.GameStatus.insert_game_status(game_status_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
            
            team_id = await AsyncCore.SeasonTeam.get_team_id(season_id, season_team_id)
            
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
             