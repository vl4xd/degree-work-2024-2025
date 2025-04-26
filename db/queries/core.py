from datetime import datetime, time, date, timezone, timedelta

from sqlalchemy import Integer, and_, func, insert, select, text, update
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError

from ..database import async_session_factory


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
            first_name: str | None,
            last_name: str | None,
            birth_date: date | None
        ):
            if await AsyncCore.Player.is_player_exist(player_id):
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
            first_name: str | None,
            middle_name: str | None,
            last_name: str | None,
            birth_date: date | None):
            
            if await AsyncCore.Coach.is_coach_exist(coach_id):
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
        
        @staticmethod
        async def is_amplua_name_exist(name: str) -> int | None:
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
        async def is_team_id_exist(team_id: str):
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
                                             team_id: str):
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
                    print(team_id)
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
                    print(season_team_id)
                    return season_team_id
                except Exception as e:
                    await session.rollback()
                    raise
                
    class TeamPlayer:
        
        @staticmethod
        async def is_team_id_season_id_player_id_exist(team_id: str,
                                                       season_id: str,
                                                       player_id: str):
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
        async def insert_team_player(team_id: str,
                                     season_id: str,
                                     player_id: str,
                                     is_active: bool = True):
            if await AsyncCore.TeamPlayer.is_team_id_season_id_player_id_exist(season_id, team_id, player_id):
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
        pass