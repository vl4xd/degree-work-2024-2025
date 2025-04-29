from typing import Annotated
from sqlalchemy import ForeignKey, Index, UniqueConstraint, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeMeta
from datetime import date, time, datetime

from .database import Base, int_3, str_100, str_200

intpk_a = Annotated[int, mapped_column(primary_key=True, autoincrement=True)]
intpk_m = Annotated[int, mapped_column(primary_key=True, autoincrement=False)]
strpk_m = Annotated[str, mapped_column(primary_key=True, autoincrement=False)]


class CoachOrm(Base):
    __tablename__ = 'coach'
    
    coach_id: Mapped[strpk_m] 
    first_name: Mapped[str_100 | None]
    middle_name: Mapped[str_100 | None]
    last_name: Mapped[str_100 | None]
    birth_date: Mapped[date | None]

    team_coach: Mapped[list['TeamCoachOrm']] = relationship(
        back_populates='coach'
    )


class PlayerOrm(Base):
    __tablename__ = 'player'
    
    player_id: Mapped[strpk_m]
    first_name: Mapped[str_100 | None]
    last_name: Mapped[str_100 | None]
    birth_date: Mapped[date | None]
    
    
class AmpluaOrm(Base):
    __tablename__ = 'amplua'
    
    amplua_id: Mapped[intpk_a]
    name: Mapped[str_100]
    
    
class SeasonOrm(Base):
    __tablename__ = 'season'
    
    season_id: Mapped[strpk_m]
    start_date: Mapped[date | None]
    end_date: Mapped[date | None]
    

class PlayerStatOrm(Base):
    __tablename__ = 'player_stat'
    
    player_stat_id: Mapped[intpk_a]
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE')
    )
    amplua_id: Mapped[int] = mapped_column(
        ForeignKey('amplua.amplua_id', ondelete='CASCADE')
    )
    season_id: Mapped[str] = mapped_column(
        ForeignKey('season.season_id', ondelete='CASCADE')
    )
    number: Mapped[int_3 | None]
    growth: Mapped[int_3 | None]
    weight: Mapped[int_3 | None]
    transfer_value: Mapped[int | None]
    created_at: Mapped[datetime]


class TeamOrm(Base):
    __tablename__ = 'team'
    
    team_id: Mapped[strpk_m]
    name: Mapped[str_200]
    
    
class SeasonTeamOrm(Base):
    __tablename__ = 'season_team'
    
    season_id: Mapped[str] = mapped_column(
        ForeignKey('season.season_id', ondelete='CASCADE'),
        primary_key=True
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    season_team_id: Mapped[str]
  
   
class TeamPlayerOrm(Base):
    __tablename__ = 'team_player'
    
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    season_id: Mapped[str] = mapped_column(
        ForeignKey('season.season_id', ondelete='CASCADE'),
        primary_key=True
    )
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE'),
        primary_key=True
    )
    is_active: Mapped[bool]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]


class TeamCoachOrm(Base):
    __tablename__ = 'team_coach'
    
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    season_id: Mapped[str] = mapped_column(
        ForeignKey('season.season_id', ondelete='CASCADE'),
        primary_key=True
    )
    coach_id: Mapped[str] = mapped_column(
        ForeignKey('coach.coach_id', ondelete='CASCADE'),
        primary_key=True
    )
    is_active: Mapped[bool]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    
    
    coach: Mapped['CoachOrm'] = relationship(
        back_populates='team_coach'
    )
    
    
class GameStatusOrm(Base):
    __tablename__ = 'game_status'
    
    game_status_id: Mapped[intpk_m]
    name: Mapped[str_100]


class GameOrm(Base):
    __tablename__ = 'game'
    
    game_id: Mapped[intpk_a]
    season_game_id: Mapped[str]
    season_id: Mapped[str] = mapped_column(
        ForeignKey('season.season_id', ondelete='CASCADE')
    )
    left_team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE')
    )
    right_team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE')
    )
    game_status_id: Mapped[int] = mapped_column(
        ForeignKey('game_status.game_status_id', ondelete='CASCADE')
    )
    left_coach_id: Mapped[str | None] = mapped_column(
        ForeignKey('coach.coach_id', ondelete='CASCADE')
    )
    right_coach_id: Mapped[str | None] = mapped_column(
        ForeignKey('coach.coach_id', ondelete='CASCADE')
    )
    tour_number: Mapped[int_3 | None]
    start_date: Mapped[date | None]
    start_time: Mapped[time | None]
    min: Mapped[int_3 | None]
    plus_min: Mapped[int_3 | None]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    
    __table_args__ = (
        UniqueConstraint('season_game_id', 'season_id', name='unique_season_game_id_season_id'),
    )
    
    
class GoalTypeOrm(Base):
    __tablename__ = 'goal_type'
    
    goal_type_id: Mapped[intpk_a]
    name: Mapped[str_100]
    
    
class GoalOrm(Base):
    __tablename__ = 'goal'
    
    goal_id: Mapped[intpk_a]
    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE')
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE')
    )
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE')
    )
    player_sub_id: Mapped[str | None] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE')
    )
    goal_type_id: Mapped[int] = mapped_column(
        ForeignKey('goal_type.goal_type_id', ondelete='CASCADE')
    )
    min: Mapped[int_3 | None]
    plus_min: Mapped[int_3 | None]
    created_at: Mapped[datetime]
    

class RefereeOrm(Base):
    __tablename__ = 'referee'
    
    referee_id: Mapped[strpk_m]
    first_name: Mapped[str_100 | None]
    last_name: Mapped[str_100 | None] 


class RefereeGameOrm(Base):
    __tablename__ = 'referee_game'
    
    referee_id: Mapped[str] = mapped_column(
        ForeignKey('referee.referee_id', ondelete='CASCADE'),
        primary_key=True
    )
    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE'),
        primary_key=True
    )      


class PenaltyTypeOrm(Base):
    __tablename__ = 'penalty_type'
    
    penalty_type_id: Mapped[intpk_a]
    name: Mapped[str_100]
    
    
class PenaltyOrm(Base):
    __tablename__ = 'penalty'
    
    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE'),
        primary_key=True
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE'),
        primary_key=True
    )
    penalty_type_id: Mapped[int] = mapped_column(
        ForeignKey('penalty_type.penalty_type_id', ondelete='CASCADE'),
        primary_key=True
    )
    min: Mapped[int_3 | None]
    plus_min: Mapped[int_3 | None]
    created_at: Mapped[datetime]


class LineupOrm(Base):
    __tablename__ = 'lineup'

    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE'),
        primary_key=True
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE'),
        primary_key=True
    )
    min_in: Mapped[int_3 | None]
    plus_min_in: Mapped[int_3 | None]
    min_out: Mapped[int_3 | None]
    plus_min_out: Mapped[int_3 | None]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    

class SaveOrm(Base):
    __tablename__ = 'save'
    
    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE'),
        primary_key=True
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
        primary_key=True
    )
    player_id: Mapped[str] = mapped_column(
        ForeignKey('player.player_id', ondelete='CASCADE'),
        primary_key=True
    )
    count: Mapped[int_3]
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    
    
class StatOrm(Base):
    __tablename__ = 'stat'
    
    stat_id: Mapped[intpk_a]
    name: Mapped[str_200]
    
    
class GameStatOrm(Base):
    __tablename__ = 'game_stat'
    
    game_stat_id: Mapped[intpk_a]
    game_id: Mapped[int] = mapped_column(
        ForeignKey('game.game_id', ondelete='CASCADE'),
    )
    team_id: Mapped[str] = mapped_column(
        ForeignKey('team.team_id', ondelete='CASCADE'),
    )
    stat_id: Mapped[int] = mapped_column(
        ForeignKey('stat.stat_id', ondelete='CASCADE'),
    )
    count: Mapped[int_3]
    min: Mapped[int_3 | None]
    plus_min: Mapped[int_3 | None]
    created_at: Mapped[datetime]
