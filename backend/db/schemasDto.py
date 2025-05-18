from pydantic import BaseModel
from datetime import datetime, time, date, timezone, timedelta


class SeasonDto(BaseModel):
    season_id: str
    
    
class SeasonAddDto(SeasonDto):
    start_date: date
    end_date: date
    season_url: str
    

class SortSeasonGameDto(SeasonDto):
    sort_type: str = 'ASC'
    game_status: list[int] = []
    left_team_id: str = None
    right_team_id: str = None
    from_start_date: date = None
    to_start_date: date = None
    
    
class GameDto(BaseModel):
    game_id: int
    

class GameAddDto(GameDto):
    season_game_id: str
    season_id: str
    left_team_id: str
    right_team_id: str
    game_status_id: int
    left_coach_id: str | None
    right_coach_id: str | None
    tour_number: int
    start_date: date
    start_time: time
    min: int | None
    plus_min: int | None
    created_at: datetime
    updated_at: datetime
    game_url: str
    
    
class SeasonTeamDto(BaseModel):
    season_id: str
    team_id: str
    season_team_id: str
    
    
class SeasonTeamAddDto(SeasonTeamDto):
    name: str
    season_team_url: str
    

class PredictionDrawLeftRightDto(BaseModel):
    prediction_id: int
    min: int
    plus_min: int 
    left_coach_id: int
    right_coach_id: int
    referee_id: int
    left_num_v: int
    left_num_z: int
    left_num_p: int
    left_num_n: int
    left_num_u: int
    right_num_v: int
    right_num_z: int
    right_num_p: int
    right_num_n: int
    right_num_u: int
    left_num_y: int
    left_num_y2r: int
    right_num_y:int 
    right_num_y2r:int 
    right_num_goal_g: int 
    right_num_goal_p: int 
    right_num_goal_a: int 
    left_num_goal_g: int 
    left_num_goal_p: int 
    left_num_goal_a: int 
    left_total_transfer_value: float 
    right_total_transfer_value: float 
    left_avg_transfer_value: float 
    right_avg_transfer_value: float 
    left_goal_score: int 
    right_goal_score: int 
    left_avg_time_player_in_game: float 
    right_avg_time_player_in_game: float 
    left_right_transfer_value_div: float 
    right_left_transfer_value_div: float 
    res_event: int
    draw_p:float | None
    left_p:float | None
    right_p:float | None
    res_p: float | None
    res: int | None
    created_at: datetime
    updated_at: datetime

    
class GamePredictionDrowLeftRightDto(GameDto):
    
    prediction_list: list[PredictionDrawLeftRightDto]
