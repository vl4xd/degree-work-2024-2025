import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from enum import Enum

from db.queries.core import AsyncCore as AC
from db.schemasDto import * # noqa

app = FastAPI()


origins = [
    'http://localhost:5173',
    'http://127.0.0.1:5173'
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)


class SortName(str, Enum):
    asc = 'ASC'
    desc = 'DESC'


@app.get('/seasons', response_class=JSONResponse, summary='Получение списка всех сезонов РПЛ', tags=['Сезоны'])
async def get_seasons():
    return await AC.Season.get_season_list()


@app.get('/season/games', response_class=JSONResponse, summary='Получение списка матчей', tags=['Матчи'])
async def get_season_games(
    sort_type: SortName,
    season_id: str = None,
    game_statuses: List[int] = Query([]),
    left_team_id: Optional[str] = None,
    right_team_id: Optional[str] = None,
    from_start_date: Optional[date] = None,
    to_start_date: Optional[date] = None,
    limit: Optional[int] = 5,
    offset: Optional[int] = 0,
):
    return await AC.Game.get_game_list(season_id,
                                        sort_type,
                                        game_statuses,
                                        left_team_id,
                                        right_team_id,
                                        from_start_date,
                                        to_start_date,
                                        limit,
                                        offset)
    
    
@app.get('/season/teams', response_class=JSONResponse, summary='Получение списка команд', tags=['Команды'])
async def get_teams(season_id: str = None):
    return await AC.SeasonTeam.get_season_team_list(season_id)


@app.get('/season/game/prediction', summary='Получение списка предсказаний игры', tags=['Предсказания'])
async def get_prediction(game_id: int, sort_type: SortName):
    return await AC.PredictionDrawLeftRight.get_game_prediction(game_id, sort_type)


if __name__=='__main__':
    uvicorn.run('main:app', reload=True)