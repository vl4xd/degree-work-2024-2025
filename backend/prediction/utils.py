import sys
import os
# Получаем абсолютный путь к директории текущего скрипта (collection/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на уровень выше (в корень проекта)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# Добавляем корень проекта в пути поиска модулей
sys.path.append(project_root)

import asyncio

from db.queries.core import AsyncCore as AC
from prediction.model import ModelDrawLeftRight #from model import ModelDrawLeftRight


async def check_predict_game_in_db():
    # Индетификатор текущего сезона
    current_season_id: str = await AC.Season.get_current_season_id()
    # Лист идентификторов прошедших (необработанных) и текущих игры
    predict_game_id: list[int] = await AC.Game.get_active_season_game_id_for_prediction(season_id=current_season_id)
    return predict_game_id


async def predict_game(game_id: int):
    prediction_id_list = await AC.PredictionDrawLeftRight.get_unpredicted_prediction_id(game_id=game_id)
    print(f'get_unpredicted_prediction_id {prediction_id_list}')
    for prediction_id in prediction_id_list:
        print(f'{prediction_id=}')
        model_left_draw_right = ModelDrawLeftRight()
        attributes = await AC.PredictionDrawLeftRight.get_attributes_prediction(prediction_id=prediction_id)
        draw_p, left_p, right_p, res_p = await model_left_draw_right.predict(*attributes)
        await AC.PredictionDrawLeftRight.update_prediction(prediction_id=prediction_id,
                                                           draw_p=draw_p,
                                                           left_p=left_p,
                                                           right_p=right_p,
                                                           res_p=res_p)        
    
async def train_model(game_id: int):
    
    attributes_list = await AC.PredictionDrawLeftRight.get_attributes_train(game_id=game_id)
    for attributes in attributes_list:
        model_left_draw_right = ModelDrawLeftRight()
        print(*attributes, 'Атрибуты для обучения')
        await model_left_draw_right.train(*attributes)

async def insert_predict_game_into_db(game_id: int):
    
    game_status_id = await AC.Game.get_game_status_id_by_game_id(game_id=game_id)
    game_statud_id_played = 1
    game_status_id_played_not_predicted = 5
    
    if AC.GAME_STATUS_DICT[game_statud_id_played] != 'окончен': raise Exception('Идентификатор оконченного матча был изменен')
    if AC.GAME_STATUS_DICT[game_status_id_played_not_predicted] != 'окончен, не спрогнозирован': raise Exception('Идентификатор не спрогнозированного матча был изменен')
    
    await predict_game(game_id=game_id)
    
    if game_status_id == game_status_id_played_not_predicted:
        await AC.PredictionDrawLeftRight.set_res(game_id=game_id) # Расчитываем результат игры по всем событиям
        # await train_model(game_id=game_id)
        await AC.Game.set_game_status_id_played_by_game_id(game_id=game_id) # Обновляем статус игры как "окончена (проанализирована)"

async def manage_predict_game():
    predict_game_id = await check_predict_game_in_db()    
    tasks = []
    print(f'Выявленные игры для прогноза manage_predict_game: {predict_game_id}')
    for game_id in predict_game_id:
        task = asyncio.create_task(insert_predict_game_into_db(game_id=game_id))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res


# asyncio.run(manage_predict_game())