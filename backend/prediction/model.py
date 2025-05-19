import sys
import os
# Получаем абсолютный путь к директории текущего скрипта (collection/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на уровень выше (в корень проекта)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# Добавляем корень проекта в пути поиска модулей
sys.path.append(project_root)


import numpy as np
from catboost import CatBoostClassifier, Pool
import asyncio

class ModelDrawLeftRight:
    
    def __init__(self, model_path: str = os.path.join(current_dir, 'model_cbc_without_goals')):
        self.model_params = {
            'iterations':1000,
            'early_stopping_rounds':50,
            'depth':8,
            'loss_function':'MultiClass',
            'eval_metric':'TotalF1',
            'use_best_model':True,
        }
        self.model_path = model_path
        self.model = self._load_model()

    def _load_model(self):
        """Загрузка модели из файла"""
        model = CatBoostClassifier() # **self.model_params - дообучение не реализовано пока не нужны
        model.load_model(self.model_path)
        return model
    
    async def predict(self,
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
                    res_event: int):
        data = [
            #left_coach_id,
            #right_coach_id,
            #referee_id,
            #left_num_v,
            left_num_z,
            left_num_p,
            left_num_n,
            #left_num_u,
            #right_num_v,
            right_num_z,
            right_num_p,
            right_num_n,
            #right_num_u,
            left_num_y,
            left_num_y2r,
            right_num_y,
            right_num_y2r,
            #right_num_goal_g,
            #right_num_goal_p,
            #right_num_goal_a,
            #left_num_goal_g,
            #left_num_goal_p,
            #left_num_goal_a,
            left_total_transfer_value,
            right_total_transfer_value,
            left_avg_transfer_value,
            right_avg_transfer_value,
            #left_goal_score,
            #right_goal_score,
            left_avg_time_player_in_game,
            right_avg_time_player_in_game,
            left_right_transfer_value_div,
            right_left_transfer_value_div,
            #res_event
        ]
        predict_proba = self.model.predict_proba(data)
        draw_p, left_p, right_p = predict_proba[0], predict_proba[1], predict_proba[2]
        res_p = self.model.predict(data)
        print(draw_p, left_p, right_p, res_p[0])
        return float(draw_p), float(left_p), float(right_p), int(res_p[0])
    
    async def train(self,
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
                    res: int):
        
        train_data = [
            [
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
            ]
        ]
        target = [[res]]

        # Асинхронное выполнение обучения
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, 
            self._train_model, 
            train_data, 
            target
        )


    def _validate_data(self, X, y):
        if len(X) == 0 or len(y) == 0:
            raise ValueError("Empty training data")
        if np.isnan(X).any() or np.isnan(y).any():
            raise ValueError("NaN values in data")


    def _train_model(self, X, y):
        """Синхронное выполнение дообучения модели"""
        try:
            train_pool = Pool(
                data=X,
                label=y
            )
            
            self._validate_data(X, y)

            print('model X', X)
            print('model y', y)
            
            # Продолжаем обучение существующей модели
            self.model.fit(
                train_pool,
                eval_set=(X, y),
                init_model=self.model,  # Используем текущую модель как базовую
                use_best_model=False,            # Отключаем вывод логов
            )
            # Анализ результатов
            print("Feature importances:", self.model.get_feature_importance())
            print("Feature names:", self.model.feature_names_)
            
            # Сохраняем обновленную модель
            self.model.save_model(self.model_path+'123')
                        
            
        except Exception as e:
            print(f"Ошибка при дообучении модели: {str(e)}")
            # Восстанавливаем исходную модель в случае ошибки
            self._load_model()
