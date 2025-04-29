import pickle
import os

from collection.pages import *
from collection.browser import BrowserConnection

from db.queries.core import AsyncCore as AC


# datatime_now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
start_season_indx = 0
end_season_indx = 9

season_for_search = []
with BrowserConnection() as br: 
    mp = SeasonPage(br)
    seasons = mp.get_season_list_options()
    season_for_search = seasons[start_season_indx:end_season_indx]
   
while len(season_for_search) > 0 or count_exept > 50:
    count_exept = 0
    season = season_for_search.pop(0)
    try:
        print(season)
        with BrowserConnection() as br:
            mp = SeasonPage(br)  
            mp.go_to_season(season)
            res = mp.get_info()
            season_name_for_file = season.replace('/', '_')
            # Сохранение в файл
            with open(f"collection/filled_schemas/season_{season_name_for_file}.pkl", "wb") as f:
                pickle.dump(res, f)
    except Exception as e:
        print(f'{e=}')
        count_exept += 1
        season_for_search.append(season)
        

