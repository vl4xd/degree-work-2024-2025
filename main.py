from datetime import datetime, timezone, date
import time
    
from collection.pages import SeasonPage, CalendarPage, GamePage
from collection.browser import BrowserConnection


with BrowserConnection() as br: 
    # mp = SeasonPage(br)
    # seasons = mp.get_season_list_options()
    # mp.go_to_season(seasons[1])
    # res = mp.get_season_info()
    
    # datatime_now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
    # with open(f'collection/runs_collection/{datatime_now}.txt', 'w') as text_file:
    #     text_file.write(str(res))
    
    # cp = CalendarPage(br, 'https://www.championat.com/football/_russiapl/tournament/5980/calendar/')
    # cp.get_calendar_info()
    
    gp = GamePage(br, 'https://www.championat.com/football/_russiapl/tournament/5980/match/1175858/#stats')
    gp.get_game_info()