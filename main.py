from datetime import datetime, timezone, date
import time
    
from collection.pages import SeasonPage, CalendarPage, GamePage
from collection.browser import BrowserConnection

with BrowserConnection() as br: 
    mp = SeasonPage(br)
    seasons = mp.get_season_list_options()
    mp.go_to_season(seasons[0])
    res = mp.get_info()
    
    datatime_now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
    with open(f'collection/runs_collection/{datatime_now}.txt', 'w') as text_file:
        text_file.write(str(res))
    
    # cp = CalendarPage(br, 'https://www.championat.com/football/_russiapl/tournament/5980/calendar/')
    # cp.get_calendar_info()
    
    # gp = GamePage(br, 'https://www.championat.com/football/_england/tournament/6118/match/1190746/#stats')
    # game = gp.get_game_info()
    # print(game.right_team_goals[0].player_id.id, game.right_team_goals[0].player_sub_id.id, game.right_team_goals[0].min)
    # print(game.game_stats[0].stat_name, game.game_stats[0].left_team_stat)