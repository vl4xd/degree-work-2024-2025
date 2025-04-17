from datetime import datetime, timezone, date
import time
    
from collection.pages import MainPage, TeamPage
from collection.browser import BrowserConnection


with BrowserConnection() as br: 
    mp = MainPage(br)
    res = mp.get_season_info()
    
    datatime_now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
    with open(f'runs_collection/{datatime_now}.txt') as text_file:
        text_file.write(res)