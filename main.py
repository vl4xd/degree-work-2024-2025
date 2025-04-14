from datetime import datetime, timezone, date
import time
    
from collection.pages import TransformDate, MainPage, TournirTablePage
from collection.browser import BrowserConnection

    
start_year = date(2019, 1, 1)
end_year = date(2020, 1, 1)


with BrowserConnection() as br:
    mp = MainPage(br)
    mp.go_to_page()
    mp.go_to_season(TransformDate.to_year_option_value(start_year, end_year))
    print(mp.get_season_list())
    print(mp.get_season_date())
    ttp = TournirTablePage(br, mp.page_href)
    print(ttp.page_href)
    ttp.go_to_page()
    time.sleep(2)
    # tmp.go_to_season(start_year, end_year)