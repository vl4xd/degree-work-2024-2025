import pickle
import unittest


from datetime import date
from collection.pages import *
from collection.browser import BrowserConnection

class TestCollectionPage(unittest.TestCase):
    
    def test_season(self):
        season_2023_2024 = './collection/filled_schemas/season_2023_2024.pkl'
        with open(season_2023_2024, 'rb') as f:
            trust_season = pickle.load(f)
            
        season_2023_2024_link = 'https://www.championat.com/football/_russiapl/tournament/5441/#stats'
        with BrowserConnection() as br:
            season = SeasonPage(br, season_2023_2024_link)
            test_season = season.get_info()
            
        self.assertEqual(trust_season, test_season)