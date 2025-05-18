from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By


class MainPageLocators(object):
    
    LINK_REFRESH = (By.XPATH, "//div[@class='entity-header__title-name']/a")
    
    YEAR_SELECT = (By.NAME, "year")
    TOURNIR_SELECT = (By.NAME, "tournir_id")
    
    YEAR_TOURNIR_OPTION = (By.TAG_NAME, "option")
    DATE_CSS = (By.CSS_SELECTOR, ".entity-header__facts-list.swiper-wrapper")
    DATE_TAG = (By.TAG_NAME, "div")
    
    TOURNIR_TABLE_TBODY = (By.XPATH, "//div[@data-type='0']/table/tbody")
    TOURNIR_TABLE_TEAM_NAME = (By.XPATH, ".//tr/td/a/span[@class='table-item__name']")
    TOURNIR_TABLE_TEAM_LINK = (By.XPATH, ".//tr/td/a[@class='table-item']")
    
    
    def year_option(value: str) -> tuple:
        return (By.XPATH, f"//select/option[@value='{value}']")
    
    
    def tournir_option() -> tuple:
        return (By.XPATH, f"//select/option[contains(text(), 'лига') or contains(text(), 'Лига')]")
    

class TeamPageLocators(object):
    
    TEAM_ABOUT_BUTTON = (By.XPATH, "//div[@class='entity-header__title-link']/noindex/a")
    TEAM_COACH_LINK = (By.XPATH, "//div[@data-type='team']/div/ul/li[span[contains(text(), 'Тренер')]]/a")
    TEAM_PLAYER_LINKS = (By.XPATH, "//div[@class='js-tournament-filter-content']/table/tbody/tr/td/a")
    
    TEAM_NAME = (By.XPATH, "//div[@class='entity-header__title-name']")
    
    
class CoachPageLocators(object):
        
    COACH_FIRST_LAST_NAME_WITH_ABOUT = (By.XPATH, "//div[@class='entity-header__title-name']/a")
    COACH_FIRST_LAST_NAME_WITH_OUT_ABOUT = (By.XPATH, "//div[@class='entity-header__title-name']")
    COACH_BIRTH_DATE = (By.XPATH, "//div[@data-type='coach']/div/ul/li[span[contains(text(), 'Дата рождения')]]/div")
    
class PlayerPageLocators(object):
    
    PLAYER_FIRST_LAST_NAME_WITH_ABOUT = (By.XPATH, "//div[@class='entity-header__title-name']/a")
    PLAYER_FIRST_LAST_NAME_WITH_OUT_ABOUT = (By.XPATH, "//div[@class='entity-header__title-name']")
    PLAYER_NUMBER = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Номер')]]/div")
    PLAYER_ROLE = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Амплуа')]]/div")
    PLAYER_BIRTH_DATE = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Дата рождения')]]/div")
    PLAYER_GROWTH = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Рост')]]/div")
    PLAYER_WEIGHT = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Вес')]]/div")
    PLAYER_TRANSFER_VALUE = (By.XPATH, "//div[@data-type='player']/div/ul/li[span[contains(text(), 'Трансферная стоимость')]]/div")
    
    
class CalendarPageLocators(object):
    
    TBODY_TR_LIST = (By.XPATH, "//table/tbody/tr")
    LEFT_TEAM_LINK = (By.XPATH, ".//a[@class='table-item']")
    RIGHT_TEAM_LINK = (By.XPATH, ".//a[@class='table-item _last']")
    GAME_SCORE = (By.XPATH, ".//span[@class='stat-results__count-main']")
    GAME_DATATIME = (By.XPATH, ".//td[@class='stat-results__date-time _hidden-td']")
    GAME_LINK = (By.XPATH, ".//td[@class='stat-results__link']/a")
    
    
class GamePageLocators(object):
    
    REFEREE_A = (By.XPATH, "//div[@class='match-info__extra']/div[2]/a[1]")
    
    LEFT_COACH_A = (By.XPATH, "(//div[contains(@class, 'swiper-slide')][.//div[contains(@class, 'tournament-title') and normalize-space()='Главные тренеры']][1]//a[contains(@class, 'table-item')])[1]") #//div[@class='swiper-slide js-swiping-tabs-item'][1]/table/tbody/tr/td/a
    RIGHT_COACH_A = (By.XPATH, "(//div[contains(@class, 'swiper-slide')][.//div[contains(@class, 'tournament-title') and normalize-space()='Главные тренеры']][2]//a[contains(@class, 'table-item')])[1]") #//div[@class='swiper-slide js-swiping-tabs-item'][2]/table/tbody/tr/td/a
    
    LEFT_TEAM_GOALS = (By.XPATH, "//div[h2[contains(text(), 'Голы')]]/div[@class='match-stat']/div[@class='match-stat__row _team1']/div")
    RIGHT_TEAM_GOALS = (By.XPATH, "//div[h2[contains(text(), 'Голы')]]/div[@class='match-stat']/div[@class='match-stat__row _team2']/div")
    PLAYER_GOAL_A = (By.XPATH, ".//a[@class='match-stat__player']")
    PLAYER_SUB_GOAL_A = (By.XPATH, ".//div[@class='match-stat__player-sub']/a[@class='match-stat__player2']")
    TYPE_GOAL_DIV = (By.XPATH, ".//div[@class='match-stat__add-info']/div")
    MIN_PLUS_MIN_GOAL_DIV = (By.XPATH, ".//div[@class='match-stat__main-value']/div[@data-goal-label='goal']")
    
    LEFT_TEAM_PENALTIES = (By.XPATH, "//div[h2[contains(text(), 'Наказания')]]/div[@class='match-stat']/div[@class='match-stat__row _team1']/div")
    RIGHT_TEAM_PENALTIES = (By.XPATH, "//div[h2[contains(text(), 'Наказания')]]/div[@class='match-stat']/div[@class='match-stat__row _team2']/div")
    PLAYER_PENALTY_A = (By.XPATH, ".//div[@class='match-stat__info']/a[@class='match-stat__player']")
    MIN_PLUS_MIN_PYNALTY_DIV = (By.XPATH, ".//div[@class='match-stat__main-value']")
    TYPE_PENALTY_SPAN = (By.XPATH, ".//div[@class='match-stat__add-info']/span")
    
    LEFT_TEAM_LINEUP_TR = (By.XPATH, "(//table[contains(@class, 'match-lineup__players')])[1]/tbody/tr")
    RIGHT_TEAM_LINEUP_TR = (By.XPATH, "(//table[contains(@class, 'match-lineup__players')])[2]/tbody/tr")
    PLAYER_LINEUP_HREF_A = (By.XPATH, ".//a[@class='table-item']")
    PLAYER_SAVES_TD = (By.XPATH, ".//td[3]")
    PLAYER_IN_SPAN = (By.XPATH, ".//td[4]/span[@class='_in']")
    PLAYER_OUT_SPAN = (By.XPATH, ".//td[4]/span[@class='_out']")
    
    AUTOUPDATE_SELECT_OFF_OPTION = (By.XPATH, "//div[@class='autoupdate__select select']/select[@name='update-time']/option[@value='0']")
    STAT_DIV = (By.XPATH, "//div[h2[contains(text(), 'Статистика')]]/div[@class='stat-graph__row']")
    LEFT_TEAM_STAT = (By.XPATH, ".//div[contains(@class, 'stat-graph__value') and contains(@class, '_left')]/strong")
    RIGHT_TEAM_STAT = (By.XPATH, ".//div[contains(@class, 'stat-graph__value') and contains(@class, '_right')]/strong")
    STAT_TITLE = (By.XPATH, ".//div[@class='stat-graph__title']")
    
    GAME_STATUS = (By.XPATH, "//div[@class='match-info__score']/div[@class='match-info__status']")