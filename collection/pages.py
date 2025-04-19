import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from datetime import date, datetime

from collection.locators import *
from collection.schemas import *


class BasePage(object):
    """Base class to initialize the base page that will be called from all pages"""
    
    driver: webdriver.Firefox
    page_href: str
    

    def __init__(self, driver: webdriver.Firefox, page_href: str = ''):
        self.driver: webdriver.Firefox = driver
        self.page_href = page_href
        
    
    def go_to_page(self):
        """Переход на страницу
        """
        self.driver.get(self.page_href)
    

class SeasonPage(BasePage):
    
    
    def __init__(self, driver, page_href = 'https://www.championat.com/football/_russiapl.html'):
        super().__init__(driver, page_href)
        self.go_to_page()
        # при переходе по стандартной ссылке необходимо ее обновить нажав на название турнира
        # https://www.championat.com/football/_russiapl.html (ссылка на текущий турнир РПЛ)
        # -> 
        # https://www.championat.com/football/_russiapl/tournament/5980/ (ссылка на текущий турнир РПЛ)
        self.driver.find_element(*MainPageLocators.LINK_REFRESH).click()
        self.page_href = self.driver.current_url # обновляем ссылку
    
    
    def get_season_info(self) -> Season:
        """Получить дату проведения сезона

        Returns:
            tuple[date, date]: дата начала, дата окончания
        """
        # получение данных сезона
        start_date: date
        end_date: date
        el_date_class = self.driver.find_element(*MainPageLocators.DATE_CSS)
        el_date_tag = el_date_class.find_element(*MainPageLocators.DATE_TAG) # первый div-тэг в выборке - искомые даты проведения
        start_end_date = el_date_tag.text.split("—")
        start_date = datetime.strptime(start_end_date[0], "%d.%m.%Y").date()
        end_date = datetime.strptime(start_end_date[1], "%d.%m.%Y").date()
        
        # получение данных команд
        table_body = self.driver.find_element(*MainPageLocators.TOURNIR_TABLE_TBODY)
        team_links = table_body.find_elements(*MainPageLocators.TOURNIR_TABLE_TEAM_LINK)
        team_links_list = [link.get_attribute('href') for link in team_links]
        team_list = []
        for team_link in team_links_list:
            tp = TeamPage(self.driver, team_link)
            team = tp.get_team_info()
            team_list.append(team)
            
        # получение id сезона
        season_id = self.page_href.split('/')[-2] # получаем уникальный id сезона
        
        return Season(id=season_id,
                      start_date=start_date,
                      end_date=end_date,
                      teams=team_list)
    
    
    def get_season_list_options(self) -> list[str]:
        """Получить список доступных сезоновы

        Raises:
            NoSuchElementException: _description_
            NoSuchElementException: _description_

        Returns:
            list[str]: _description_
        """
        try:
            el_year_select = self.driver.find_element(*MainPageLocators.YEAR_SELECT)
        except NoSuchElementException:
            raise NoSuchElementException(f"DOM элемент 'year_select' не найден (возможно была изменена DOM структура страницы).")
        else:
            try:
                els_year_option = el_year_select.find_elements(
                    *MainPageLocators.YEAR_TOURNIR_OPTION)
            except NoSuchElementException:
                raise NoSuchElementException(f"DOM элемент(ы) 'year_option' не найдены (возможно была изменена DOM структура страницы).")
            else: return [el.text for el in els_year_option]
    
    
    def go_to_season(self, year_option_value: str):
        """Переход к сезону на главной странице

        Args:
            year_option_value (str): Example: 2018/2019

        Raises:
            NoSuchElementException: DOM элемент 'year_select' не найден (возможно была изменена DOM структура страницы).
            NoSuchElementException: DOM элемент 'year_select' со значением 'value'
            NoSuchElementException: DOM элемент 'tournir_select' не найден (возможно была изменена DOM структура страницы).
        """
            
        try:
            el_year_select = self.driver.find_element(*MainPageLocators.YEAR_SELECT)
        except NoSuchElementException:
            raise NoSuchElementException(f"DOM элемент 'year_select' не найден (возможно была изменена DOM структура страницы).")
        else:
            try:
                el_year_option = el_year_select.find_element(
                    *MainPageLocators.year_option(year_option_value))
            except NoSuchElementException:
                raise NoSuchElementException(f"DOM элемент 'year_select' со значением 'value' = {year_option_value}")
            else: el_year_option.click()
            
        try:
            el_tournir_select = self.driver.find_element(*MainPageLocators.TOURNIR_SELECT)
        except NoSuchElementException:
            raise NoSuchElementException(f"DOM элемент 'tournir_select' не найден (возможно была изменена DOM структура страницы).")
        else:
            try:
                # блок для проверки наличия tournir_option в tournir_select
                el_tournir_option = el_tournir_select.find_element(*MainPageLocators.tournir_option())
            except NoSuchElementException:
                """
                не обрабатываем, поскольку:
                при правильно введенных значениях start_date и end_date
                и
                наличие tournir_select
                значения option либо есть, либо их нет
                """
                pass
            else: el_tournir_option.click()
        
        # устанавливаем актуальную ссылку
        self.page_href = self.driver.current_url


class TeamPage(BasePage):
    
    
    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
    
    
    def get_team_name_id(self) -> tuple[str, str]:
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab') # соаздем новую страницу
        self.go_to_page() # переходим на страницу команды в текущем сезоне
        self.driver.find_element(*TeamPageLocators.TEAM_ABOUT_BUTTON).click() # переходим на главную страницу команды
        team_name: str = self.driver.find_element(*TeamPageLocators.TEAM_NAME).text # получаем название команды
        team_id: str = self.driver.current_url.split('/')[-2] # получаем уникальный тег команды
        self.driver.close() # закрываем страницу команды
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу
        return team_name, team_id
        
    
    def get_team_info(self) -> Team:
        '''
        '''
        # https://www.selenium.dev/documentation/webdriver/interactions/windows/
        
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab') # соаздем новую страницу
        self.page_href = self.page_href.replace('result', 'players') # изменяем ссылку для перехода на отображение состава
        self.go_to_page() # переходим на страницу команды в текущем сезоне
        
        season_team_id = self.driver.current_url.split('/')[-1]
        
        try:
            # https://www.championat.com/football/_russiapl/tournament/5980/teams/255784/result/
            # играют без тренера) в играх указан, поэтому информацю по нему брать из игр
            coach_link = self.driver.find_element(*TeamPageLocators.TEAM_COACH_LINK).get_attribute('href')
            cp = CoachPage(self.driver, coach_link)
            coach = cp.get_coach_info()
        except NoSuchElementException:
            coach = None
        
        player_links = self.driver.find_elements(*TeamPageLocators.TEAM_PLAYER_LINKS)
        player_links_list = [link.get_attribute('href') for link in player_links]
        player_list = []
        for player_link in player_links_list:
            pp = PlayerPage(self.driver, player_link)
            player = pp.get_player_info()
            player_list.append(player)
        
        team_name, team_id = self.get_team_name_id()
        
        self.driver.close() # закрываем страницу команды
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу
        
        return Team(id=team_id,
                    season_team_id=season_team_id,
                    name=team_name,
                    players=player_list,
                    coach=coach)
    
    
class CoachPage(BasePage):
    
    
    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
        
    
    def get_coach_info(self) -> Coach:
        
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab') # соаздем новую страницу
        self.go_to_page() # переходим на страницу команды в текущем сезоне
        
        coach_id = self.driver.current_url.split('/')[-2] # получаем уникальный id тренера
        
        first_name = None
        middle_name = None
        last_name = None
        birth_date = None

        try: 
            first_middle_last_name = self.driver.find_element(*CoachPageLocators.COACH_FIRST_LAST_NAME_WITH_ABOUT).text.strip().split()
            if len(first_middle_last_name) == 0:
                first_middle_last_name = self.driver.find_element(*CoachPageLocators.COACH_FIRST_LAST_NAME_WITH_OUT_ABOUT).text.strip().split()
            
            if len(first_middle_last_name) == 1:
                first_name = first_middle_last_name[0]
            if len(first_middle_last_name) == 2:
                first_name = first_middle_last_name[0]
                last_name = first_middle_last_name[1]
            if len(first_middle_last_name) > 2:
                first_name = first_middle_last_name[0]
                middle_name = first_middle_last_name[1]
                last_name = first_middle_last_name[2]
        except NoSuchElementException: pass
        except IndexError:
            raise IndexError(f'list index out of range in name: {first_middle_last_name}, url: {self.page_href}')
        
        try: 
            birth_date = self.driver.find_element(*CoachPageLocators.COACH_BIRTH_DATE).text.strip()
            birth_date = datetime.strptime(birth_date, "%d.%m.%Y").date()
        except NoSuchElementException: pass
        
        self.driver.close() # закрываем страницу команды
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу
        
        return Coach(id=coach_id,
                     first_name=first_name,
                     middle_name=middle_name,
                     last_name=last_name,
                     birth_date=birth_date)
        
    
class PlayerPage(BasePage):
    
    
    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
        
        
    def get_player_info(self) -> Player:
        
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab') # соаздем новую страницу
        self.go_to_page() # переходим на страницу команды в текущем сезоне
        
        player_id = self.driver.current_url.split('/')[-2] # получаем уникальный id игрока
        
        first_name = None
        last_name = None
        number = None
        role = None
        birth_date = None
        growth = None
        weight = None
        transfer_value = None
        
        try: 
            first_last_name = self.driver.find_element(*PlayerPageLocators.PLAYER_FIRST_LAST_NAME_WITH_ABOUT).text.strip().split()
            if len(first_last_name) == 0: # для игоков, которые не имеют ссылки на главную карточку игрока
                first_last_name = self.driver.find_element(*PlayerPageLocators.PLAYER_FIRST_LAST_NAME_WITH_OUT_ABOUT).text.strip().split()
                
            if len(first_last_name) > 0:
                first_name = first_last_name[0]
            if len(first_last_name) > 1:
                last_name = first_last_name[1]
                
        except NoSuchElementException: pass
        except IndexError:
            raise IndexError(f'list index out of range in name: {first_last_name}, url: {self.page_href}')
        
        try: number = int(self.driver.find_element(*PlayerPageLocators.PLAYER_NUMBER).text.strip())
        except NoSuchElementException: pass
        
        try: role = self.driver.find_element(*PlayerPageLocators.PLAYER_ROLE).text.strip()
        except NoSuchElementException: pass
        
        try: 
            birth_date = self.driver.find_element(*PlayerPageLocators.PLAYER_BIRTH_DATE).text.strip()
            birth_date = datetime.strptime(birth_date, "%d.%m.%Y").date()
        except NoSuchElementException: pass
        
        try: growth = int(self.driver.find_element(*PlayerPageLocators.PLAYER_GROWTH).text.strip().replace(' см', ''))
        except NoSuchElementException: pass
        
        try: weight = int(self.driver.find_element(*PlayerPageLocators.PLAYER_WEIGHT).text.strip().replace(' кг', ''))
        except NoSuchElementException: pass
        
        try: 
            transfer_value = self.driver.find_element(*PlayerPageLocators.PLAYER_TRANSFER_VALUE).text.strip()
            transfer_value = int(transfer_value.replace(' ', '').replace('€', ''))
        except NoSuchElementException: pass
        
        self.driver.close() # закрываем страницу команды
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу
        
        return Player(id=player_id, 
                      first_name=first_name,
                      last_name=last_name,
                      number=number,
                      role=role,
                      birth_date=birth_date,
                      growth=growth,
                      weight=weight,
                      transfer_value=transfer_value)
        
    
class CalendarPage(BasePage):
    
    
    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
        
        
    def get_calendar_info(self) -> Calendar:
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab')
        self.go_to_page() # переходим на календарь игр сезона
        
        try:
            tr_list = self.driver.find_elements(*CalendarPageLocators.TBODY_TR_LIST)
        except NoSuchElementException: raise NoSuchElementException(f'таблица с играми не найдена. ссылка на календарь сезона: {self.page_href}')
        
        for tr in tr_list:
            tour_number = tr.get_attribute('data-tour')
            is_played = tr.get_attribute('data-played')
            
            # left_team_score = None
            # right_team_score = None
            
            # if (is_played == 1):
            #     game_score = tr.find_element(*CalendarPageLocators.GAME_SCORE).text.strip().split(':')
            #     left_team_score = int(game_score[0]) # автоматически при преобразовании удалит лишние пробелы
            #     right_team_score = int(game_score[1])
            
            left_team_link = tr.find_element(*CalendarPageLocators.LEFT_TEAM_LINK).get_attribute('href')
            left_team = TeamPage(self.driver, left_team_link)
            _, left_team_id = left_team.get_team_name_id()
            right_team_link = tr.find_element(*CalendarPageLocators.RIGHT_TEAM_LINK).get_attribute('href')
            right_team = TeamPage(self.driver, right_team_link)
            _, right_team_id = right_team.get_team_name_id()
            
            game_datatime_str = tr.find_element(*CalendarPageLocators.GAME_DATATIME).text
            game_date_str = re.search(r"\d{2}\.\d{2}\.\d{4}", game_datatime_str)
            game_time_str = re.search(r"\d{2}:\d{2}", game_datatime_str)
            game_date = datetime.strptime(game_date_str, "%d.%m.%Y").date()
            game_time = datetime.strptime(game_time_str, "%H:%M").time()
            # меняем preview на stats для перехода на протокол игры
            game_link = tr.find_element(*CalendarPageLocators.GAME_LINK).get_attribute('href').replace('preview', 'stats')
            game_id = game_link.split('/')[-2]
            
            # обработка страницы игры
            
            
            
        
        self.driver.close() # закрываем страницу
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу    
            
class GamePage(BasePage):
    

    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
        
        
    def get_game_info(self) -> Game:
        original_window = self.driver.current_window_handle # запоминаем текущую страницу
        self.driver.switch_to.new_window('tab')
        self.go_to_page() # переходим на страницу игры
        
        # сбор информации о главном судье
        referee = None
        try:
            el_a_referee = self.driver.find_element(*GamePageLocators.REFEREE_A)
            referee_id = el_a_referee.get_attribute('href').strip().split('/')[-2]
            first_last_referee_name = el_a_referee.text.strip().split(' ')
            first_name_referee = first_last_referee_name[0]
            last_name_referee = first_last_referee_name[1]
            
            referee = Referee(id=referee_id, first_name=first_name_referee, last_name=last_name_referee)
        except NoSuchElementException: pass
        
        left_coach_id = None
        try:
            left_coach_link = self.driver.find_element(*GamePageLocators.LEFT_COACH_A).get_attribute('href')
            left_coach_id = CoachID(left_coach_link.strip().split('/')[-2])
        except: pass
        
        right_coach_id = None
        try:
            right_coach_link = self.driver.find_element(*GamePageLocators.RIGHT_COACH_A).get_attribute('href')
            right_coach_id = CoachID(right_coach_link.strip().split('/')[-2])
        except: pass
        
        # сбор информации о голах
        left_team_goals: list[Goal] = []
        right_team_goals: list[Goal] = []
        
        
        
        self.driver.close() # закрываем страницу
        self.driver.switch_to.window(original_window) # возвращаемся на начальную страницу    
        
        