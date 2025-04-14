import time
import abc
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from datetime import date, datetime

from collection.locators import *


class TransformToYearOptionValue(abc.ABC):
    
    @staticmethod
    @abc.abstractmethod
    def to_year_option_value():
        pass
    
    
class TransformDate(TransformToYearOptionValue):
    
    @staticmethod
    def to_year_option_value(start_date: date, end_date: date) -> str:
        """Преобразование в строку для year_option_value

        Args:
            start_date (date): дата начала сезона
            end_date (date): дата окончания сезона

        Raises:
            ValueError: год end_date не должен превышать год start_date
            ValueError: end_date не должен превышать один год со start_date

        Returns:
            str: year_option_value
        """
        start_date_year, end_date_year = start_date.year, end_date.year
        
        chech_date = end_date_year - start_date_year
        if chech_date == 0:
            year_option_value = f"{start_date_year}"
        elif chech_date == 1:
            year_option_value = f"{start_date_year}/{end_date_year}"
        elif chech_date < 0: raise ValueError("год end_date не должен превышать год start_date")
        else: raise ValueError("end_date не должен превышать один год со start_date")
        
        return year_option_value
    


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
    

class MainPage(BasePage):
    
    
    def __init__(self, driver, page_href = 'https://www.championat.com/football/_russiapl/tournament/5980/'):
        super().__init__(driver, page_href)
            
    
    def get_season_date(self) -> tuple[date, date]:
        """Получить дату проведения сезона

        Returns:
            tuple[date, date]: дата начала, дата окончания
        """
        start_date: date
        end_date: date
        
        el_date_class = self.driver.find_element(*MainPageLocators.DATE_CSS)
        # первый div-тэг в выборке - искомые даты проведения
        el_date_tag = el_date_class.find_element(*MainPageLocators.DATE_TAG)
        start_end_date = el_date_tag.text.split("—")
        start_date = datetime.strptime(start_end_date[0], "%d.%m.%Y").date()
        end_date = datetime.strptime(start_end_date[1], "%d.%m.%Y").date()
        
        return (start_date, end_date)
    
    
    def get_season_list(self) -> list[str]:
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


class TournirTablePage(BasePage):
    
    def __init__(self, driver, page_href):
        super().__init__(driver, page_href)
        self.page_href += "table/"    
    