from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By


class MainPageLocators(object):
    
    YEAR_SELECT = (By.NAME, "year")
    TOURNIR_SELECT = (By.NAME, "tournir_id")
    YEAR_TOURNIR_OPTION = (By.TAG_NAME, "option")
    DATE_CSS = (By.CSS_SELECTOR, ".entity-header__facts-list.swiper-wrapper")
    DATE_TAG = (By.TAG_NAME, "div")
    
    
    def year_option(value: str) -> tuple:
        return (By.XPATH, f"//select/option[@value='{value}']")
    
    
    def tournir_option() -> tuple:
        return (By.XPATH, f"//select/option[contains(text(), 'лига') or contains(text(), 'Лига')]")
    

class TournirTableLocators(object):
    
    pass