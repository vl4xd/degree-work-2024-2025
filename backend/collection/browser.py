from selenium import webdriver
import asyncio
from concurrent.futures import ThreadPoolExecutor


class BrowserConnection:
    """Контекстный менеджер веб-браузера Firefox
    """
    def __init__(self):
        pass
    
    def __enter__(self):
        options = webdriver.FirefoxOptions()
        options.set_preference('dom.webdriver.enabled', False) # деактивация вебдрайвера
        options.set_preference('media.volume_scale', '0.0')
        options.add_argument('--headless') # не запускать GUI браузера
        options.set_preference('general.useragent.override', 'useragent1')
        
        self.browser = webdriver.Firefox(options=options, executable_path='/usr/local/bin/geckodriver') # executable_path на сервере
        
        # Устанавливаем тайм-аут для поиска элементов
        # self.browser.implicitly_wait(10)  # 10 секунд
        
        # Устанавливаем тайм-аут для загрузки страницы
        self.browser.set_page_load_timeout(30)  # 30 секунд
        
        # Устанавливаем тайм-аут для выполнения скриптов
        #self.browser.set_script_timeout(30)  # 30 секунд
        
        return self.browser
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.browser.quit()
        

class AsyncBrowserConnection:
    """Асинхронный контекстный менеджер браузера"""
    def __init__(self):
        self.executor = ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()

    async def __aenter__(self):
        options = webdriver.FirefoxOptions()
        options.set_preference('dom.webdriver.enabled', False) # деактивация вебдрайвера
        options.set_preference('media.volume_scale', '0.0')
        options.add_argument('--headless') # не запускать GUI браузера
        options.set_preference('general.useragent.override', 'useragent1')
        
        self.browser = await self.loop.run_in_executor(
            self.executor,
            lambda: webdriver.Firefox(options=options, executable_path='/usr/local/bin/geckodriver'))
        
        # Устанавливаем тайм-аут для поиска элементов
        # self.browser.implicitly_wait(10)  # 10 секунд
        
        # Устанавливаем тайм-аут для загрузки страницы
        self.browser.set_page_load_timeout(30)  # 30 секунд
        
        # Устанавливаем тайм-аут для выполнения скриптов
        #self.browser.set_script_timeout(30)  # 30 секунд
        
        return self.browser
    
    async def __aexit__(self, *args):
        await self.loop.run_in_executor(
            self.executor,
            self.browser.quit)
        


    