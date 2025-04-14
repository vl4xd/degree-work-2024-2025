from selenium import webdriver


class BrowserConnection:
    """Контекстный менеджер веб-браузера Firefox
    """
    def __init__(self):
        pass
    
    def __enter__(self):
        options = webdriver.FirefoxOptions()
        options.set_preference('dom.webdriver.enabled', False) # деактивация вебдрайвера
        options.set_preference('media.volume_scale', '0.0')
        # options.add_argument('--headless') # не запускать GUI браузера
        options.set_preference('general.useragent.override', 'useragent1')
        
        self.browser = webdriver.Firefox(options=options)
        return self.browser
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.browser.quit()
    