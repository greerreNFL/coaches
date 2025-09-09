import numpy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

## utility func ##
def id_from_url(url):
    '''
    Pulls the coaches ID from the anchor tag
    '''
    try:
        return url.split('.htm')[0].split('/coaches/')[1]
    except:
        return numpy.nan

class Browser:
    '''
    Singleton selenium browser wrapper for scraping pro-football-reference.com.
    
    '''
    _instance = None
    _driver = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Browser, cls).__new__(cls)
        return cls._instance

    def start(self, headless=True):
        '''
        Start the browser if not already running
        '''
        if self._driver is None:
            options = Options()
            if headless:
                options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36')
            self._driver = webdriver.Chrome(options=options)

    def stop(self):
        '''
        Stop the browser
        '''
        if self._driver:
            self._driver.quit()
            self._driver = None

    def get_page_html(self, url, wait_for_element=None, timeout=10):
        '''
        Get HTML content from a URL, optionally wait for specific element
        '''
        if not self._driver:
            self.start()
        
        self._driver.get(url)
        
        ## wait for specific element if provided ##
        if wait_for_element:
            wait = WebDriverWait(self._driver, timeout)
            wait.until(EC.presence_of_element_located(wait_for_element))
        
        return self._driver.page_source