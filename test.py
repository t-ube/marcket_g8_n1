import time
from get_chrome_driver import GetChromeDriver
from selenium import webdriver
from scripts import seleniumDriverWrapper as wrap
from scripts import cardrush

get_driver = GetChromeDriver()
get_driver.install()

'''
def driver_init():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    return webdriver.Chrome(options=options)
'''

wrapper = wrap.seleniumDriverWrapper()
wrapper.begin(webdriver)
cardrush = cardrush.cardrushCsvBot()

dataDir = './data/marcket/s12_072_098'
cardrush.download(wrapper, 'カイリュー', '072/098', dataDir)
wrapper.end()

#driver = driver_init()
#driver.get("https://google.com")
#time.sleep(3)
#driver.quit()
