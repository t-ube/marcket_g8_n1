from get_chrome_driver import GetChromeDriver
from selenium import webdriver

get_driver = GetChromeDriver()
get_driver.install()
driver = webdriver.Chrome()

driver.get("https://google.com")
time.sleep(3)
driver.quit()
