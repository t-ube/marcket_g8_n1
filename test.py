from get_chrome_driver import GetChromeDriver
from selenium import webdriver
from scripts import seleniumDriverWrapper as wrap
from scripts import cardrush
from scripts import hareruya2
from scripts import magi
from scripts import torecolo
import socket

get_driver = GetChromeDriver()
get_driver.install()

wrapper = wrap.seleniumDriverWrapper()
wrapper.begin(webdriver)
cardrushBot = cardrush.cardrushCsvBot()
hareruya2Bot = hareruya2.hareruya2CsvBot()
magiBot = magi.magiCsvBot()
torecoloBot = torecolo.torecoloCsvBot()

dataDir = './data/marcket/s12_072_098'
#cardrushBot.download(wrapper, 'カイリュー', '072/098', dataDir)
#hareruya2Bot.download(wrapper, 'カイリュー', '072/098', dataDir)
#magiBot.download(wrapper, 1, 'カイリュー', '072/098', dataDir)
torecoloBot.download(wrapper, 'カイリュー', '072/098', dataDir)
wrapper.end()

ip = socket.gethostbyname(socket.gethostname())
print(ip)
