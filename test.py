from get_chrome_driver import GetChromeDriver
from selenium import webdriver
import socket
import pandas as pd
from scripts import seleniumDriverWrapper as wrap
from scripts import cardrush
from scripts import hareruya2
from scripts import magi
from scripts import torecolo
from scripts import marcketConfig

get_driver = GetChromeDriver()
get_driver.install()

wrapper = wrap.seleniumDriverWrapper()
wrapper.begin(webdriver)
cardrushBot = cardrush.cardrushCsvBot()
hareruya2Bot = hareruya2.hareruya2CsvBot()
magiBot = magi.magiCsvBot()
torecoloBot = torecolo.torecoloCsvBot()

expansionList = [
    'S12'
]

for expansion in expansionList:
    dfExp = pd.read_csv('./data/card/'+expansion+'.csv', header=0, encoding='utf_8_sig')

    for index, row in dfExp.iterrows():
        if pd.isnull(row['master_id']):
            print('skip:'+row['name'])
            continue
        if row['is_mirror'] == 'TRUE':
            print('skip:'+row['name'])
            continue
        
        dataDir = './data/marcket/'+row['master_id']
        conf = marcketConfig.marcketConfigIO(dataDir)
        conf.load()

        #cardrushBot.download(wrapper, 'カイリュー', '072/098', dataDir)
        #hareruya2Bot.download(wrapper, 'カイリュー', '072/098', dataDir)
        #magiBot.download(wrapper, 1, 'カイリュー', '072/098', dataDir)

        if conf.checkUpdate('torecolo', 24):
            torecoloBot.download(wrapper, row['name'], row['cn'], dataDir)
            conf.update('torecolo')
            conf.save()

wrapper.end()

ip = socket.gethostbyname(socket.gethostname())
print(ip)
