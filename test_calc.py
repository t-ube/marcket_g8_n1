import os
import json
import httpx
import postgrest
import datetime
import time
import copy
import pandas as pd
import expansion
from pathlib import Path
from uuid import UUID
from scripts import jst
from scripts import marcketCalc
from scripts import marcketPrice

def getCardMarketResult(master_id:str,price):
    daily = marcketPrice.priceDaily()
    
    daily.setDescribeData(price['current'])
    if daily.validate() == False:
        print('Validation alert:'+master_id)
        daily.inf2zero()
        price['summary7Days'] = daily.get()

    daily.setDescribeData(price['summary7Days'])
    if daily.validate() == False:
        print('Validation alert:'+master_id)
        daily.inf2zero()
        price['summary7Days'] = daily.get()

    vol = marcketPrice.priceVolatility()

    vol.set(price['volatility'])
    if vol.validate() == False:
        print('Validation alert :'+master_id)
        vol.inf2zero()
        price['volatility'] = vol.get()

    timestamp = datetime.datetime.utcnow()
    batch_item = {
        "master_id": master_id,
        "updated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
        "calculated_at": timestamp.strftime('%Y-%m-%d %H:%M:%S+00'),
        "card_price": price
    }
    return batch_item

# 1週間分のデータを取得する。
def getWeeklyData(ioCsv, currentDT):
    firstDate = currentDT - datetime.timedelta(days=7)
    rangeDf = pd.DataFrame(index=pd.date_range(
        firstDate.strftime('%Y-%m-%d'),
        currentDT.strftime('%Y-%m-%d')))
    dfCsv = ioCsv.getDataframe()
    d7Df = pd.merge(rangeDf,dfCsv,how='outer',left_index=True,right_index=True)
    #d7Df = d7Df.replace(0, {'count': None})
    fillDf = d7Df.interpolate('ffill')
    formatDf = fillDf.asfreq('1D', method='ffill').fillna(0).tail(7)
    print(formatDf)
    return formatDf

# 半年分のデータを取得する。（2週間間隔）
def getHalfYearData(ioCsv, currentDT):
    firstDate = currentDT - datetime.timedelta(days=168)
    rangeDf = pd.DataFrame(index=pd.date_range(
        firstDate.strftime('%Y-%m-%d'),
        currentDT.strftime('%Y-%m-%d')))
    dfCsv = ioCsv.getDataframe()
    d168Df = pd.merge(rangeDf,dfCsv,how='outer',left_index=True,right_index=True)
    #d168Df = d168Df.replace(0, {'count': None})
    fillDf = d168Df.interpolate('ffill')
    formatDf = fillDf.asfreq('14D', method='ffill').fillna(0)
    print(formatDf)
    return formatDf

currentDT = jst.now()
print(currentDT)

#batch = ['s12_115_098','s12a_014_172']
batch = ['s12_115_098']
batch_results = []

for master_id in batch:
    records = []
    recordDf = pd.DataFrame.from_records(records)
    print(master_id)
    if len(records) > 0:
        recordDf = recordDf.sort_values(by=['datetime'], ascending=False) 
        recordDf = recordDf[~recordDf.duplicated(subset=['market','date','name','link'],keep='first')]
    #print(recordDf)
    dataDir = './data/marcket/'+master_id
    if os.path.exists(dataDir) == False:
        Path(dataDir).mkdir(parents=True, exist_ok=True)

    # 日次情報
    dailyCsv = marcketPrice.dailyPriceIOCSV(dataDir)
    dailyCsv.load()
    calc = marcketCalc.calc(currentDT.strftime('%Y-%m-%d'))
    
    # 集計結果
    # 1週間分のデータを取得する。（日間）
    daysDf = getWeeklyData(dailyCsv, currentDT)
    halfYearDf = getHalfYearData(dailyCsv, currentDT)
    # 最初と最後を抽出する
    sampleDf = pd.concat([daysDf.head(1), daysDf.tail(1)])
    batch_results.append(getCardMarketResult(master_id,
        calc.getWriteDailyDf(
        None,
        daysDf.tail(1),
        sampleDf.diff().tail(1),
        daysDf,
        daysDf.diff(),
        halfYearDf,
        halfYearDf.diff())
    ))
print(batch_results)