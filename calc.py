import socket
import pandas as pd
from pathlib import Path
from scripts import jst
from scripts import marcketCalc
from scripts import marcketPrice

# 半年分のデータを取得する。（2週間間隔）
def getHalfYearData(ioCsv):
    d180Df = ioCsv.getDataframe().tail(180)
    d180Df = d180Df.replace(0, {'count': None})
    fillDf = d180Df.interpolate('ffill').interpolate('bfill')
    formatDf = fillDf.asfreq('14D', method='ffill')
    print(formatDf)
    return formatDf

currentDT = jst.now()
print(currentDT)

Path('./dist').mkdir(parents=True, exist_ok=True)

dataDir = './data/marcket/s12_072_098'
file = './dist/s12_072_098.json'

calc = marcketCalc.calc(currentDT.strftime('%Y-%m-%d'))
if calc.checkUpdate(file, 4) is False:
    print('Already calculated')
else:
    ioCsv = marcketPrice.dailyPriceIOCSV(dataDir)
    backup = marcketPrice.backupPriceRawCSV(dataDir)
    ioCsv.load()
    df = calc.getBaseDf(dataDir)
    days30Df = calc.getDailyDf(df,30)

    #ioCsv.add(days30Df)
    #ioCsv.save()

    # 1週間分のデータを取得する。（日間）
    daysDf = ioCsv.getDataframe().tail(7)
    daysDf = daysDf.replace(0, {'count': None})
    fillDf = daysDf.interpolate('ffill').interpolate('bfill')
    halfYearDf = getHalfYearData(ioCsv)
    # 最初と最後を抽出する
    sampleDf = pd.concat([fillDf.head(1), fillDf.tail(1)])
    calc.writeDailyDf(
        file,
        fillDf.tail(1),
        sampleDf.diff().tail(1),
        daysDf,
        fillDf.diff(),
        halfYearDf,
        halfYearDf.diff())
    backup.backup(1)

ip = socket.gethostbyname(socket.gethostname())
print(ip)
