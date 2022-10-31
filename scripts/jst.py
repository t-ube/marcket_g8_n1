import datetime
from pytz import timezone

def now():
    jst_now = timezone('Asia/Tokyo').localize(datetime.datetime.utcnow())
    #print(str(jst_now))
    #print(jst_now.strftime('%Y-%m-%d %H:%M:%S'))
    return jst_now
