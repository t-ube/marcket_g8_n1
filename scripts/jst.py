import datetime
from pytz import timezone

def now():
    dt = datetime.datetime.utcnow()
    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    jst_now = datetime.datetime.now(JST)
    #jst_now = timezone('Asia/Tokyo').localize()
    #print(str(jst_now))
    #print(jst_now.strftime('%Y-%m-%d %H:%M:%S'))
    return jst_now
