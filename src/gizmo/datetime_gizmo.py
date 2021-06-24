import datetime
from calendar import monthrange
import pytz
from tzlocal import get_localzone
from core import constants as constants

def today(region=None):
    if region is None:
        dt = datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time())
    else:
        if region=="AMER":
            dt = datetime.datetime.combine(datetime.datetime.now(pytz.timezone("America/New_York")), datetime.datetime.min.time())
        elif region=="EMEA":
            dt = datetime.datetime.combine(datetime.datetime.now(pytz.timezone("Europe/London")), datetime.datetime.min.time())
        elif region=="APAC":
            dt = datetime.datetime.combine(datetime.datetime.now(pytz.timezone("Asia/Shanghai")), datetime.datetime.min.time())    
    return dt