import pandas as pd
from futu import *
import datetime
from datetime import date, timedelta

FUTU_API = r"C:\Users\kevin\AppData\Roaming\Futu\FutuOpenD\futu-open-d\windows\FutuOpenD.exe"
LOG_PATH = r"D:\windows_server\server_setup\stat_arb\temp\logs.txt"

TRADING_PWD             = '040891'
TRADING_ENVIRONMENT     = TrdEnv.REAL
TRADING_MARKET          = TrdMarket.US
FUTUOPEND_ADDRESS       = '127.0.0.1'
FUTUOPEND_PORT          = 11111
CURRENCY                = 'USD'
STATE                   = ['PRE_MARKET_BEGIN', 'AFTERNOON', 'AFTER_HOURS_BEGIN', 'AFTER_HOURS_END']

TRADE_SIZE              = 1
ENTRY_SIZE              = 0.00
RISK                    = -2.0
REWARD                  = 3.00
DAILY_RISK              = -2.0
DAILY_REWARD            = 3.00
MAX_SLIPPAGE            = -3.0
QUICK_FILL              = 0.01
LEVERAGE                = 1

PRE_MARKET              = '20:00'
POST_MARKET             = '05:01'
BEF_CLOSE               = '03:50'
AFT_CLOSE               = '04:00'


def printlog(*args, **kwargs):   
    print(*args, **kwargs)
    with open(LOG_PATH, 'a') as file:
        print(*args, **kwargs, file=file)

def get_current_time(f='%Y-%m-%d %H:%M'):
    return datetime.datetime.now().strftime(f)


class HoldingPosition:
    #Get holding positions data as dataframe.
    def data(self):
        trade_context = OpenSecTradeContext(filter_trdmarket=TRADING_MARKET, host=FUTUOPEND_ADDRESS, port=FUTUOPEND_PORT, security_firm=SecurityFirm.FUTUSG)
        ret, data = trade_context.position_list_query(trd_env=TRADING_ENVIRONMENT, refresh_cache=True)
        if ret == RET_OK:
            if data.shape[0] > 0:
                trade_context.close()
                df = pd.DataFrame(data)
                df.index = df.index + 1 #Index count start at 1.
                return df
            else:
                printlog(f'{get_current_time()} [ERROR] Holding position error: N/A position')
                trade_context.close()
        else:
            printlog(f'{get_current_time()} [ERROR] Holding position error: {data}')
            trade_context.close()
            return 0

    def code(self, i):
        #Get holding position code.
        try:
            return str(HoldingPosition.data(self=self).code.get(i))
        except Exception:
            return False
        
    def quantity(self, i):
        #Get holding position quantity.
        try:
            return int(HoldingPosition.data(self=self).qty.get(i))
        except Exception:
            return 0

    def pl_ratio(self, i):
        #Get holding position ratio.
        try:
            return float(HoldingPosition.data(self=self).pl_ratio.get(i).__round__(2))
        except Exception:
            return 0

    def position_side(self, i):
        #Get holding position side.
        try:
            return str(HoldingPosition.data(self=self).position_side.get(i))
        except Exception:
            return False
    
