import pandas as pd
import datetime as dt
import subprocess
import psutil
import warnings
import threading
import math
from os import getpid
from futu import *
from time import sleep

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

pd.options.mode.chained_assignment = None
lock = threading.Lock()
LOG_PATH = r"D:\windows_server\server_setup\stat_arb\temp\logs.txt"
FUTU_API = r"C:\Users\kevin\AppData\Roaming\Futu\FutuOpenD\futu-open-d\windows\FutuOpenD.exe"
WATCHLIST_PATH = r"D:\windows_server\server_setup\stat_arb\gui\watchlist.txt"
TRADING_PWD = '040891'
TRADING_ENVIRONMENT = TrdEnv.SIMULATE
TRADING_MARKET = TrdMarket.US
FUTUOPEND_ADDRESS = '127.0.0.1'
FUTUOPEND_PORT = 11111
CURRENCY = 'USD'
LEVERAGE = 1
TRADE_SIZE = 0.80
QUICK_FILL = 0.01
LOT_SIZE = 100

class ApiHandler:
    error_data = [
                'Cancel order error: ERROR. the type of order_id param is wrong',
                'This protocol request is too frequent, triggering a frequency limit, please try again later',
                'NN_ProtoRet_TimeOutNN_ProtoRet_TimeOut'
                ]

    def __init__(self):
        pass

    def __str__(self):
        time_format = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
        return time_format

    def printlog(*args, **kwargs):   
        print(*args, **kwargs)
        with open(LOG_PATH, 'a') as file:
            print(*args, **kwargs, file=file)

    def get_trade_context(self):
        return OpenSecTradeContext(filter_trdmarket=TRADING_MARKET, 
                                host=FUTUOPEND_ADDRESS, 
                                port=FUTUOPEND_PORT,
                                security_firm=SecurityFirm.FUTUSG
                                )

    def _init(self):
        subprocess.Popen(FUTU_API)
        self.printlog('[CONNECTED] Trading account is connected.')
        
    def _term(self):
        subprocess.call("taskkill /F /IM FutuOpenD.exe")
        self.printlog('DISCONNECTED] Trading account is disconnected.')

    def _kill(self):
        process_id = getpid()
        for process in psutil.process_iter():
            if process.pid == process_id:
                process.kill()

    def _unlock(self):
        trade_context = self.get_trade_context()
        ret, data = trade_context.unlock_trade(TRADING_PWD)
        if ret == RET_OK:
            try:
                self.printlog('[UNLOCK] Trading account is unlocked.') 
            except Exception:
                pass
        else:
            self._kill()
            self.printlog(f'[ERROR] Unlock trade error: {data}')       
        trade_context.close()

    def get_ratelimit(self):
        try:
            if self.data in self.error_data:
                sleep(30)
        except Exception:
            pass

    def get_current_value(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.accinfo_query(trd_env=TRADING_ENVIRONMENT, 
                                            refresh_cache=True, 
                                            currency=CURRENCY
                                            )
        if ret == RET_OK:
            self.data = []
            self.value = float(data['total_assets'][0].__round__(2))
            trade_context.close()
            return float(data['total_assets'][0].__round__(2))        
        else:
            self.data = data
            if self.data in self.error_data:
                pass            
            trade_context.close()
            return self.value

    def get_all_position(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.position_list_query(trd_env=TRADING_ENVIRONMENT, 
                                                    refresh_cache=True
                                                    )
        if ret == RET_OK:
            if data.shape[0] > 0:         
                qty = data.loc[:,['qty']]
                qty = qty[qty.qty != 0]
                qty = qty.shape[0]
                self.data = []
                self.value = int(qty)
                trade_context.close()
                return self.value
            else:
                qty = 0
                self.data = []
                self.value = int(qty)
                trade_context.close()
                return self.value
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Check position error: {data}')        
            trade_context.close()
            return self.value

    def get_market_state(self):
        self.get_ratelimit()
        trade_context = OpenQuoteContext(FUTUOPEND_ADDRESS, FUTUOPEND_PORT)
        ret, data = trade_context.get_market_state('US.SPY')
        if ret == RET_OK:
            self.data = []
            self.value = str(data['market_state'].iloc[0])
            trade_context.close()
            return self.value
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Market state error: {data}')
            trade_context.close()
            return self.value

    def get_position(self, ticker):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.position_list_query(code=ticker, 
                                                    trd_env=TRADING_ENVIRONMENT, 
                                                    refresh_cache=True
                                                    )
        if ret == RET_OK:
            if data.shape[0] > 0:
                size = data['qty'][0]
                self.data = []
                self.value = int(size)
                trade_context.close()
                return self.value
            else:
                size = 0
                self.data = []
                self.value = int(size)
                trade_context.close()
                return self.value
        else:
            self.data = data
            if self.data != self.error_data:
                self.printlog(f'[ERROR] Holding position error: {data}.')
            trade_context.close()
            return self.value

    def get_side(self, ticker):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.position_list_query(code=ticker, 
                                                    trd_env=TRADING_ENVIRONMENT, 
                                                    refresh_cache=True
                                                    )
        if ret == RET_OK:
            if data.shape[0] > 0:
                self.side = data['position_side'].loc[data['qty'] != 0]
                self.side = self.side.values[0]
                if self.side == 'LONG':
                    self._side = 1
                else:
                    self._side = -1
                self.printlog(f'[POSITION] The position side of {ticker} is {self.side}.')
                trade_context.close()
                self.data = []
                return self._side
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Position side error: {data}.')
            self.printlog(f'[POSITION] The position side of {ticker} is {self.side}.')
            trade_context.close()
            return self._side

    def get_cancel_order(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.cancel_all_order()
        if ret == RET_OK:
            trade_context.close()
            self.data = []
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Cancel order error: {data}.')
            trade_context.close()

    def get_modify_order(self, ticker):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        order_id = self.get_order_id(ticker)
        ret, data = trade_context.modify_order(ModifyOrderOp.CANCEL, 
                                            trd_env=TRADING_ENVIRONMENT,
                                            order_id=order_id, 
                                            qty=0, 
                                            price=0
                                            )
        if ret == RET_OK:
            trade_context.close()
            self.data = []
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Cancel order error: {data}.')
            trade_context.close()

    def get_order_status(self, ticker):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.order_list_query(code=ticker, 
                                                trd_env=TRADING_ENVIRONMENT, 
                                                refresh_cache=True
                                                )
        if ret == RET_OK:
            if data.shape[0] > 0:
                order_status = data['order_status'][0]
                trade_context.close()
                self.data = []
                self.value = str(order_status)
                return self.value
            else:
                order_status = 'UNAVAILABLE'
                self.data = []
                trade_context.close()
                self.value = str(order_status)
                return self.value
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Order status error: {data}.')
            trade_context.close() 
            return self.value

    def get_order_id(self, ticker):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.order_list_query(code=ticker, 
                                                trd_env=TRADING_ENVIRONMENT, 
                                                refresh_cache=True
                                                )
        if ret == RET_OK:
            if data.shape[0] > 0:
                try:
                    order_id = data.loc[:,['order_status', 'order_id']]
                    order_id = order_id[order_id.order_status == 'SUBMITTED']
                    order_id = order_id.order_id.iloc[0]
                    trade_context.close()
                    self.data = []
                    return order_id  
                except Exception:
                    trade_context.close()
                    pass    
            else:
                self.data = []
                trade_context.close()
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Order status error: {data}.')
            trade_context.close() 
            return None

    def get_entry_size(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.accinfo_query(trd_env=TRADING_ENVIRONMENT, 
                                            refresh_cache=True, 
                                            currency=CURRENCY
                                            )
        if TRADING_ENVIRONMENT == TrdEnv.REAL:
            account_type = 'power'
        else:
            account_type = 'cash'

        if ret == RET_OK:
            buying_power = (data[account_type][0] * LEVERAGE).__round__(2)
            trade_context.close()
            self.data = []
        else:
            buying_power = 0.00
            trade_context.close()
            self.data = self.error_data

        entry_size = (buying_power * TRADE_SIZE).__round__(2)
        return float(entry_size)

    def get_long(self, close, ticker):
        self.get_ratelimit()
        buy_price = close * QUICK_FILL
        buy_price = (close + buy_price).__round__(2)
        entry_size = self.get_entry_size()
        trade_context = self.get_trade_context()
        i = 0

        for i in range(5):
            i += 1
            try:
                if self.get_position(ticker) == 0:
                    size = math.ceil(entry_size / close)
                else:
                    size = abs(self.get_position(ticker))
                self.printlog(f'[BID] {size:,} Shares for ${buy_price} of {ticker}.')
               
                ret, data = trade_context.place_order(price=buy_price, 
                                                    qty=size, 
                                                    code=ticker, 
                                                    trd_side=TrdSide.BUY, 
                                                    trd_env=TRADING_ENVIRONMENT
                                                    )
                if ret == RET_OK:
                    price = data['price'][0].__round__(2)
                    trade_context.close()
                    self.data = []
                if ret != RET_OK:
                    self.data = data
                    if self.data in self.error_data:
                        self.printlog(f'[ERROR] long position error: {data}.')
                    if data == 'Insufficient buying power.': 
                        raise ValueError
                    trade_context.close()
            except ValueError:
                entry_size = entry_size - (i * LOT_SIZE)
                sleep(8)
            else: 
                break    

    def get_short(self, close, ticker):  
        self.get_ratelimit()     
        sell_price = close * QUICK_FILL
        sell_price = (close - sell_price).__round__(2)
        entry_size = self.get_entry_size()
        trade_context = self.get_trade_context()
        i = 0

        for i in range(5):
            i += 1
            try:
                if self.get_position(ticker) == 0:        
                    size = math.ceil(entry_size / close)
                else:
                    size = abs(self.get_position(ticker))
                self.printlog(f'[ASK] {size:,} Shares for ${sell_price} of {ticker}.')
                
                ret, data = trade_context.place_order(price=sell_price, 
                                                    qty=size, 
                                                    code=ticker, 
                                                    trd_side=TrdSide.SELL, 
                                                    trd_env=TRADING_ENVIRONMENT
                                                    )
                if ret == RET_OK:
                    price = data['price'][0].__round__(2)
                    trade_context.close()   
                    self.data = []
                if ret != RET_OK:
                    self.data = data
                    if self.data in self.error_data:
                        self.printlog(f'[ERROR] Short position error: {data}.')
                    if data == 'Insufficient buying power.': 
                        raise ValueError
                    trade_context.close()
            except ValueError:
                enumerate = entry_size - (i * LOT_SIZE)
                sleep(8)
            else: 
                break

    def get_list(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.position_list_query(trd_env=TRADING_ENVIRONMENT, refresh_cache=True)
        if ret == RET_OK:
            if data.shape[0] > 0:
                qty = data[data['qty'] != 0]
                trade_context.close()
                self.data = []
            else:
                trade_context.close()
                self.data = []
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] Pair position error: {data}.')
            trade_context.close()
            return
        try:
            df = pd.DataFrame(qty)
            df = df[['code', 'qty', 'cost_price', 'market_val', 'position_side', 'pl_ratio']]
            df['qty'] = int(df['qty'])
            df['pl_ratio'] = df['pl_ratio'].__round__(2)
            if not df.empty:
                df.to_string(WATCHLIST_PATH, max_colwidth=11, col_space=7, index=False, header=False)
        except Exception:
            pass

    def get_interday(self):
        self.get_ratelimit()
        trade_context = self.get_trade_context()
        ret, data = trade_context.history_order_list_query(trd_env=TRADING_ENVIRONMENT)
        if ret == RET_OK:
            if data.shape[0] > 0:
                holding_date = data['updated_time'].iloc[0]
                old_format ='%Y-%m-%d %H:%M:%S'
                new_format ='%Y-%m-%d'
                holding_date = dt.datetime.strptime(holding_date, old_format) - timedelta(1)
                holding_date = dt.datetime.strftime(holding_date, new_format)
                date_today = dt.datetime.now().strftime('%Y-%m-%d')
                trade_context.close()
                self.data = []
            else:
                trade_context.close()
                self.data = []
        else:
            self.data = data
            if self.data in self.error_data:
                self.printlog(f'[ERROR] interday error: {data}.')
            trade_context.close()
            return 0

        if holding_date < date_today:
            return 1
        else:
            return -1
