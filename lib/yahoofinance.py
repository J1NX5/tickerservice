import yfinance as yf
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime
import logging
from pathlib import Path
from lib.database import DBManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('../log.txt'), 
        logging.StreamHandler()
    ]
)

#open
#high
#close
#volume

class YahooFinanceModul:

    def __init__(self):
        self.__BASE_DIR = Path(__file__).parent.parent
        self.__symbol_list = self.load_symbols()
        self.__dbo = DBManager()

    def load_symbols(self):
        with open(f'{self.__BASE_DIR}/symbols.yml', 'r') as file:
            data = yaml.safe_load(file)
        return data
    
    def get_ticker(self):
        for e in self.__symbol_list['symbols']['stocks']:
            tdata = yf.Ticker(e)
            # print(f'Symbol: {e}, Data:{tdata.info}')
            self._write_file(e, tdata.info)


    '''
    Look if the symbol is in database
    if symbol is in db get the last timestamp and set this to start and today to end call get_history_by_range
    if not call get_history_data with period 8d interval 1m
    '''
    def get_history_data(self):
        for s in self.__symbol_list['symbols']['stocks']:
            check_symbol = self.__dbo.check_symbol(s)
            if check_symbol:
                last_ts = self.__dbo.get_last_ts_of_symbol(s)[0]
                today_ts = self.get_ts_for_today()
                if self.check_day_different(last_ts, today_ts) > 8:
                    self.get_history_first_run("8d","1m")
                else:
                    dd = self.check_day_different(last_ts, today_ts)
                    self.get_history_by_range(s, f'{dd}d', "1m", last_ts)
            else:
                self.get_history_first_run(s,"8d","1m")

    def check_day_different(self, ts1, ts2):
        dt1 = datetime.fromtimestamp(ts1)
        dt2 = datetime.fromtimestamp(ts2)
        delta = dt2 - dt1
        days = abs(delta).days
        return days



    def get_ts_for_today(self) -> int:
        return int(time.time())

    def get_history_first_run(self, symb, per, interv):
        ticker = yf.Ticker(symb)
        hist_data = ticker.history(period=per, interval=interv).reset_index()
        for row in hist_data.itertuples(index=False):
            ts = int(row.Datetime.timestamp())
            self.__dbo.insert_ticker(
                symb,
                ts,
                float(row.Open),
                float(row.High),
                float(row.Low),
                float(row.Close),
                int(row.Volume),
            )

    def get_history_by_range(self, symb, per, interv, start_var):
        ticker = yf.Ticker(symb)
        print(self.cast_ts_to_iso(start_var))
        hist_data = ticker.history(period=per, interval=interv, start=start_var).reset_index()
        for row in hist_data.itertuples(index=False):
            ts = int(row.Datetime.timestamp())
            self.__dbo.insert_ticker(
                symb,
                ts,
                float(row.Open),
                float(row.High),
                float(row.Low),
                float(row.Close),
                int(row.Volume),
            )

    def cast_ts_to_iso(self, ts: int):
        dt = datetime.fromtimestamp(ts)
        fmt = "%Y-%m-%d %H:%M"
        return dt.strftime(fmt)




if __name__ == "__main__":
    yfm = YahooFinanceModul()
    yfm.get_history_data()
    #yfm.get_history_data("1d","1m")