import yfinance as yf
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime, timezone
import logging
from pathlib import Path
from database import DBManager

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

    def get_history_data(self):
        now_utc = int(self.get_utc_now())
        ds_utc_now = self.cast_utc_ts_to_iso(now_utc)
        # for s in self.__symbol_list['symbols']['stocks']:
        #     self.get_history_by_range(s,"1m", str(ds_utc_now))

    def get_utc_now(self) -> int:
        utc_ts = datetime.now(timezone.utc).timestamp()
        return utc_ts

    def get_history_every_minute(self, symb, per, interv):
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

    def get_history_by_range(self, symb, interv, start_var):
        ticker = yf.Ticker(symb)
        hist_data = ticker.history(interval=interv, start=start_var).reset_index()
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

    def cast_utc_ts_to_iso(self, ts: int):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        fmt = "%Y-%m-%d %H:%M"
        return dt.strftime(fmt)




if __name__ == "__main__":
    yfm = YahooFinanceModul()
    yfm.get_history_data()
    #yfm.get_history_data("1d","1m")