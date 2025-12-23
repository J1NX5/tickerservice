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

    
    def get_history_data(self, per, interv):
        logging.info("Run: get_history_data() in yahoofinance.py ")
        for s in self.__symbol_list['symbols']['stocks']:
            ticker = yf.Ticker(s)
            hist_data = ticker.history(period=per, interval=interv).reset_index()
            for row in hist_data.itertuples(index=False):
                ts = int(row.Datetime.timestamp())
                self.__dbo.insert_ticker(
                    s,
                    ts,
                    float(row.Open),
                    float(row.High),
                    float(row.Low),
                    float(row.Close),
                    int(row.Volume),
                )


if __name__ == "__main__":
    yfm = YahooFinanceModul()
    yfm.get_history_data("1d","1m")