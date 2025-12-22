import yfinance as yf
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime
import logging
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('log.txt'), 
        logging.StreamHandler()
    ]
)

class YahooFinanceModul:

    def __init__(self):
        self.__BASE_DIR = Path(__file__).parent.parent
        self.__symbol_list = self.load_symbols()

    def load_symbols(self):
        with open(f'{self.__BASE_DIR}/symbols.yml', 'r') as file:
            data = yaml.safe_load(file)
        return data


    def get_ticker(self):
        for e in self.__symbol_list['symbols']['stocks']:
            tdata = yf.Ticker(e)
            # print(f'Symbol: {e}, Data:{tdata.info}')
            self._write_file(e, tdata.info)

    def _write_file(self, symbol, data) -> None:
        row = {
        "symbol": [symbol],
        "price": [data.get("regularMarketPrice")],
        "volume": [data.get("regularMarketVolume")],
        "market_cap": [data.get("marketCap")],
        "currency": [data.get("currency")],
        "timestamp": [int(time.time() * 1000)],
        }
        table = pa.Table.from_pydict(row)

        filename = self._make_filename(symbol)
        pq.write_table(table, f'{self.__BASE_DIR}/ticker/{filename}')
        return None


    def _make_filename(self, symbol: str) -> str:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
        return f"{symbol}_{ts}.parquet"

if __name__ == "__main__":
    yfm = YahooFinanceModul()
    yfm.get_ticker()