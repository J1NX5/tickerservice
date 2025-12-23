import sqlite3
from pathlib import Path

class DBManager:
    def __init__(self):
        self.__BASE_DIR = Path(__file__).parent.parent
        self.db_path: str = str(self.__BASE_DIR) + "/data/data.db"
        self.conn = self._create_connection(self.db_path)
        self.create_table()

    def _create_connection(self, db_file):
        connection = sqlite3.connect(db_file)
        return connection

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticker_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                datetime INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            ); 
        ''')

        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS unique_symbol_datetime ON ticker_table(symbol, datetime);
        ''')
        return self.conn.commit()

    def insert_ticker(
        self, 
        symbol: str, 
        datetime: int, 
        open: float, 
        high: float, 
        low: float, 
        close: float,
        volume: float
        ):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_table(
                symbol,
                datetime,
                open,
                high,
                low,
                close,
                volume

            )
            VALUES (?, ?, ?, ?, ?, ?, ? );
            ''', (  symbol, datetime, open, high, low, close, volume )
        ) 
        return self.conn.commit()

