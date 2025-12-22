from apscheduler.schedulers.background import BackgroundScheduler
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('log.txt'), 
        logging.StreamHandler()
    ]
)

class Jobcenter:

    def __init__(self):
        self.__scheduler = BackgroundScheduler()
        self.__scheduler.add_job(self.get_ticker, 'interval', minutes=1)

    def start(self):
        return self.__scheduler.start()

    def get_ticker(self):
        pass


if __name__ == "__main__":
    jc = Jobcenter()
    jc.start()

    
