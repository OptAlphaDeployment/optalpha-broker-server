from BrokerAuthInit import BrokerAuthInit
from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
import time

class BrokerPos(ABC):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init

    def update_position_update_time(self, username:str) -> None:
        self.broker_auth_init.red.set(username + '_position_update_time', time.time())

    def get_positions_df(self, username:str) -> Union[int, pd.DataFrame]:
        try:
            positions_df = pd.read_json(self.broker_auth_init.red.get(username + '_positions_df'))
            positions_df = positions_df.astype(str)
            positions_df.actualPNL = positions_df.actualPNL.astype(float)
            positions_df.averageStockPrice = positions_df.averageStockPrice.astype(float)
            positions_df.lastPrice = positions_df.lastPrice.astype(float)
            positions_df.netTrdQtyLot = positions_df.netTrdQtyLot.astype(int)
        except: positions_df = 0
        return positions_df

    def get_position_update_time(self, username:str) -> Union[int, str]:
        try: position_update_time = self.broker_auth_init.red.get(username + '_position_update_time')
        except: position_update_time = ''
        return position_update_time

    @abstractmethod
    def positions(self, username:str) -> pd.DataFrame:
        pass