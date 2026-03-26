from BrokerAuthInit import BrokerAuthInit
from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
import time

class BrokerOrd(ABC):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init

    def update_order_update_time(self, username:str) -> None:
        self.broker_auth_init.red.set(username + '_order_update_time', time.time())

    def get_orders_df(self, username:str) -> Union[int, pd.DataFrame]:
        try:
            orders_df = pd.read_json(self.broker_auth_init.red.get(username + '_orders_df'))
            orders_df = orders_df.astype(str)
            orders_df.orderQuantity = orders_df.orderQuantity.astype(int)
            orders_df.pendingQuantity = orders_df.pendingQuantity.astype(int)
            orders_df.price = orders_df.price.astype(float)
        except: orders_df = 0
        return orders_df

    def get_order_update_time(self, username:str) -> Union[int, str]:
        try: order_update_time = self.broker_auth_init.red.get(username + '_order_update_time')
        except: order_update_time = ''
        return order_update_time

    @abstractmethod
    def orders(self, username:str) -> pd.DataFrame:
        pass