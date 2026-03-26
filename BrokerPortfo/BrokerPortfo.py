from BrokerAuthInit import BrokerAuthInit
from abc import ABC, abstractmethod
from typing import Union
import pandas as pd

class BrokerPortfo(ABC):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init

    def get_portfolio_df(self, username:str) -> Union[int, pd.DataFrame]:
        try:
            portfolio_df = pd.read_json(self.broker_auth_init.red.get(username + '_portfolio_df'))
            portfolio_df = portfolio_df.astype(str)
            portfolio_df.averageStockPrice = portfolio_df.averageStockPrice.astype(float)
            portfolio_df.lastPrice = portfolio_df.lastPrice.astype(float)
            portfolio_df.netTrdQtyLot = portfolio_df.netTrdQtyLot.astype(int)
        except: portfolio_df = 0
        return portfolio_df

    @abstractmethod
    def portfolio(self, username:str) -> pd.DataFrame:
        pass