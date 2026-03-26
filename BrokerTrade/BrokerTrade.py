from abc import ABC, abstractmethod
from typing import Union
import pandas as pd

class BrokerTrade(ABC):
    def __init__(self) -> None: pass

    @abstractmethod
    def get_available_cash(self, username:str) -> float:
        pass

    @abstractmethod
    def get_required_margin(self, username:str, transaction_type:str, token:str, price_:float, product:str = '') -> float:
        pass

    @abstractmethod
    def get_quote(self, username:str, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '') -> pd.DataFrame:
        pass

    @abstractmethod
    def place_order(self, username:str, transaction_type:str, price_:float, quantity:int, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '',  trigger:float = 0, product:str = '') -> str:
        pass

    @abstractmethod
    def modify_order(self, username:str, order_id:str,  price:float, quantity:Union[int, str] = '', trigger:float = 0) -> str:
        pass

    @abstractmethod
    def cancel_order(self, username:str, order_id:str) -> str:
        pass