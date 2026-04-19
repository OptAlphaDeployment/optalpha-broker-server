from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Union
import pandas as pd
import requests
import datetime

class BrokerTrade(ABC):
    def __init__(self) -> None: pass

    @abstractmethod
    def get_available_cash(self, username:str) -> float:
        pass

    def get_required_margin(self, instruments:list) -> dict:
        instruments_grouped = defaultdict(list)
        [instruments_grouped[bool(i['token_detail'][2])].append(self.transform(i)) for i in instruments]

        required_margin_details = requests.post("https://margin.truedata.in/api/getPortfolioMargin", json=instruments_grouped[True]).json()

        return required_margin_details

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

    def transform(self, item:dict) -> dict:
        d = item['token_detail']
        return {
            "symbol": d[0],
            "expiry": datetime.datetime.strptime(d[2], "%d%b%y").strftime("%d-%m-%Y") if d[2] else "",
            "strike": float(d[3]) if d[3] else 0,
            "series": d[4],
            "type": item['transaction_type'].lower(),
            "qty": item['lots']
        }