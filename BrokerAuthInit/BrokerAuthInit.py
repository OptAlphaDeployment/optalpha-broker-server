import io
import os
import time
import redis
import logging
import telegram
import datetime
import warnings
import requests
import numpy as np
import pandas as pd
from typing import Tuple, Any
from urllib.parse import quote
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text

class BrokerAuthInit(ABC):
    def __init__(self) -> None:
        self.tokens_df = 0
        self.token_symbol_mapping = 0
        self.try_fin = 10

        self._host_ = os.getenv('host')
        self._port_postgres_ = int(os.getenv('port_postgres'))
        self._username_ = os.getenv('username')
        self._password_ = os.getenv('password')

        self.red = redis.Redis(host = 'redis-service', port = 6379, db=0, decode_responses = True)

        self.logger = logging.getLogger('broker_logger')
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            self.console_handler = logging.StreamHandler()
            self.file_handler = logging.FileHandler('/app/BrokerData/Logs/' + str(datetime.datetime.now().date()) + '.log')

            self.console_handler.setLevel(logging.DEBUG)
            self.file_handler.setLevel(logging.INFO)

            self.console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

            self.logger.addHandler(self.console_handler)
            self.logger.addHandler(self.file_handler)

        try:
            if self._host_ == '' or self._host_ is None: pass
            else:
                self.postgres_cluster_users = create_engine(
                    f'postgresql+psycopg2://{self._username_}:%s@{self._host_}:{self._port_postgres_}/users' % quote(self._password_),
                    pool_recycle=3600,
                    pool_size=10,
                    max_overflow=20
                )
        except Exception as e:
            self.logger.error('Unable to connect to postgres engine: ' + str(e))

        try: self.bot =  telegram.bot.Bot(os.getenv('token'))
        except: self.bot = None
        try: self.chat_id = int(os.getenv('chat_id'))
        except: self.chat_id = os.getenv('chat_id')

        self.all = pd.read_csv('/app/BrokerData/Stocks/all.csv').name.to_list()
        self.index_ = ['BANKNIFTY', 'NIFTY', 'MIDCPNIFTY']
        self.index_diff_ = ['NIFTY BANK', 'NIFTY 50', 'NIFTY MID SELECT']

        kite_tokens = pd.read_csv('/app/Tokens/kite_tokens.csv')
        kite_tokens = kite_tokens[['name', 'exchange', 'expiry_date', 'strike', 'instrument_type', 'freeze_quantity']]
        kite_tokens = kite_tokens[(kite_tokens.name.isin(self.index_ + self.all)) & (kite_tokens.exchange.isin(['NSE','NFO']))]
        kite_tokens.drop_duplicates('name', inplace=True)
        self.name_freeze_quantity_mapping = dict(zip(kite_tokens.name, kite_tokens.freeze_quantity))

        warnings.filterwarnings('ignore')

    @abstractmethod
    def login(self, file:dict, verbose:bool = True, single_try:bool = False) -> Any:
        pass

    @abstractmethod
    def update_token_files(self) -> None:
        pass

    @abstractmethod
    def get_tokens_df_from_files(self) -> pd.DataFrame:
        pass

    def get_token(self, name:str, exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '') -> str:
        ''' Get token (unique identifier) of an instrument from the tokens dataframe of the broker.

            Parameters
            ----------
            name: str
                name of the underlying item
                example: 'SBIN'
            exchange: str: 'NSE' / 'NFO'
                'NFO' to get data for options
                'NSE' to get data for cash segment
            expiry: str
                weekly / monthly expiry date in format - '13JAN22' if this is used for an option, else- ''
            strike: str: example: '2500.0'
                strike price if this is used for an option, else- ''
            optionType: str
                'CE'/'PE' if this is used for an option, else- ''

            Returns
            -------
            str: containing unique identifier of the instrument
        '''
        tokens = self.tokens_df.copy()
        if exchange == 'NFO':
            t = tokens[(tokens.strike == strike) & (tokens.expiry == expiry) & (tokens.optionType == optionType) & (tokens.instrumentName == name)]
            item_token = t.instrumentToken.iloc[0]
        else:
            t = tokens[(tokens.instrumentName==name)&(tokens.optionType=='')]
            item_token = t.instrumentToken.iloc[0]
        return str(item_token)

    def get_name(self, token:str) -> Tuple[str, str, str, str, str]:
        try:
            tokens = self.tokens_df[self.tokens_df.instrumentToken == str(token)].copy()
            nam = tokens.instrumentName.iloc[0]
            exp = tokens.expiry.iloc[0]
            strk = tokens.strike.iloc[0]
            typ = tokens.optionType.iloc[0]
            lot = tokens.lotSize.iloc[0]
            return nam, exp, strk, typ, lot
        except:
            return '', '', '', '', ''

    def get_tokens_df(self) -> pd.DataFrame:
        return self.tokens_df

    def round_to(self, row:Any, num_column:str = 'open', precision_column_val:Any = .05) -> float:
        ''' Round given number to nearsest tick size which is .05 in FNO
            Source: https://stackoverflow.com/questions/4265546/python-round-to-nearest-05

            Parameters
            ----------
                row: int/float/Series - 12569.67 or Series with columns __ num_column, optional __ precision_column_val
                num_column: str - 'date' Column name to round
                precision_column_val: float, int, str 100, 2.5 or 'roundvalue' tick_size of the item or  Column name contaning tick_size

        '''
        if type(row) in [int, float, np.float64, np.float32, np.float16, np.int8, np.int16, np.int32, np.int64, str]:
            n = row
        else:
            n = row[num_column]

        if type(precision_column_val)==str:
            if type(row) == int or type(row) == float:
                precision = .05
            else:
                precision = row[precision_column_val]
        else:
            precision = precision_column_val

        n = float(n)
        correction = 0.5 if n >= 0 else -0.5
        return round(int( n/precision+correction ) * precision, 2)

    def get_all_users(self) -> pd.DataFrame:
        with self.postgres_cluster_users.connect() as users_session:
            return pd.read_sql(text(f"SELECT * FROM user_login_info"), users_session)

    def get_user(self, username:str) -> dict:
        with self.postgres_cluster_users.connect() as users_session:
            file = users_session.execute(text(f"SELECT login_info FROM user_login_info WHERE users='{username}'")).all()[0]
            return self.get_data_structures(file[0])

    def update_user(self, username:str, file:dict) -> None:
        with self.postgres_cluster_users.connect() as users_session:
            users_session.execute(text(f"UPDATE user_login_info SET login_info=$${str(file)}$$ WHERE users='{username}'"))
            users_session.commit()

    def set_user(self, username:str, file:dict) -> None:
        with self.postgres_cluster_users.connect() as users_session:
            users_session.execute(text(f"INSERT INTO user_login_info (users, login_info) VALUES ('{username}', $${str(file)}$$)"))
            users_session.commit()

    def delete_user(self, username:str) -> None:
        with self.postgres_cluster_users.connect() as users_session:
            users_session.execute(text(f"DELETE FROM user_login_info WHERE users='{username}'"))
            users_session.commit()

    def print_to_chat(self, username:str, msg:str) -> None:
        try: self.bot.send_message(self.chat_id, username + ': ' + msg)
        except: self.logger.error('Unable to send chat to telegram')

    def get_data_structures(self, str_data_structure:str) -> Any:
        _lst_ = 0
        _locals_ = {'_lst_':_lst_}
        exec('_lst_ = ' + str_data_structure, {}, _locals_)
        return _locals_['_lst_']

    @staticmethod
    def list_update() -> None:
        try:
            nifty_500 = pd.read_csv("https://archives.nseindia.com/content/indices/ind_nifty%dlist.csv" %500).Symbol.to_list()
            try:
                all = pd.read_csv('/app/BrokerData/Stocks/all.csv').name.to_list()
            except:
                all = []
            all = all + nifty_500
            all = list(set(all))
            all.sort()
            pd.DataFrame(all, columns=['name']).to_csv('/app/BrokerData/Stocks/all.csv', index=False)
        except Exception as e:
            print('ATTENTION: unable to update all list. Error is ' + str(e))

        try:
            res = requests.get("https://api.kite.trade/instruments.json")
            kite_tokens = pd.read_json(io.StringIO(res.text), lines=True)
            kite_tokens.to_csv('/app/Tokens/kite_tokens.csv', index=False)
        except Exception as e:
            print('ATTENTION: unable to get kite_tokens. Error is ' + str(e))