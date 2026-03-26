from BrokerAuthInit import BrokerAuthInit
from BrokerPortfo import BrokerPortfo
from SmartApi import SmartConnect
import pandas as pd
import numpy as np
import datetime
import time
import os

class AngelPortfo(BrokerPortfo):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init
        super().__init__(broker_auth_init)

    def portfolio(self, username:str) -> pd.DataFrame:
        ''' Returns dataframe with info on portfolio

            Returns
            -------
            dataframe with following columns:
                averageStockPrice: float
                instrumentToken: str
                lastPrice: float
                netTrdQtyLot: int
        '''
        try:
            user_data = self.broker_auth_init.red.get(username)
            user_data = self.broker_auth_init.get_data_structures(user_data)
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-portfolio auth is ' + str(e))
            raise Exception("The error in ANGEL-portfolio auth is", e)

        portfolio_df = self.get_portfolio_df(username)

        path_posord = '/app/BrokerData/PosOrd/' + str(datetime.datetime.today().date()) + '/'
        if not os.path.isdir(path_posord): os.mkdir(path_posord)
        _path_ = path_posord + username + '_prt.pkl'
        try:
            lst_tm = datetime.datetime.fromtimestamp(os.path.getmtime(_path_))
            if (datetime.datetime.now() - datetime.timedelta(seconds = .5)) < lst_tm:
                self.broker_auth_init.logger.info(username + ': Returning portfolio_df without api call as the call is within 0.5s')
                return portfolio_df
        except:
            time.sleep(np.random.rand()+1)
        i = 0
        while True:
            try:
                portfolio = pd.DataFrame(broker_obj.allholding()['data']['holdings'])
                if portfolio.shape[0] == 0:
                    df = pd.DataFrame(columns=['averageStockPrice', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'item_name'])
                    portfolio_df = df.copy()
                    while True:
                        try:
                            portfolio_df.to_pickle(_path_)
                            break
                        except Exception as e:
                            self.broker_auth_init.logger.error(username + ': Error in writing portfolio_df: ' + str(e))
                            time.sleep(np.random.rand())
                    self.broker_auth_init.red.set(username + '_portfolio_df', portfolio_df.to_json())
                    return portfolio_df
                df = portfolio[['averageprice', 'symboltoken', 'ltp', 'quantity']]
                df.columns = ['averageStockPrice', 'instrumentToken', 'lastPrice', 'netTrdQtyLot']
                df['item_name'] = ''
                for row in range(df.shape[0]):
                    try:
                        nam, exp, strk, typ, lot = self.broker_auth_init.get_name(df.iloc[row].instrumentToken)
                        df.item_name.iloc[row] = nam
                    except:
                        pass
                df['averageStockPrice'] = pd.to_numeric(df['averageStockPrice'])
                df['lastPrice']         = pd.to_numeric(df['lastPrice'])
                df['netTrdQtyLot']      = df['netTrdQtyLot'].astype(int)
                portfolio_df = df.copy()

                portfolio_df = portfolio_df.groupby('instrumentToken').agg({'averageStockPrice': 'max', 'lastPrice':'first', 'netTrdQtyLot':'sum', 'item_name':'first'})
                portfolio_df['instrumentToken'] = portfolio_df.index
                portfolio_df.reset_index(drop=True,inplace=True)
                portfolio_df = portfolio_df[['averageStockPrice', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'item_name']]

                while True:
                    try:
                        portfolio_df.to_pickle(_path_)
                        break
                    except Exception as e:
                        self.broker_auth_init.logger.error(username + ': Error in writing portfolio_df: ' + str(e))
                        time.sleep(np.random.rand())
                self.broker_auth_init.red.set(username + '_portfolio_df', portfolio_df.to_json())
                return portfolio_df
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-portfolio is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-portfolio is", e)