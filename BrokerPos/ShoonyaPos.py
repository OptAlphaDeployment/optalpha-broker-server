from NorenRestApiPy.NorenApi import NorenApi
from BrokerAuthInit import BrokerAuthInit
from BrokerPos import BrokerPos
import pandas as pd
import numpy as np
import datetime
import time
import os

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

class ShoonyaPos(BrokerPos):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init
        super().__init__(broker_auth_init)

    def positions(self, username:str) -> pd.DataFrame:
        ''' Returns dataframe with info on open positions, and today's closed positions

            Returns
            -------
            dataframe with following columns:
                actualPNL: float
                averageStockPrice: float
                expiryDate: str
                    example- "13JAN22"
                instrumentToken: str
                lastPrice: float
                netTrdQtyLot: int
                optionType: str - "CE" / "PE"
        '''
        try:
            user_data = self.broker_auth_init.red.get(username)
            user_data = self.broker_auth_init.get_data_structures(user_data)
            broker_obj = ShoonyaApiPy()
            broker_obj._NorenApi__username = user_data['auth']['_NorenApi__username']
            broker_obj._NorenApi__accountid = user_data['auth']['_NorenApi__accountid']
            broker_obj._NorenApi__password = user_data['auth']['_NorenApi__password']
            broker_obj._NorenApi__susertoken = user_data['auth']['_NorenApi__susertoken']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-positions auth is ' + str(e))
            raise Exception("The error in SHOONYA-positions auth is", e)

        positions_df = self.get_positions_df(username)

        path_posord = '/app/BrokerData/PosOrd/' + str(datetime.datetime.today().date()) + '/'
        if not os.path.isdir(path_posord): os.mkdir(path_posord)
        _path_ = path_posord + username + '_pos.pkl'
        try:
            lst_tm = datetime.datetime.fromtimestamp(os.path.getmtime(_path_))
            if (datetime.datetime.now() - datetime.timedelta(seconds = .5)) < lst_tm:
                self.broker_auth_init.logger.info(username + ': Returning positions_df without api call as the call is within .5s')
                return positions_df
        except:
            time.sleep(np.random.rand()+1)
        i = 0
        while True:
            try:
                positions = pd.DataFrame(broker_obj.get_positions())
                if positions.shape[0] == 0:
                    df = pd.DataFrame(columns=['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType', 'item_name', 'strk'])
                    positions_df = df.copy()
                    while True:
                        try:
                            positions_df.to_pickle(_path_)
                            break
                        except Exception as e:
                            self.broker_auth_init.logger.error(username + ': Error in writing positions_df: ' + str(e))
                            time.sleep(np.random.rand())
                    self.update_position_update_time(username)
                    self.broker_auth_init.red.set(username + '_positions_df', positions_df.to_json())
                    return positions_df
                df = positions[['rpnl', 'netavgprc', 'tsym', 'lp', 'netqty']]
                df.columns = ['actualPNL', 'averageStockPrice', 'instrumentToken', 'lastPrice', 'netTrdQtyLot']
                df.loc[positions.netqty.astype(int)>0, 'averageStockPrice'] = positions.daybuyavgprc
                df.loc[positions.netqty.astype(int)<0, 'averageStockPrice'] = positions.daysellavgprc
                df['item_name'] = ''
                df['strk'] = ''
                df['expiryDate'] = ''
                df['optionType'] = ''
                for row in range(df.shape[0]):
                    try:
                        nam, exp, strk, typ, lot = self.broker_auth_init.get_name(df.iloc[row].instrumentToken)
                        df.item_name.iloc[row] = nam
                        df.strk.iloc[row] = strk
                        df.expiryDate.iloc[row] = exp
                        df.optionType.iloc[row] = typ
                    except:
                        pass
                df = df[['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType', 'item_name', 'strk']]
                df['actualPNL']         = pd.to_numeric(df['actualPNL'])
                df['averageStockPrice'] = pd.to_numeric(df['averageStockPrice'])
                df['lastPrice']         = pd.to_numeric(df['lastPrice'])
                df['netTrdQtyLot']      = df['netTrdQtyLot'].astype(int)
                positions_df = df.copy()

                positions_df = positions_df.groupby('instrumentToken').agg({'actualPNL': 'sum', 'averageStockPrice': 'max', 'expiryDate':'first', 'lastPrice':'first', 'netTrdQtyLot':'sum', 'optionType':'first', 'item_name':'first', 'strk':'first'})
                positions_df['instrumentToken'] = positions_df.index
                positions_df.reset_index(drop=True,inplace=True)
                positions_df = positions_df[['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType', 'item_name', 'strk']]

                while True:
                    try:
                        positions_df.to_pickle(_path_)
                        break
                    except Exception as e:
                        self.broker_auth_init.logger.error(username + ': Error in writing positions_df: ' + str(e))
                        time.sleep(np.random.rand())
                self.update_position_update_time(username)
                self.broker_auth_init.red.set(username + '_positions_df', positions_df.to_json())
                return positions_df
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-positions is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in SHOONYA-positions is", e)