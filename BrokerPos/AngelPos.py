from BrokerAuthInit import BrokerAuthInit
from BrokerPos import BrokerPos
from SmartApi import SmartConnect
import pandas as pd
import numpy as np
import datetime
import time
import os

class AngelPos(BrokerPos):
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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-positions auth is ' + str(e))
            raise Exception("The error in ANGEL-positions auth is", e)

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
                positions = pd.DataFrame(broker_obj.position()['data'])
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
                df = positions[['realised', 'netprice', 'expirydate', 'symboltoken', 'ltp', 'netqty', 'optiontype']]
                df.columns = ['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType']
                df.loc[positions.netqty.astype(int)>0, 'averageStockPrice'] = positions.totalbuyavgprice
                df.loc[positions.netqty.astype(int)<0, 'averageStockPrice'] = positions.totalsellavgprice
                # df['averageStockPrice'] = np.where(positions.netqty.astype(int)>0, positions.totalbuyavgprice, positions.totalsellavgprice)
                df['item_name'] = ''
                df['strk'] = ''
                for row in range(df.shape[0]):
                    try:
                        nam, exp, strk, typ, lot = self.broker_auth_init.get_name(df.iloc[row].instrumentToken)
                        df.item_name.iloc[row] = nam
                        df.strk.iloc[row] = strk
                    except:
                        pass
                df = df[['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType', 'item_name', 'strk']]
                df['actualPNL']         = pd.to_numeric(df['actualPNL'])
                df['averageStockPrice'] = pd.to_numeric(df['averageStockPrice'])
                df['expiryDate']        = df.expiryDate.str[:-4] + df.expiryDate.str[-2:] # correction: expiry format from 27JAN2022 to 27JAN22
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
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-positions is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-positions is", e)