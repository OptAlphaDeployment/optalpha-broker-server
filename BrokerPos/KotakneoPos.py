from BrokerAuthInit import BrokerAuthInit
from BrokerPos import BrokerPos
import pandas as pd
import numpy as np
import datetime
import requests
import time
import os

class KotakneoPos(BrokerPos):
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
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-positions auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-positions auth is", e)

        positions_df = self.get_positions_df(username)

        path_posord = '/app/BrokerData/PosOrd/' + str(datetime.datetime.today().date()) + '/'
        if not os.path.isdir(path_posord): os.mkdir(path_posord)
        _path_ = path_posord + username + '_pos.pkl'
        try:
            lst_tm = datetime.datetime.fromtimestamp(os.path.getmtime(_path_))
            if (datetime.datetime.now() - datetime.timedelta(seconds = .25)) < lst_tm:
                self.broker_auth_init.logger.info(username + ': Returning positions_df without api call as the call is within .25s')
                return positions_df
        except: 
            time.sleep(np.random.rand()+1)
        i = 0
        while True:
            try:
                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }

                positions = requests.get(user_data['auth']["base_url"] + f'/quick/user/positions', headers=headers).json()
                if 'data' in list(positions.keys()): positions = pd.DataFrame(positions['data'])
                else:
                # if positions.shape[0] == 0:
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
                
                positions = positions[['buyAmt', 'sellAmt', 'cfBuyAmt', 'cfSellAmt', 'cfBuyQty', 'cfSellQty', 'flBuyQty', 'flSellQty', 'tok', 'exSeg', 'trdSym', 'sym', 'stkPrc', 'expDt', 'optTp', 'lotSz']]

                headers = {
                    'Authorization': user_data['file']['token']
                }
                ltps = requests.get(user_data['auth']["base_url"] + f'/script-details/1.0/quotes/neosymbol/{pd.Series(["%2C".join(positions.exSeg + "%7C" + positions.tok.astype(str))]).iloc[0]}/scrip_details', headers=headers).json()
                ltps = pd.DataFrame(ltps)[['exchange_token', 'ltp']]
                ltps.columns = ['tok', 'lastPrice']

                positions = pd.merge(left = positions, right = ltps, on = 'tok')

                positions.loc[positions.buyAmt=='0.00', 'buyAmt'] = positions.cfBuyAmt
                positions.loc[positions.sellAmt=='0.00', 'sellAmt'] = positions.cfSellAmt

                # positions.loc[positions.flBuyQty=='0', 'flBuyQty'] = positions.cfBuyQty
                # positions.loc[positions.flSellQty=='0', 'flSellQty'] = positions.cfSellQty
                positions['flBuyQty'] = (positions.cfBuyQty.astype(int) + positions.flBuyQty.astype(int)).astype(int)
                positions['flSellQty'] = (positions.cfSellQty.astype(int) + positions.flSellQty.astype(int)).astype(int)

                positions = positions[['buyAmt', 'sellAmt', 'flBuyQty', 'flSellQty', 'trdSym', 'sym', 'stkPrc', 'expDt', 'optTp', 'lotSz', 'lastPrice']]

                positions['buyAmt'] = positions.buyAmt.astype(float)
                positions['sellAmt'] = positions.sellAmt.astype(float)

                # positions['flBuyQty'] = positions.flBuyQty.astype(int)
                # positions['flSellQty'] = positions.flSellQty.astype(int)

                positions['stkPrc'] = positions.stkPrc.astype(float).astype(str)

                positions['expDt'] = pd.to_datetime(positions.expDt, errors='coerce').dt.strftime("%d") + pd.to_datetime(positions.expDt, errors='coerce').dt.strftime("%b").str.upper() + pd.to_datetime(positions.expDt, errors='coerce').dt.strftime("%y")

                positions['lotSz'] = positions.lotSz.astype(int)

                positions['lastPrice'] = positions.lastPrice.astype(float)

                max_qty = positions[['flBuyQty', 'flSellQty']].max(axis=1)
                remaning_qty = (positions.flBuyQty - positions.flSellQty).abs()

                positions['actualPNL'] = 0
                positions.loc[(positions.buyAmt != 0) & (positions.sellAmt != 0), 'actualPNL'] = (positions.sellAmt/positions.flSellQty - positions.buyAmt/positions.flBuyQty)*(max_qty - remaning_qty)

                positions['averageStockPrice'] = 0
                positions.loc[(positions.buyAmt == 0), 'averageStockPrice'] = positions.sellAmt/positions.flSellQty
                positions.loc[(positions.sellAmt == 0), 'averageStockPrice'] = positions.buyAmt/positions.flBuyQty

                positions['netTrdQtyLot'] = positions.flBuyQty - positions.flSellQty

                positions.rename(columns = {'expDt':'expiryDate', 'trdSym':'instrumentToken', 'optTp':'optionType', 'sym':'item_name', 'stkPrc':'strk'}, inplace = True)

                # positions_df = positions_df.groupby('instrumentToken').agg({'actualPNL': 'sum', 'averageStockPrice': 'max', 'expiryDate':'first', 'lastPrice':'first', 'netTrdQtyLot':'sum', 'optionType':'first', 'item_name':'first', 'strk':'first'})
                # positions_df['instrumentToken'] = positions_df.index
                # positions_df.reset_index(drop=True,inplace=True)
                positions_df = positions[['actualPNL', 'averageStockPrice', 'expiryDate', 'instrumentToken', 'lastPrice', 'netTrdQtyLot', 'optionType', 'item_name', 'strk']]

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
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-positions is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in KOTAKNEO-positions is", e)