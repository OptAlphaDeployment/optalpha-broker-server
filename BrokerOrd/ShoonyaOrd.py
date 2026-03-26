from NorenRestApiPy.NorenApi import NorenApi
from BrokerAuthInit import BrokerAuthInit
from BrokerOrd import BrokerOrd
import pandas as pd
import numpy as np
import datetime
import time
import os

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

class ShoonyaOrd(BrokerOrd):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init
        super().__init__(broker_auth_init)

    def orders(self, username:str) -> pd.DataFrame:
        ''' Returns dataframe with today's order details and orders_df is created.

            Returns
            -------
            dataframe with following columns:
                orderId: str
                orderQuantity: int
                orderTimestamp: str
                    example: "Jan 11 2022 09:28:20:000000AM"
                pendingQuantity: int
                    it is non-zero only when status is OPF 
                price: float
                status: str
                    OPN: open order generally when the limit-price is not met by the market, or liquidity is not there
                    TRAD: when the order is traded / fully-filled
                    SLO: open SL order, when the SL trigger price is not breached by the market 
                    CAN: cancelled
                    OPF: open partially filled order; partial quantity is traded and remaining is left as OPN (remaining is indicated by pendingQuantity)
                    rejected: if the order is rejected by broker / exchange: not handled as of 18JAN22
                instrumentToken: str
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-orders auth is ' + str(e))
            raise Exception("The error in SHOONYA-orders auth is", e)

        orders_df = self.get_orders_df(username)

        path_posord = '/app/BrokerData/PosOrd/' + str(datetime.datetime.today().date()) + '/'
        if not os.path.isdir(path_posord): os.mkdir(path_posord)
        _path_ = path_posord + username + '_ord.pkl'
        try:
            lst_tm = datetime.datetime.fromtimestamp(os.path.getmtime(_path_))
            if (datetime.datetime.now() - datetime.timedelta(seconds = .5)) < lst_tm:
                self.broker_auth_init.logger.info(username + ': Returning orders_df without api call as the call is within .5s')
                return orders_df
        except:
            time.sleep(np.random.rand()+1)
        i = 0
        while True:
            try:
                orders = pd.DataFrame(broker_obj.get_order_book())
                if orders.shape[0] == 0:
                    df = pd.DataFrame(columns=['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'item_name', 'exp', 'strk', 'optionType', 'transactionType'])
                    orders_df = df.copy()
                    while True:
                        try:
                            orders_df.to_pickle(_path_)
                            break
                        except Exception as e:
                            self.broker_auth_init.logger.error(username + ': Error in writing orders_df: ' + str(e))
                            time.sleep(np.random.rand())                          
                    self.update_order_update_time(username)
                    self.broker_auth_init.red.set(username + '_orders_df', orders_df.to_json())
                    return orders_df

                for nam in ['norenordno', 'qty', 'norentm', 'fillshares', 'avgprc', 'status', 'tsym', 'trantype', 'prctyp']:
                    try:
                        orders[nam].iloc[0]
                    except:
                        orders[nam] = 0
                orders['qty'] = orders['qty'].astype(int)
                orders['fillshares'] = orders['fillshares'].fillna(0).astype(int)
                orders['avgprc'] = orders['avgprc'].astype(float)

                orders['remainingQuantity'] = orders['qty'] - orders['fillshares']
                del orders['fillshares']
                df = orders[['norenordno', 'qty', 'norentm', 'remainingQuantity', 'avgprc', 'status', 'tsym', 'trantype', 'prctyp']].copy()
                df.columns = ['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'transactionType', 'type']
                df.price.fillna(0, inplace=True)
                df.loc[df.price==0, 'price'] = orders.prc
                df['item_name'] = ''
                df['exp'] = ''
                df['strk'] = ''
                df['optionType'] = ''
                for row in range(df.shape[0]):
                    try:
                        nam, exp, strk, typ, lot = self.broker_auth_init.get_name(df.iloc[row].instrumentToken)
                        df.item_name.iloc[row] = nam
                        df.exp.iloc[row] = exp
                        df.strk.iloc[row] = strk
                        df.optionType.iloc[row] = typ
                    except:
                        pass
                df = df[['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'item_name', 'exp', 'strk', 'optionType', 'transactionType', 'type']]
                df.loc[df.transactionType=='B', 'transactionType'] = "BUY"
                df.loc[df.transactionType=='S', 'transactionType'] = "SELL"

                df.loc[df.status=='CANCELED', 'status']= "CAN"
                df.loc[df.status=='COMPLETE', 'status'] = "TRAD"
                df.loc[df.status=='REJECTED', 'status']= "REJ"

                df.loc[df.status=='PENDING', 'status']= "OPN" # this status is unknown yet

                df.loc[(df.type == 'LMT') & (df.status=='OPEN'), 'status'] = "OPN"
                df.loc[(df.type == 'SL-LMT') & ((df.status=='OPEN')|(df.status=='TRIGGER_PENDING')), 'status']= "SLO"
                df.orderQuantity, df.pendingQuantity = df.orderQuantity.astype(int), df.pendingQuantity.astype(int) 
                df.loc[(df.pendingQuantity!=0)&(df.orderQuantity!=df.pendingQuantity)&(df.status.isin(['OPN', 'OPF', 'SLO'])), 'status']= "OPF"

                df.price = pd.to_numeric(df.price)
                df.orderTimestamp = pd.to_datetime(df.orderTimestamp).apply(lambda x: x.strftime('%b %d %Y %I:%M:%S:%f%p'))
                del df['type']
                orders_df = df.copy()
                while True:
                    try:
                        orders_df.to_pickle(_path_)
                        break
                    except Exception as e:
                        self.broker_auth_init.logger.error(username + ': Error in writing orders_df: ' + str(e))
                        time.sleep(np.random.rand())   
                self.update_order_update_time(username)
                self.broker_auth_init.red.set(username + '_orders_df', orders_df.to_json())
                return orders_df
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-orders is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in SHOONYA-orders is", e)