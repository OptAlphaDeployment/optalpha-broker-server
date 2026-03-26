from BrokerAuthInit import BrokerAuthInit
from BrokerOrd import BrokerOrd
import pandas as pd
import numpy as np
import datetime
import requests
import time
import os

class KotakneoOrd(BrokerOrd):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init
        super().__init__(broker_auth_init)

    def orders(self, username:str) -> pd.DataFrame:
        ''' Returns dataframe with today's order details.

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
                instrumentToken: str
        '''
        try:
            user_data = self.broker_auth_init.red.get(username)
            user_data = self.broker_auth_init.get_data_structures(user_data)
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-orders auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-orders auth is", e)

        orders_df = self.get_orders_df(username)

        path_posord = '/app/BrokerData/PosOrd/' + str(datetime.datetime.today().date()) + '/'
        if not os.path.isdir(path_posord): os.mkdir(path_posord)
        _path_ = path_posord + username + '_ord.pkl'
        try:
            lst_tm = datetime.datetime.fromtimestamp(os.path.getmtime(_path_))
            if (datetime.datetime.now() - datetime.timedelta(seconds = .25)) < lst_tm:
                self.broker_auth_init.logger.info(username + ': Returning orders_df without api call as the call is within .25s')
                return orders_df
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

                orders = requests.get(user_data['auth']["base_url"] + f'/quick/user/orders', headers=headers).json()
                if 'data' in list(orders.keys()): orders = pd.DataFrame(orders['data'])
                else:
                # if orders.shape[0] == 0:
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

                orders = orders[['nOrdNo', 'qty', 'ordDtTm', 'unFldSz', 'prc', 'avgPrc', 'ordSt', 'trdSym', 'sym', 'expDt', 'stkPrc', 'optTp', 'trnsTp']]
                orders.loc[orders.prc == '0.00', 'prc'] = orders['avgPrc']
                del orders['avgPrc']
                orders.columns = ['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'item_name', 'exp', 'strk', 'optionType', 'transactionType']

                orders['orderQuantity'] = orders.orderQuantity.astype(int)
                orders['pendingQuantity'] = orders.pendingQuantity.astype(int)
                orders['orderTimestamp'] = pd.to_datetime(orders.orderTimestamp).apply(lambda x: x.strftime('%b %d %Y %I:%M:%S:%f%p'))
                orders['price'] = orders['price'].astype(float)

                orders['strk'] = orders['strk'].astype(float)
                orders['strk'] = orders['strk'].astype(str)

                orders.loc[orders.transactionType=='B', 'transactionType'] = "BUY"
                orders.loc[orders.transactionType=='S', 'transactionType'] = "SELL"

                orders['exp'] = pd.to_datetime(orders.exp, errors='coerce').dt.strftime("%d") + pd.to_datetime(orders.exp, errors='coerce').dt.strftime("%b").str.upper() + pd.to_datetime(orders.exp, errors='coerce').dt.strftime("%y")

                orders.loc[orders.status=='open', 'status'] = "OPN" #partially filled is also 'open' in angel
                orders.loc[orders.status=='complete', 'status'] = "TRAD"
                orders.loc[orders.status=='trigger pending', 'status'] = "SLO"
                orders.loc[orders.status=='cancelled', 'status'] = "CAN"
                orders.loc[orders.status=='rejected', 'status'] = "REJ"
                orders.loc[(orders.pendingQuantity!=0)&(orders.orderQuantity!=orders.pendingQuantity)&(orders.status=='OPN'), 'status'] = "OPF"
                orders.loc[orders.status=='REJ', 'pendingQuantity'] = orders.orderQuantity

                df = orders.copy()
                df.orderId = df.orderId.astype(str)
                df.instrumentToken = df.instrumentToken.astype(str)
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
                try:
                    if int(e.status) == 555:
                        self.broker_auth_init.logger.error(username + ': NEW CHECK: The error in KOTAKNEO-orders is ' + str(e) + ' Waiting for 1 min.')
                        time.sleep(np.random.randint(40,60))
                except: pass
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-orders is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in KOTAKNEO-orders is", e)