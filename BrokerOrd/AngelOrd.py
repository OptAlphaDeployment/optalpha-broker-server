from BrokerAuthInit import BrokerAuthInit
from SmartApi import SmartConnect
from BrokerOrd import BrokerOrd
import pandas as pd
import numpy as np
import datetime
import time
import os

class AngelOrd(BrokerOrd):
    def __init__(self, broker_auth_init:BrokerAuthInit) -> None:
        self.broker_auth_init = broker_auth_init
        super().__init__(broker_auth_init)

    def orders(self, username:str) -> pd.DataFrame:
        ''' Returns dataframe with today's order details and orders_df is created with details only used while using angel api i.e:
                'tradingsymbol', 'exchange', 'variety'

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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-orders auth is ' + str(e))
            raise Exception("The error in ANGEL-orders auth is", e)

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
                orders = pd.DataFrame(broker_obj.orderBook()['data'])
                if orders.shape[0] == 0:
                    df = pd.DataFrame(columns=['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'item_name', 'exp', 'strk', 'optionType', 'transactionType'])
                    orders_df = pd.concat([df, pd.DataFrame(columns=['tradingsymbol', 'exchange', 'variety'])],1)
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
                orders = orders[~((orders.orderid=='')&(orders.updatetime==''))]
                orders.reset_index(drop=True, inplace=True)
                df = orders[['orderid', 'quantity', 'updatetime' , 'unfilledshares', 'averageprice', 'orderstatus', 'symboltoken', 'transactiontype']]
                df.columns = ['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'transactionType']
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
                df = df[['orderId', 'orderQuantity', 'orderTimestamp', 'pendingQuantity', 'price', 'status', 'instrumentToken', 'item_name', 'exp', 'strk', 'optionType', 'transactionType']]
                df.loc[df.status=='open', 'status'] = "OPN" #partially filled is also 'open' in angel
                df.loc[df.status=='complete', 'status'] = "TRAD"
                df.loc[df.status=='trigger pending', 'status']= "SLO"
                df.loc[df.status=='cancelled', 'status']= "CAN"
                df.loc[df.status=='rejected', 'status']= "REJ"
                df.loc[df.status=='modify validation pending', 'status']= "OPF"
                df.loc[df.status=='validation pending', 'status']= "NEWF"
                df.orderQuantity, df.pendingQuantity = df.orderQuantity.astype(int), df.pendingQuantity.astype(int)
                df.loc[(df.pendingQuantity!=0)&(df.orderQuantity!=df.pendingQuantity)&(df.status=='OPN'), 'status']= "OPF"
                df.price = pd.to_numeric(df.price)
                df.orderTimestamp = pd.to_datetime(df.orderTimestamp).apply(lambda x: x.strftime('%b %d %Y %I:%M:%S:%f%p'))
                orders_df = pd.concat([df, orders[['tradingsymbol', 'exchange', 'variety']]],1)
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
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-orders is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-orders is", e)