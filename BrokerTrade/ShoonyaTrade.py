from NorenRestApiPy.NorenApi import NorenApi
from BrokerAuthInit import BrokerAuthInit
from BrokerTrade import BrokerTrade
from BrokerOrd import BrokerOrd
from typing import Union
import pandas as pd
import numpy as np
import datetime
import requests
import time
import os

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

class ShoonyaTrade(BrokerTrade):
    def __init__(self, broker_auth_init:BrokerAuthInit, broker_ord:BrokerOrd) -> None:
        self.broker_auth_init = broker_auth_init
        self.broker_ord = broker_ord
        super().__init__()

    def get_available_cash(self, username:str) -> float:
        ''' Return available_cash

            Returns
            -------
            available_cash: float
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-get_available_cash auth is ' + str(e))
            raise Exception("The error in SHOONYA-get_available_cash auth is", e)

        i = 0
        while True:
            try:
                limits = broker_obj.get_limits()
                try:
                    return float(limits['cash']) - float(limits['marginused'])
                except:
                    return float(limits['cash'])
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-get_available_cash is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in SHOONYA-get_available_cash is", e)

    def get_required_margin(self, username:str, transaction_type:str, token:str, price_:float, product:str = '') -> float:
        _nam_, _exp_, _strk_, _typ_, _lot_ = self.broker_auth_init.get_name(token)
        _exp_ = datetime.datetime.strptime(_exp_, "%d%b%y").strftime("%d-%m-%Y")
        transaction_type = 'buy' if transaction_type == "BUY" else 'sell'
        if _typ_ == "":
            return self.get_quote(username, token=token).ltp.iloc[0]
        elif _typ_ == "FUT":
            url = f"https://margin.truedata.in/api/getfuturemargin?symbol={_nam_}&expiry={_exp_}&buysell={transaction_type}&response=json"
        else:
            _strk_ = float(_strk_)
            url = f"https://margin.truedata.in/api/getoptionmargin?symbol={_nam_}&expiry={_exp_}&strike={_strk_}&series={_typ_}&buysell={transaction_type}&response=json"
        res = requests.get(url).json()
        return float(res['span']) + float(res['exposure'])

    def get_quote(self, username:str, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '') -> pd.DataFrame:
        ''' Return ltp, open and last_traded_time (is still pending) of given token or (name+exchange+expiry+strike+optionType) in a dataframe.

            Parameters
            ----------
            token: str
                unique identifier of an item in shoonya
                If token is given, all other items are not needed
            name: str
                name of the underlying item
                example: 'SBIN'
            exchange: str: 'NSE' / 'NFO'
                'NFO' to get data for options
                'NSE' to get data for cash segment
            expiry: str
                weekly / monthly expiry date in format - "13JAN22" if this is used for an option, else- ''
            strike: str
                strike price if this is used for an option, else- ''
            optionType: str
                "CE"/"PE" if this is used for an option, else- ''

            Returns
            -------
            dataframe containing ltp, open and last_traded_time
                ltp: float
                open: float
                last_traded_time: str
                    example: "10-01-2022  12:39:21 PM"
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-get_quote auth is ' + str(e))
            raise Exception("The error in SHOONYA-get_quote auth is", e)

        i = 0
        while True:
            try:
                if token == '':
                    token = self.broker_auth_init.get_token(name = name, exchange = exchange, expiry = expiry, strike = strike, optionType = optionType)
                elif token != '':
                    if self.broker_auth_init.get_name(token)[1] == '': exchange = 'NSE'
                    elif self.broker_auth_init.get_name(token)[1] != '': exchange = 'NFO'
                token = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.instrumentToken==token].instrumentToken_.iloc[0]
                data = broker_obj.get_quotes(exchange, token)
                ltp_ = data['lp']
                try:
                    open_ = data['o']
                except:
                    open_ = ''
                try:
                    ltt_ = data['ltt']
                except:
                    ltt_ = ''
                data = pd.DataFrame({'ltp':ltp_,  'open_price':open_, 'BD_last_traded_time':ltt_}, index=[0])
                return data
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-get_quote is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in SHOONYA-get_quote is", e)

    def place_order(self, username:str, transaction_type:str, price_:float, quantity:int, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '',  trigger:float = 0, product:str = '') -> str:
        ''' Place an order in fyesr for the user session saved in broker_obj
            All normal orders are supported: buy / sell / SL / target / carry-forward 
            Give token or (name, exchange, expiry, strike, optionType) to identify the item for which the order is to be placed

            Parameters
            ----------
            transaction_type: str
                'BUY'/'SELL'
            price_: float
                price
            quantity: int
                Quantity
            token: str
                unique identifier of an item in shoonya
                If token is given, (name, exchange, expiry, strike, optionType) are not needed
            name: str
                name of the underlying item
                example: 'SBIN'
            exchange: str: 'NSE' / 'NFO'
                'NFO' to get data for options
                'NSE' to get data for cash segment
            expiry: str
                weekly / monthly expiry date in format - "13JAN22" if this is used for an option, else- ''
            strike: str
                strike price if this is used for an option, else- ''
            optionType: str
                "CE"/"PE" if this is used for an option, else- ''
            trigger: float
                used when placing sl / target order
            product: str
                'MARGIN' / 'INTRADAY' / ''

            Returns
            -------
                order_id: str
                    order_id of the placed order
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-placeorder auth is ' + str(e))
            raise Exception("The error in SHOONYA-placeorder auth is", e)

        i = 0
        if quantity == 0:
            self.broker_auth_init.logger.error(username + ': ATTENTION: SHOONYA-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            self.broker_auth_init.print_to_chat(username, 'ATTENTION: SHOONYA-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            return ''

        if token == '': 
            token = self.broker_auth_init.get_token(name = name, exchange = exchange, expiry = expiry, strike = strike, optionType = optionType)
        elif token != '':
            if self.broker_auth_init.get_name(token)[1] == '': exchange = 'NSE'
            elif self.broker_auth_init.get_name(token)[1] != '': exchange = 'NFO'

        if transaction_type == 'BUY': transaction_type_ = 'B'
        elif transaction_type == 'SELL': transaction_type_ = 'S'

        type_ = 'LMT'
        if price_ == 0: type_ = 'MKT'
        if trigger != 0: type_ = 'SL-LMT'

        product_type = 'M' if exchange == 'NFO' else 'C'
        if product == 'INTRADAY': product_type = 'I'

        while True:
            try:
                _order_ = broker_obj.place_order(buy_or_sell=transaction_type_,product_type=product_type,exchange=exchange,
                            tradingsymbol=str(token),quantity=int(quantity),discloseqty=0,price_type=type_, price=self.broker_auth_init.round_to(price_),
                            trigger_price=self.broker_auth_init.round_to(trigger),retention='DAY', remarks='')

                order_id = _order_['norenordno']
                if (_order_['norenordno'] == '') or (_order_['stat'] != 'Ok'):
                    raise Exception("The error in SHOONYA-placeorder is")
                time.sleep(2.5)
                # try: df = self.broker_ord.orders(username) # check for order status
                # except Exception as e: raise Exception('ATTENTION: The error in SHOONYA-placeorder: fetching orderbook unsuccessful: unable to check the status', e)
                # df = df[(df.orderId == str(order_id))]
                # if df.shape[0] == 0: raise Exception('ATTENTION: The error in SHOONYA-placeorder: order not placed orderId: ', str(order_id))
                # if df.status.iloc[0] == 'REJ': raise Exception('ATTENTION: The error in SHOONYA-placeorder: order rejected orderId: ', str(order_id))
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                try:
                    try: df = self.broker_ord.orders(username) # upon error, we check if the order is already placed in last 2mins (assuming 2 orders of same token won't be placed within 2mins)
                    except Exception as e3: raise Exception('ATTENTION: The error in SHOONYA-placeorder: fetching orderbook unsuccessful: order is placed, manually cancel / exit the order', e, e3)
                    df = df[(df.instrumentToken == token) & (df.transactionType == transaction_type) & (df.status != 'CAN')]
                    df.sort_values(by='orderTimestamp', ascending=False, inplace=True)
                    for row_ in range(df.shape[0]): # will go in only if len(df) > 0 
                        if datetime.datetime.strptime(df.orderTimestamp.iloc[row_], '%b %d %Y %H:%M:%S:%f%p') > (datetime.datetime.now()-datetime.timedelta(minutes=2)):
                            if price_*.95 < float(df.price.iloc[row_]) < price_*1.05: # price of executed order should be +/- 5% of the intended order price
                                self.broker_auth_init.logger.error(username + ': ATTENTION: the order was placed with error in shoonya-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                self.broker_auth_init.print_to_chat(username, 'ATTENTION: the order was placed with error in shoonya-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                return str(df.orderId.iloc[row_]) # return order_id if the orderId is found else sleep for 1 sec and try to place again
                except Exception as e1:
                    self.broker_auth_init.logger.error(username + ': ATTENTION: The error in SHOONYA-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                    self.broker_auth_init.print_to_chat(username, 'ATTENTION: The error in SHOONYA-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-placeorder is ' + str(e) + ' ' + str(_order_) + ' Retrying.....')
                self.broker_auth_init.print_to_chat(username, 'The error in SHOONYA-placeorder is ' + str(e) + ' ' + str(_order_) + ' Retrying.....')
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in SHOONYA-placeorder is", _order_, e)

    def modify_order(self, username:str, order_id:str,  price:float, quantity:Union[int, str] = '', trigger:float = 0) -> str:
        ''' Modify a given order_id in shoonya for immidiate execution of the order. Do not use for trailing SL due to 999113 error handling.
            Should be only used when the order status is OPN / SLO / OPF. 
            All orders are supported: buy / sell / SL / target / carry-forward.

            Parameters
            ----------
            order_id: str
                order_id to modify
            price: float
                Updated price
            quantity: int / ''
                Updated Quantity
                if '' pendingQuantity will be fetched from orders
            trigger: float
                Updated trigger to be used in sl / target  order
                0 in case of other orders

            Returns
            -------
                order_id: str
                    order_id of the placed order
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-modifyorder auth is ' + str(e))
            raise Exception("The error in SHOONYA-modifyorder auth is", e)

        i = 0
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: # print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
            # print_logger('Order:', order_id, 'can not be modified as the status is-', order.status.iloc[0])
            return str(order_id)

        token = order.instrumentToken.iloc[0]

        if self.broker_auth_init.get_name(token)[1] == '': exchange = 'NSE'
        elif self.broker_auth_init.get_name(token)[1] != '': exchange = 'NFO'

        type_ = 'LMT'
        if price == 0: type_ = 'MKT'
        if trigger != 0: type_ = 'SL-LMT'

        while True:
            try:
                if quantity=='':
                    orders_df_ = self.broker_ord.orders(username)
                    order = orders_df_[orders_df_.orderId == order_id]
                    quantity = order.pendingQuantity.iloc[0]

                _order_ = broker_obj.modify_order(exchange=str(exchange), tradingsymbol=str(token), orderno=str(order_id), newquantity=int(quantity), newprice_type=type_, newprice=self.broker_auth_init.round_to(price), newtrigger_price= self.broker_auth_init.round_to(trigger))
                
                order_id = _order_['result']
                if (_order_['result'] == '') or (_order_['stat'] != 'Ok'):
                    raise Exception("The error in SHOONYA-modifyorder is")
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                orders_df_ = self.broker_ord.orders(username)
                order = orders_df_[orders_df_.orderId == order_id]
                if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: # print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
                    self.broker_auth_init.logger.error(username + ': Order: ' + str(order_id) + ' can not be modified as the status is- ' + str(order.status.iloc[0]))
                    return str(order_id)
                if order.status.iloc[0] == 'OPF': quantity = order.pendingQuantity.iloc[0]
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-modifyorder is ' + str(e) + ' ' + str(_order_) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception("The error in SHOONYA-modifyorder is", _order_, e)

    def cancel_order(self, username:str, order_id:str) -> str:
        ''' Cancel a given order_id in shoonya for the user session saved in broker_obj.
            Should be only used when the order status is OPN / SLO.
            All orders are supported: buy / sell / SL / target / carry-forward.

            Parameters
            ----------
            order_id: str
                order_id to modify

            Returns
            -------
                order_id: str
                    order_id of the cancelled order
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
            self.broker_auth_init.logger.error(username + ': The error in SHOONYA-cancelorder auth is ' + str(e))
            raise Exception("The error in SHOONYA-cancelorder auth is", e)

        i = 0
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
            if order.shape[0] == 0:
                self.broker_auth_init.logger.error(username + ': ' + str(order_id) + ' order_id cannot be canceled as it does not exist')
                return str(order_id)
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: #print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
            #print_logger('Order:', order_id, 'can not be cancelled as the status is-', order.status.iloc[0])
            return str(order_id)
        while True:
            try:
                _order_ = broker_obj.cancel_order(str(order_id))
                order_id = _order_['result']

                if (_order_['result'] == '') or (_order_['stat'] != 'Ok'):
                    raise Exception("The error in SHOONYA-cancelorder is")
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                orders_df_ = self.broker_ord.orders(username)
                order = orders_df_[orders_df_.orderId == order_id]
                if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: #print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
                    self.broker_auth_init.logger.error(username + ': Order: ' + str(order_id) + ' can not be cancelled as the status is- ' + str(order.status.iloc[0]))
                    return str(order_id)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                self.broker_auth_init.logger.error(username + ': The error in SHOONYA-cancelorder is ' + str(e) + ' ' + str(_order_) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception("The error in SHOONYA-cancelorder is", _order_, e)