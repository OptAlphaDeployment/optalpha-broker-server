from BrokerAuthInit import BrokerAuthInit
from BrokerTrade import BrokerTrade
from BrokerOrd import BrokerOrd
from typing import Union
import urllib.parse
import pandas as pd
import datetime
import requests
import json
import time

class KotakneoTrade(BrokerTrade):
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
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_available_cash auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-get_available_cash auth is", e)

        i = 0
        while True:
            try:
                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }
                cash_data = { "brkName": "Kotak",
                            "brnchId": "ONLINE",
                            "exSeg": "nse_cm",
                            "prc": "0",
                            "prcTp": "L",
                            "prod": "NRML",
                            "qty": "1",
                            "tok": "3045",
                            "trnsTp": "B"
                            }
                cash_data = json.dumps(cash_data)
                cash_data = urllib.parse.quote(cash_data)
                cash_data = f"jData={cash_data}"

                cash = requests.post(user_data['auth']["base_url"] + f'/quick/user/check-margin', headers=headers, data=cash_data).json()
                return float(cash['avlCash'])
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_available_cash is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception('The error in KOTAKNEO-get_available_cash is', e)

    def get_required_margin(self, username:str, transaction_type:str, token:str, price_:float, product:str = '') -> float:
        ''' Return required_margin

            Parameters
            ----------
            transaction_type: str
                'BUY'/'SELL'
            token: str
                unique identifier of an item in angel
                If token is given, all other items are not needed
            price_: float
                price
            product: str
                'MARGIN' / 'INTRADAY' / ''

            Returns
            -------
            required_margin: float
        '''
        try:
            user_data = self.broker_auth_init.red.get(username)
            user_data = self.broker_auth_init.get_data_structures(user_data)
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_required_margin auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-get_required_margin auth is", e)

        i = 0
        while True:
            try:
                _nam_, _exp_, _strk_, _typ_, _lot_ = self.broker_auth_init.get_name(token)
                token_ = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.instrumentToken == token].instrumentToken_.iloc[0]

                prod = "CNC" if _exp_ == "" else "NRML"
                if product == 'INTRADAY': prod = "MIS"

                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }
                cash_data = { "brkName": "Kotak",
                            "brnchId": "ONLINE",
                            "exSeg": "nse_cm" if _exp_ == "" else "nse_fo",
                            "prc": str(price_),
                            "prcTp": "L",
                            "prod": prod,
                            "qty": "1" if _lot_ == "" else str(_lot_),
                            "tok": str(token_),
                            "trnsTp": "S" if transaction_type == "SELL" else "B"
                            }
                cash_data = json.dumps(cash_data)
                cash_data = urllib.parse.quote(cash_data)
                cash_data = f"jData={cash_data}"

                cash = requests.post(user_data['auth']["base_url"] + f'/quick/user/check-margin', headers=headers, data=cash_data).json()
                return float(cash['ordMrgn'])
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_required_margin is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception('The error in KOTAKNEO-get_required_margin is', e)

    def get_quote(self, username:str, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '') -> pd.DataFrame:
        ''' Return ltp, open and last_traded_time of given token or (name+exchange+expiry+strike+optionType) in a dataframe.
            As of 13JAN22, quote of indices is not available in kotakneo.

            Parameters
            ----------
            token: str
                unique identifier of an item in kotakneo
                If token is given, all other items are not needed
            name: str
                name of the underlying item
                example: 'SBIN'
            exchange: str: 'NSE' / 'NFO'
                'NFO' to get data for options
                'NSE' to get data for cash segment
            expiry: str
                weekly / monthly expiry date in format - '13JAN22' if this is used for an option, else- ''
            strike: str
                strike price if this is used for an option, else- ''
            optionType: str
                'CE'/'PE' if this is used for an option, else- ''

            Returns
            -------
            dataframe containing ltp, open and last_traded_time
                ltp: float
                open: float
                last_traded_time: str
                    example: '10-01-2022  12:39:21 PM'
        '''
        try:
            user_data = self.broker_auth_init.red.get(username)
            user_data = self.broker_auth_init.get_data_structures(user_data)
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_quote auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-get_quote auth is", e)

        i = 0
        while True:
            try:
                if token == '': token = self.broker_auth_init.get_token(name = name, exchange = exchange, expiry = expiry, strike = strike, optionType = optionType)
                _exp_ = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.instrumentToken == token].expiry.iloc[0]
                token_ = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.instrumentToken == token].instrumentToken_.iloc[0]

                headers = {
                    'Authorization': user_data['file']['token']
                }
                ltps = requests.get(user_data['auth']["base_url"] + f'/script-details/1.0/quotes/neosymbol/{("nse_cm" if _exp_ == "" else "nse_fo") + "%7C" + str(token_)}/all', headers=headers).json()

                data = pd.DataFrame({'ltp':ltps[0]['ltp'], 'open_price':ltps[0]['ohlc']['open'], 'BD_last_traded_time':ltps[0]['lstup_time']}, index=[0])
                return data
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-get_quote is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception('The error in KOTAKNEO-get_quote is', e)

    def place_order(self, username:str, transaction_type:str, price_:float, quantity:int, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '',  trigger:float = 0, product:str = '') -> str:
        ''' Place an order in kotakneo for the user session saved in user_data
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
                unique identifier of an item in kotakneo
                If token is given, (name, exchange, expiry, strike, optionType) are not needed
            name: str
                name of the underlying item
                example: 'SBIN'
            exchange: str: 'NSE' / 'NFO'
                'NFO' to get data for options
                'NSE' to get data for cash segment
            expiry: str
                weekly / monthly expiry date in format - '13JAN22' if this is used for an option, else- ''
            strike: str
                strike price if this is used for an option, else- ''
            optionType: str
                'CE'/'PE' if this is used for an option, else- ''
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
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-placeorder auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-placeorder auth is", e)

        i = 0
        _order_ = ''
        if quantity == 0:
            self.broker_auth_init.logger.error(username + ': ATTENTION: KOTAKNEO-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            self.broker_auth_init.print_to_chat(username, 'ATTENTION: KOTAKNEO-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            return ''

        while True:
            try:
                if token == '': token = self.broker_auth_init.get_token(name = name, exchange = exchange, expiry = expiry, strike = strike, optionType = optionType)
                else: exchange = 'NSE' if self.broker_auth_init.tokens_df[self.broker_auth_init.tokens_df.instrumentToken == token].expiry.iloc[0] == '' else 'NFO'
                order_type = 'L'
                if trigger != 0: order_type = 'SL'
                if price_ == 0: order_type = 'MKT'

                prod = "CNC" if exchange == "NSE" else "NRML"
                if product == 'INTRADAY': prod = "MIS"

                place_data = {
                    'dq':'0',
                    'es': 'nse_fo' if exchange == 'NFO' else 'nse_cm',
                    'mp':'0',
                    'pc': prod,
                    'pr': str(self.broker_auth_init.round_to(price_)),
                    'pt': order_type,
                    'qt': str(int(quantity)),
                    'rt': 'DAY',
                    'tp': str(self.broker_auth_init.round_to(trigger)),
                    'ts': token,
                    'tt': 'S' if transaction_type == 'SELL' else 'B'
                }
                place_data = json.dumps(place_data)
                place_data = urllib.parse.quote(place_data)
                place_data = f"jData={place_data}"

                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }
                _order_ = requests.post(user_data['auth']["base_url"] + f'/quick/order/rule/ms/place', headers=headers, data=place_data).json()

                order_id = _order_['nOrdNo']
                time.sleep(2.5)
                # try: df = self.broker_ord.orders(username) # check for order status
                # except Exception as e: raise Exception('ATTENTION: The error in KOTAKNEO-placeorder: fetching orderbook unsuccessful: unable to check the status', e)
                # df = df[(df.orderId == str(order_id))]
                # if df.shape[0] == 0: raise Exception('ATTENTION: The error in KOTAKNEO-placeorder: order not placed orderId: ', str(order_id))
                # if df.status.iloc[0] == 'REJ': raise Exception('ATTENTION: The error in KOTAKNEO-placeorder: order rejected orderId: ', str(order_id))
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                try:
                    try: df = self.broker_ord.orders(username) # upon error, we check if the order is already placed in last 2mins (assuming 2 orders of same token won't be placed within 2mins)
                    except Exception as e2: raise Exception('ATTENTION: The error in KOTAKNEO-placeorder: fetching orderbook unsuccessful: order is placed, manually cancel / exit the order', e, e2)
                    df = df[(df.instrumentToken == token) & (df.transactionType == transaction_type) & (df.status != 'CAN')]
                    df.sort_values(by='orderTimestamp', ascending=False, inplace=True)
                    for row_ in range(df.shape[0]): # will go in only if len(df) > 0 
                        if datetime.datetime.strptime(df.orderTimestamp.iloc[row_], '%b %d %Y %H:%M:%S:%f%p') > (datetime.datetime.now()-datetime.timedelta(minutes=2)):
                            if price_*.95 < float(df.price.iloc[row_]) < price_*1.05: # price of executed order should be +/- 5% of the intended order price
                                self.broker_auth_init.logger.error(username + ': ATTENTION: the order was placed with error in kotakneo-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                self.broker_auth_init.print_to_chat(username, 'ATTENTION: the order was placed with error in kotakneo-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                return str(df.orderId.iloc[row_]) # return order_id if the orderId is found else sleep for 1 sec and try to place again
                except Exception as e1:
                    self.broker_auth_init.logger.error(username + ': ATTENTION: The error in KOTAKNEO-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                    self.broker_auth_init.print_to_chat(username, 'ATTENTION: The error in KOTAKNEO-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-placeorder is ' + str(e) + ' ' + str(_order_) + ' Retrying.....')
                self.broker_auth_init.print_to_chat(username, 'The error in KOTAKNEO-placeorder is ' + str(e) + ' ' + str(_order_) + ' Retrying.....')
                time.sleep(1)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    if _order_['error'][0]['code'] in ['91013']:
                        self.broker_auth_init.logger.error(username + ': ATTENTION: ' + str(e) + ' ' + str(_order_))
                        self.broker_auth_init.print_to_chat(username, 'ATTENTION: ' + str(e) + ' ' + str(_order_))
                    raise Exception('The error in KOTAKNEO-placeorder is', e, _order_)

    def modify_order(self, username:str, order_id:str,  price:float, quantity:Union[int, str] = '', trigger:float = 0) -> str:
        ''' Modify a given order_id in kotakneo for immidiate execution of the order. Do not use for trailing SL due to 999113 error handling.
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
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-modifyorder auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-modifyorder auth is", e)

        i = 0
        _order_ = ''
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        # token = order.instrumentToken.iloc[0]
        # _nam_, _exp_, _strk_, _typ_, _lot_ = get_name(token)
        _nam_ = order.item_name.iloc[0]
        _exp_ = order.exp.iloc[0]
        _strk_ = order.strk.iloc[0]
        _typ_ = order.optionType.iloc[0]
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF', 'NEWF', 'CHRF', 'CNRF']: #print_logger('Rechecked Current Status of the Order:', order.status.iloc[0])
            #print_logger('Order:', order_id, 'can not be modified as the status is-', order.status.iloc[0])
            return str(order_id)
        while True:
            try:
                if quantity=='':
                    orders_df_ = self.broker_ord.orders(username)
                    order = orders_df_[orders_df_.orderId == order_id]
                    quantity = order.pendingQuantity.iloc[0]

                modify_data = {
                "mp": "0",
                "dq": "0",
                "vd": "DAY",
                "pr": str(self.broker_auth_init.round_to(price)),
                "tp": str(self.broker_auth_init.round_to(trigger)),
                "qt": str(int(quantity)),
                "no": str(order_id),
                "pt": 'L',
                }
                modify_data = json.dumps(modify_data)
                modify_data = urllib.parse.quote(modify_data)
                modify_data = f"jData={modify_data}"
                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }
                _order_ = requests.post(user_data['auth']["base_url"] + f'/quick/order/vr/modify', headers=headers, data=modify_data).json()
                order_id = _order_['nOrdNo']
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                orders_df_ = self.broker_ord.orders(username)
                order = orders_df_[orders_df_.orderId == order_id]
                if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF', 'NEWF', 'CHRF', 'CNRF']: #print_logger('Rechecked Current Status of the Order:', order.status.iloc[0])
                    self.broker_auth_init.logger.error(username + ': Order: ' + str(order_id) + ' can not be modified as the status is- ' + str(order.status.iloc[0]))
                    return str(order_id)
                if order.status.iloc[0] == 'OPF': quantity = order.pendingQuantity.iloc[0]
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-modifyorder is ' + str(e) + ' ' + str(_order_) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception('The error in KOTAKNEO-modifyorder is', e, _order_)

    def cancel_order(self, username:str, order_id:str) -> str:
        ''' Cancel a given order_id in kotakneo for the user session saved in user_data.
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
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-cancelorder auth is ' + str(e))
            raise Exception("The error in KOTAKNEO-cancelorder auth is", e)

        i = 0
        _order_ = ''
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
            if order.shape[0] == 0:
                self.broker_auth_init.logger.error(username + ': ' + str(order_id) + ' order_id cannot be canceled as it does not exist')
                return str(order_id)
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF', 'NEWF', 'CHRF', 'CNRF']: #print_logger('Rechecked Current Status of the Order:', order.status.iloc[0])
            #print_logger('Order:', order_id, 'can not be cancelled as the status is-', order.status.iloc[0])
            return str(order_id)
        while True:
            try:
                cancel_data = {
                "on": str(order_id)
                }
                cancel_data = json.dumps(cancel_data)
                cancel_data = urllib.parse.quote(cancel_data)
                cancel_data = f"jData={cancel_data}"
                headers = {
                    'Auth': user_data['auth']['token'],
                    'sid': user_data['auth']['sid'],
                    'neo-fin-key': 'neotradeapi'
                }
                _order_ = requests.post(user_data['auth']["base_url"] + f'/quick/order/cancel', headers=headers, data=cancel_data).json()
                try: order_id = _order_['nOrdNo']
                except: order_id = _order_['result']
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                orders_df_ = self.broker_ord.orders(username)
                order = orders_df_[orders_df_.orderId == order_id]
                if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF', 'NEWF', 'CHRF', 'CNRF']: #print_logger('Rechecked Current Status of the Order:', order.status.iloc[0])
                    self.broker_auth_init.logger.error(username + ': Order: ' + str(order_id) + ' can not be cancelled as the status is- ' + str(order.status.iloc[0]))
                    return str(order_id)
                if i==5: user_data = self.broker_auth_init.login(user_data['file'])
                i=i+1
                self.broker_auth_init.logger.error(username + ': The error in KOTAKNEO-cancelorder is ' + str(e) + ' ' + str(_order_) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception('The error in KOTAKNEO-cancelorder is', e, _order_)