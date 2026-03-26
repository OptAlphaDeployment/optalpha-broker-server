from BrokerAuthInit import BrokerAuthInit
from BrokerTrade import BrokerTrade
from SmartApi import SmartConnect
from BrokerOrd import BrokerOrd
from typing import Union
import pandas as pd
import datetime
import time

class AngelTrade(BrokerTrade):
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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_available_cash auth is ' + str(e))
            raise Exception("The error in ANGEL-get_available_cash auth is", e)

        i = 0
        while True:
            try:
                return float(broker_obj.rmsLimit()['data']['availablecash'])
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_available_cash is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-get_available_cash is", e)

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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_required_margin auth is ' + str(e))
            raise Exception("The error in ANGEL-get_required_margin auth is", e)

        i = 0
        while True:
            try:
                _nam_, _exp_, _strk_, _typ_, _lot_ = self.broker_auth_init.get_name(token)
                symbol = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.token==str(token)].symbol.iloc[0]
                exchange = 'NSE' if (symbol[-3:] == '-EQ') or (symbol in self.broker_auth_init.index_) else 'NFO'
                producttype = 'CARRYFORWARD' if exchange=='NFO' else 'DELIVERY'

                if product == 'MARGIN': producttype = 'MARGIN'
                elif product == 'INTRADAY': producttype = 'INTRADAY'

                margin = broker_obj.getMarginApi({
                    "positions": [
                            {
                                "exchange": exchange,
                                "qty": 1 if _lot_ == "" else int(_lot_),
                                "price": price_,
                                "productType": producttype,
                                "token": token,
                                "tradeType": transaction_type
                            }
                        ]
                    })

                return float(margin['data']['totalMarginRequired'])
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_required_margin is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-get_required_margin is", e)

    def get_quote(self, username:str, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '') -> pd.DataFrame:
        ''' Return ltp, open and last_traded_time (is still pending) of given token or (name+exchange+expiry+strike+optionType) in a dataframe.

            Parameters
            ----------
            token: str
                unique identifier of an item in angel
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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_quote auth is ' + str(e))
            raise Exception("The error in ANGEL-get_quote auth is", e)

        i = 0
        while True:
            try:
                if token == '': token = self.broker_auth_init.get_token(name = name, exchange = exchange, expiry = expiry, strike = strike, optionType = optionType)
                symbol = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.token==str(token)].symbol.iloc[0]
                exchange = 'NSE' if (symbol[-3:] == '-EQ') or (symbol in self.broker_auth_init.index_) else 'NFO'
                data = broker_obj.ltpData(exchange, symbol, token)
                ltp_ = data['data']['ltp']
                open_ = data['data']['open']
                data = pd.DataFrame({'ltp':ltp_,  'open_price':open_, 'BD_last_traded_time':''}, index=[0])
                return data
            except Exception as e:
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-get_quote is ' + str(e) + ' Retrying..... ' + str(i))
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin:
                    raise Exception("The error in ANGEL-get_quote is", e)

    def place_order(self, username:str, transaction_type:str, price_:float, quantity:int, token:str = '', name:str = '', exchange:str = 'NSE', expiry:str = '', strike:str = '', optionType:str = '',  trigger:float = 0, product:str = '') -> str:
        ''' Place an order in angel for the user session saved in broker_obj
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
                unique identifier of an item in angel
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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-placeorder auth is ' + str(e))
            raise Exception("The error in ANGEL-placeorder auth is", e)

        i = 0
        if quantity == 0:
            self.broker_auth_init.logger.error(username + ': ATTENTION: ANGEL-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            self.broker_auth_init.print_to_chat(username, 'ATTENTION: ANGEL-placeorder recieved quantity 0 for ' + str(token) + ' ' + str(name) + " returning ''")
            return ''

        if token != '':
            symbol = self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.token==str(token)].symbol.iloc[0]
            exchange = 'NSE' if (symbol[-3:] == '-EQ') or (symbol in self.broker_auth_init.index_) else 'NFO'
        else: token = self.broker_auth_init.get_token(name = name, exchange=exchange, expiry = expiry, strike = strike, optionType = optionType)
        variety, ordertype, producttype = 'NORMAL', 'LIMIT', 'CARRYFORWARD'
        if price_ == 0: ordertype = 'MARKET'
        if trigger != 0: variety, ordertype = 'STOPLOSS', 'STOPLOSS_LIMIT'
        if exchange=='NSE': producttype = 'DELIVERY' # 'CARRYFORWARD' is for F&O

        if product == 'MARGIN': producttype = 'MARGIN'
        elif product == 'INTRADAY': producttype = 'INTRADAY'

        orderparams = { "variety": variety, "tradingsymbol": self.broker_auth_init.token_symbol_mapping[self.broker_auth_init.token_symbol_mapping.token==str(token)].symbol.iloc[0], "symboltoken": str(token), 
                        "transactiontype": transaction_type, "exchange": exchange, "ordertype": ordertype, "producttype": producttype, 
                        "duration": "DAY", "price": str(self.broker_auth_init.round_to(price_)), "triggerprice": str(self.broker_auth_init.round_to(trigger)), "quantity": str(int(quantity))}
        while True:
            try:
                order_id = broker_obj.placeOrder(orderparams)
                time.sleep(2.5)
                # try: df = self.broker_ord.orders(username) # check for order status
                # except Exception as e: raise Exception('ATTENTION: The error in ANGEL-placeorder: fetching orderbook unsuccessful: unable to check the status', e)
                # df = df[(df.orderId == str(order_id))]
                # if df.shape[0] == 0: raise Exception('ATTENTION: The error in ANGEL-placeorder: order not placed orderId: ', str(order_id))
                # if df.status.iloc[0] == 'REJ': raise Exception('ATTENTION: The error in ANGEL-placeorder: order rejected orderId: ', str(order_id))
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                try:
                    try: df = self.broker_ord.orders(username) # upon error, we check if the order is already placed in last 2mins (assuming 2 orders of same token won't be placed within 2mins)
                    except Exception as e3: raise Exception('ATTENTION: The error in ANGEL-placeorder: fetching orderbook unsuccessful: order is placed, manually cancel / exit the order', e, e3)
                    df = df[(df.instrumentToken == token) & (df.transactionType == transaction_type) & (df.status != 'CAN')]
                    df.sort_values(by='orderTimestamp', ascending=False, inplace=True)
                    for row_ in range(df.shape[0]): # will go in only if len(df) > 0
                        if datetime.datetime.strptime(df.orderTimestamp.iloc[row_], '%b %d %Y %H:%M:%S:%f%p') > (datetime.datetime.now()-datetime.timedelta(minutes=2)):
                            if price_*.95 < float(df.price.iloc[row_]) < price_*1.05: # price of executed order should be +/- 5% of the intended order price
                                self.broker_auth_init.logger.error(username + ': ATTENTION: the order was placed with error in angel-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                self.broker_auth_init.print_to_chat(username, 'ATTENTION: the order was placed with error in angel-place_order, obtained orderid from orderbook: ' + str(df.orderId.iloc[row_]) + '\nThe error was: ' + str(e))
                                return str(df.orderId.iloc[row_]) # return order_id if the orderId is found else sleep for 1 sec and try to place again
                except Exception as e1:
                    self.broker_auth_init.logger.error(username + ': ATTENTION: The error in ANGEL-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                    self.broker_auth_init.print_to_chat(username, 'ATTENTION: The error in ANGEL-placeorder-error handling_2: ' + str(i) + ' ' + str(e1) + ' ' + str(e) + ' Retrying.....')
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-placeorder is ' + str(e) + ' Retrying.....')
                self.broker_auth_init.print_to_chat(username, 'The error in ANGEL-placeorder is ' + str(e) + ' Retrying.....')
                time.sleep(1)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                if i >= self.broker_auth_init.try_fin: raise Exception("The error in ANGEL-placeorder is", e)

    def modify_order(self, username:str, order_id:str,  price:float, quantity:Union[int, str] = '', trigger:float = 0) -> str:
        ''' Modify only the price of a given order_id in angel for the user session saved in broker_obj.
            Should be only used when the order status is OPN / SLO / OPF.
            All orders are supported: buy / sell / SL / target / carry-forward.
            orders_df is used to get token, symbol, exchange used only in angel while modifing the order

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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-modifyorder auth is ' + str(e))
            raise Exception("The error in ANGEL-modifyorder auth is", e)

        i = 0
        variety, ordertype = 'NORMAL', 'LIMIT'
        if trigger != 0: variety, ordertype = 'STOPLOSS', 'STOPLOSS_LIMIT'
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        token, symbol, exchange  = order.instrumentToken.iloc[0], order.tradingsymbol.iloc[0], order.exchange.iloc[0]
        quantity = order.orderQuantity.iloc[0] # even in OPF we need to put whole orderQuantity while modifying, else order gets cancelled
        # if quantity == '': quantity = order.pendingQuantity.iloc[0]
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: # print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
            # print_logger('Order:', order_id, 'can not be modified as the status is-', order.status.iloc[0])
            return str(order_id)
        orderparams = {"variety":variety, "orderid":str(order_id), "ordertype":ordertype,"producttype":'CARRYFORWARD',
                "duration":"DAY", "price":str(self.broker_auth_init.round_to(price)),"triggerprice": str(self.broker_auth_init.round_to(trigger)), "quantity":str(int(quantity)),
                "tradingsymbol":symbol, "symboltoken": str(token), "exchange": exchange}
        while True:
            try:
                order_id = broker_obj.modifyOrder(orderparams)['data']['orderid']
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
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-modifyorder is ' + str(e) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception("The error in ANGEL-modifyorder is", e)

    def cancel_order(self, username:str, order_id:str) -> str:
        ''' Cancel a given order_id in angel for the user session saved in broker_obj.
            Should be only used when the order status is OPN / SLO.
            All orders are supported: buy / sell / SL / target / carry-forward.
            orders_df is used to get variety used only in angel while canceling the order

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
            broker_obj = SmartConnect(api_key=user_data['auth']['api_key'])
            broker_obj.access_token = user_data['auth']['access_token']
            broker_obj.feed_token = user_data['auth']['feed_token']
            broker_obj.refresh_token = user_data['auth']['refresh_token']
            broker_obj.userId = user_data['auth']['userId']
        except Exception as e:
            self.broker_auth_init.logger.error(username + ': The error in ANGEL-cancelorder auth is ' + str(e))
            raise Exception("The error in ANGEL-cancelorder auth is", e)

        i = 0
        order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        if order.shape[0] == 0:
            self.broker_ord.orders(username)
            order = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id]
        variety = self.broker_ord.get_orders_df(username)[self.broker_ord.get_orders_df(username).orderId == order_id].variety.iloc[0]
        if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: # print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
            # print_logger('Order:', order_id, 'can not be cancelled as the status is-', order.status.iloc[0])
            return str(order_id)
        while True:
            try:
                order = broker_obj.cancelOrder(str(order_id), variety)
                order_id = order['data']['orderid']
                return str(order_id)
            except Exception as e:
                time.sleep(1)
                orders_df_ = self.broker_ord.orders(username)
                order = orders_df_[orders_df_.orderId == order_id]
                if order.status.iloc[0] not in ['OPN', 'SLO', 'OPF']: # print_logger("Rechecked Current Status of the Order:", order.status.iloc[0])
                    self.broker_auth_init.logger.error(username + ': Order: ' + str(order_id) + ' can not be cancelled as the status is- ' + str(order.status.iloc[0]))
                    return str(order_id)
                if i==5: broker_obj = self.broker_auth_init.login(user_data['file'])
                i=i+1
                self.broker_auth_init.logger.error(username + ': The error in ANGEL-cancelorder is ' + str(e) + ' Status ' + str(order.status.iloc[0]) + ' Retrying....')
                if i >= self.broker_auth_init.try_fin: raise Exception("The error in ANGEL-cancelorder is", e)