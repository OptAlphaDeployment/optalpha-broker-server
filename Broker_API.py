from fastapi.middleware.cors import CORSMiddleware
from fastapi.params import Body
from fastapi import FastAPI
import numpy as np
import warnings
import json

import sys
sys.path.insert(0, '/app/BrokerAuthInit/')
sys.path.insert(1, '/app/BrokerOrd/')
sys.path.insert(2, '/app/BrokerPos/')
sys.path.insert(3, '/app/BrokerPortfo/')
sys.path.insert(4, '/app/BrokerTrade/')

from AngelAuthInit import AngelAuthInit
from AngelOrd import AngelOrd
from AngelPos import AngelPos
from AngelPortfo import AngelPortfo
from AngelTrade import AngelTrade

from KotakneoAuthInit import KotakneoAuthInit
from KotakneoOrd import KotakneoOrd
from KotakneoPos import KotakneoPos
from KotakneoPortfo import KotakneoPortfo
from KotakneoTrade import KotakneoTrade

from ShoonyaAuthInit import ShoonyaAuthInit
from ShoonyaOrd import ShoonyaOrd
from ShoonyaPos import ShoonyaPos
from ShoonyaPortfo import ShoonyaPortfo
from ShoonyaTrade import ShoonyaTrade

warnings.filterwarnings('ignore')

AngelAuthInit.list_update()

aai = AngelAuthInit()
aord = AngelOrd(aai)
apos = AngelPos(aai)
aprt = AngelPortfo(aai)
atrade = AngelTrade(aai, aord)

kai = KotakneoAuthInit()
kord = KotakneoOrd(kai)
kpos = KotakneoPos(kai)
kprt = KotakneoPortfo(kai)
ktrade = KotakneoTrade(kai, kord)

sai = ShoonyaAuthInit()
sord = ShoonyaOrd(sai)
spos = ShoonyaPos(sai)
sprt = ShoonyaPortfo(sai)
strade = ShoonyaTrade(sai, sord)

try:
    aai.update_token_files()
    kai.update_token_files()
    sai.update_token_files()

    _ = aai.get_tokens_df_from_files()
    _ = kai.get_tokens_df_from_files()
    _ = sai.get_tokens_df_from_files()

    print('Successfully Initialized')
except Exception as e:
    print('Error in Initialization', str(e))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return json.dumps({'Welcome':'This is Broker API'})

@app.post("/get_file")
def get_file_api(get_file_args: dict = Body(...)):
    try:
        aai.connect_to_postgres_db()
        file = aai.get_user(get_file_args['username'])
        aai.close_postgres_db()
        file['username'] = get_file_args['username']
        return {'file':file, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'file': {}, 'error': str(e)}

@app.post("/login")
def login_api(login_args: dict = Body(...)):
    try:
        file = login_args['file']
        broker = file['broker']
        if broker == 'angel':
            resp = aai.login(login_args['file'])
        if broker == 'kotakneo':
            resp = kai.login(login_args['file'])
        if broker == 'shoonya':
            resp = sai.login(login_args['file'])
        if resp == 0: raise Exception('Login Error')
        else: return {'error': ''}
    except Exception as e:
        print(str(e))
        return {'error': str(e)}

@app.post("/get_token")
def get_token_api(get_token_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(get_token_args['username']))['file']['broker']
        if broker == 'angel':
            token = aai.get_token(name=get_token_args['name'], exchange=get_token_args['exchange'], 
                                expiry=get_token_args['expiry'], strike=get_token_args['strike'], 
                                optionType=get_token_args['optionType'])
        if broker == 'kotakneo':
            token = kai.get_token(name=get_token_args['name'], exchange=get_token_args['exchange'], 
                                expiry=get_token_args['expiry'], strike=get_token_args['strike'], 
                                optionType=get_token_args['optionType'])
        if broker == 'shoonya':
            token = sai.get_token(name=get_token_args['name'], exchange=get_token_args['exchange'], 
                                expiry=get_token_args['expiry'], strike=get_token_args['strike'], 
                                optionType=get_token_args['optionType'])
        return {'token':token, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'token':'', 'error': str(e)}

@app.post("/get_name")
def get_name_api(get_name_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(get_name_args['username']))['file']['broker']
        if broker == 'angel':
            name, expiry, strike, optionType, lots = aai.get_name(token=get_name_args['token'])
        if broker == 'kotakneo':
            name, expiry, strike, optionType, lots = kai.get_name(token=get_name_args['token'])
        if broker == 'shoonya':
            name, expiry, strike, optionType, lots = sai.get_name(token=get_name_args['token'])
        data = {
            'name': name,
            'expiry': expiry,
            'strike': strike,
            'optionType': optionType,
            'lots': lots
        }
        return {'data':data, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'data':'', 'error': str(e)}

@app.post("/orders")
def orders_api(orders_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(orders_args['username']))['file']['broker']
        if broker == 'angel':
            orders = aord.orders(orders_args['username'])
        if broker == 'kotakneo':
            orders = kord.orders(orders_args['username'])
        if broker == 'shoonya':
            orders = sord.orders(orders_args['username'])
        orders.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        data = orders.to_dict()
        return {'data':data, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'data':'', 'error': str(e)}

@app.post("/order_update_time")
def order_update_time_api(order_update_time_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(order_update_time_args['username']))['file']['broker']
        if broker == 'angel':
            order_update_time = aord.get_order_update_time(order_update_time_args['username'])
        if broker == 'kotakneo':
            order_update_time = kord.get_order_update_time(order_update_time_args['username'])
        if broker == 'shoonya':
            order_update_time = sord.get_order_update_time(order_update_time_args['username'])
        return {'order_update_time':order_update_time, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'order_update_time':'', 'error': str(e)}

@app.post("/positions")
def positions_api(positions_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(positions_args['username']))['file']['broker']
        if broker == 'angel':
            positions = apos.positions(positions_args['username'])
        if broker == 'kotakneo':
            positions = kpos.positions(positions_args['username'])
        if broker == 'shoonya':
            positions = spos.positions(positions_args['username'])
        positions.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        data = positions.to_dict()
        return {'data':data, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'data':'', 'error': str(e)}

@app.post("/position_update_time")
def position_update_time_api(position_update_time_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(position_update_time_args['username']))['file']['broker']
        if broker == 'angel':
            position_update_time = apos.get_position_update_time(position_update_time_args['username'])
        if broker == 'kotakneo':
            position_update_time = kpos.get_position_update_time(position_update_time_args['username'])
        if broker == 'shoonya':
            position_update_time = spos.get_position_update_time(position_update_time_args['username'])
        return {'position_update_time':position_update_time, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'position_update_time':'', 'error': str(e)}

@app.post("/portfolio")
def portfolio_api(portfolio_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(portfolio_args['username']))['file']['broker']
        if broker == 'angel':
            portfolio = aprt.portfolio(portfolio_args['username'])
        if broker == 'kotakneo':
            portfolio = kprt.portfolio(portfolio_args['username'])
        if broker == 'shoonya':
            portfolio = sprt.portfolio(portfolio_args['username'])
        portfolio.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        data = portfolio.to_dict()
        return {'data':data, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'data':'', 'error': str(e)}

@app.post("/get_available_cash")
def get_available_cash_api(get_available_cash_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(get_available_cash_args['username']))['file']['broker']
        if broker == 'angel':
            available_cash = atrade.get_available_cash(username=get_available_cash_args['username'])
        if broker == 'kotakneo':
            available_cash = ktrade.get_available_cash(username=get_available_cash_args['username'])
        if broker == 'shoonya':
            available_cash = strade.get_available_cash(username=get_available_cash_args['username'])
        return {'available_cash':available_cash, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'available_cash':'', 'error': str(e)}

@app.post("/get_required_margin")
def get_required_margin_api(get_required_margin_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(get_required_margin_args['username']))['file']['broker']
        if broker == 'angel':
            required_margin = atrade.get_required_margin(username=get_required_margin_args['username'], transaction_type=get_required_margin_args['transaction_type'], 
                                token=get_required_margin_args['token'], price_=get_required_margin_args['price_'], product=get_required_margin_args['product'])
        if broker == 'kotakneo':
            required_margin = ktrade.get_required_margin(username=get_required_margin_args['username'], transaction_type=get_required_margin_args['transaction_type'], 
                                token=get_required_margin_args['token'], price_=get_required_margin_args['price_'], product=get_required_margin_args['product'])
        if broker == 'shoonya':
            required_margin = strade.get_required_margin(username=get_required_margin_args['username'], transaction_type=get_required_margin_args['transaction_type'], 
                                token=get_required_margin_args['token'], price_=get_required_margin_args['price_'], product=get_required_margin_args['product'])
        return {'required_margin':required_margin, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'required_margin':'', 'error': str(e)}

@app.post("/get_quote")
def get_quote_api(get_quote_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(get_quote_args['username']))['file']['broker']
        if broker == 'angel':
            quote = atrade.get_quote(username=get_quote_args['username'], token=get_quote_args['token'], 
                                name=get_quote_args['name'], exchange=get_quote_args['exchange'], 
                                expiry=get_quote_args['expiry'], strike=get_quote_args['strike'], 
                                optionType=get_quote_args['optionType'])
        if broker == 'kotakneo':
            quote = ktrade.get_quote(username=get_quote_args['username'], token=get_quote_args['token'], 
                                name=get_quote_args['name'], exchange=get_quote_args['exchange'], 
                                expiry=get_quote_args['expiry'], strike=get_quote_args['strike'], 
                                optionType=get_quote_args['optionType'])
        if broker == 'shoonya':
            quote = strade.get_quote(username=get_quote_args['username'], token=get_quote_args['token'], 
                                name=get_quote_args['name'], exchange=get_quote_args['exchange'], 
                                expiry=get_quote_args['expiry'], strike=get_quote_args['strike'], 
                                optionType=get_quote_args['optionType'])
        quote.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        data = quote.to_dict()
        return {'data':data, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'data':'', 'error': str(e)}

@app.post("/place_order")
def place_order_api(place_order_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(place_order_args['username']))['file']['broker']
        if broker == 'angel':
            orderid = atrade.place_order(username=place_order_args['username'], transaction_type=place_order_args['transaction_type'], 
                                price_=place_order_args['price_'], quantity=place_order_args['quantity'], 
                                token=place_order_args['token'], name=place_order_args['name'], 
                                exchange=place_order_args['exchange'], expiry=place_order_args['expiry'],
                                strike=place_order_args['strike'], optionType=place_order_args['optionType'],
                                trigger=place_order_args['trigger'], product=place_order_args['product'])
        if broker == 'kotakneo':
            orderid = ktrade.place_order(username=place_order_args['username'], transaction_type=place_order_args['transaction_type'], 
                                price_=place_order_args['price_'], quantity=place_order_args['quantity'], 
                                token=place_order_args['token'], name=place_order_args['name'], 
                                exchange=place_order_args['exchange'], expiry=place_order_args['expiry'],
                                strike=place_order_args['strike'], optionType=place_order_args['optionType'],
                                trigger=place_order_args['trigger'], product=place_order_args['product'])
        if broker == 'shoonya':
            orderid = strade.place_order(username=place_order_args['username'], transaction_type=place_order_args['transaction_type'], 
                                price_=place_order_args['price_'], quantity=place_order_args['quantity'], 
                                token=place_order_args['token'], name=place_order_args['name'], 
                                exchange=place_order_args['exchange'], expiry=place_order_args['expiry'],
                                strike=place_order_args['strike'], optionType=place_order_args['optionType'],
                                trigger=place_order_args['trigger'], product=place_order_args['product'])
        return {'orderid':orderid, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'orderid':'', 'error': str(e)}

@app.post("/modify_order")
def modify_order_api(modify_order_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(modify_order_args['username']))['file']['broker']
        if broker == 'angel':
            orderid = atrade.modify_order(username=modify_order_args['username'], order_id=modify_order_args['order_id'], 
                                price=modify_order_args['price'], quantity=modify_order_args['quantity'], 
                                trigger=modify_order_args['trigger'])
        if broker == 'kotakneo':
            orderid = ktrade.modify_order(username=modify_order_args['username'], order_id=modify_order_args['order_id'], 
                                price=modify_order_args['price'], quantity=modify_order_args['quantity'], 
                                trigger=modify_order_args['trigger'])
        if broker == 'shoonya':
            orderid = strade.modify_order(username=modify_order_args['username'], order_id=modify_order_args['order_id'], 
                                price=modify_order_args['price'], quantity=modify_order_args['quantity'], 
                                trigger=modify_order_args['trigger'])
        return {'orderid':orderid, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'orderid':'', 'error': str(e)}

@app.post("/cancel_order")
def cancel_order_api(cancel_order_args: dict = Body(...)):
    try:
        broker = aai.get_data_structures(aai.red.get(cancel_order_args['username']))['file']['broker']
        if broker == 'angel':
            orderid = atrade.cancel_order(username=cancel_order_args['username'], order_id=cancel_order_args['order_id'])
        if broker == 'kotakneo':
            orderid = ktrade.cancel_order(username=cancel_order_args['username'], order_id=cancel_order_args['order_id'])
        if broker == 'shoonya':
            orderid = strade.cancel_order(username=cancel_order_args['username'], order_id=cancel_order_args['order_id'])
        return {'orderid':orderid, 'error': ''}
    except Exception as e:
        print(str(e))
        return {'orderid':'', 'error': str(e)}