import os
import io
import sys
import time
import pyotp
import zipfile
import requests
import numpy as np
import pandas as pd
from typing import Any
from BrokerAuthInit import BrokerAuthInit
from NorenRestApiPy.NorenApi import NorenApi

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

class ShoonyaAuthInit(BrokerAuthInit):
    def __init__(self) -> None:
        super().__init__()

    def login(self, file:dict, verbose:bool = True, single_try:bool = False) -> Any:
        ''' Login in the shoonya account for the given file.

            Parameters
            ----------
            file: dict
                dictionary containing the user's file information
            verbose: boolean
                True will enable print statements to inform if the login was successful

            Returns
            -------
            broker_obj: shoonya object with session generated
        '''
        for tries in range(self.try_fin): # Trying to login until successful
            try:
                time.sleep(1)
                broker_obj = ShoonyaApiPy()
                time.sleep(1)
                data = broker_obj.login(userid=file["shoonya_user_id"], password=file["shoonya_password"], 
                        twoFA=str(pyotp.TOTP(file["otp_key"]).now()), vendor_code=file["shoonya_vc"], api_secret=file["shoonya_app_key"], imei=file["shoonya_imei"])
                time.sleep(1)
                if data == None:
                    raise Exception('Unable to Login')
                if verbose: self.logger.info('Shoonya Login Successsful: ' + data['uname'])

                user_data = {
                    'username': file['username'],
                    'file': file,
                    'auth': {
                        '_NorenApi__username': broker_obj._NorenApi__username,
                        '_NorenApi__accountid': broker_obj._NorenApi__accountid,
                        '_NorenApi__password': broker_obj._NorenApi__password,
                        '_NorenApi__susertoken': broker_obj._NorenApi__susertoken
                    }
                }
                self.red.set(file['username'], str(user_data))
                return broker_obj
            except Exception as e:
                self.logger.error(file['username'] + ': ATTENTION: The error in SHOONYA-login is ' + str(e) + ' Retrying.....')
                self.print_to_chat(file['username'], 'ATTENTION: The error in SHOONYA-login is ' + str(e) + ' Retrying.....')
                if single_try:
                    return 0
                time.sleep(1)
        return 0

    def update_token_files(self) -> None:
        ''' Fetch latest token files of shoonya and save. '''
        root = 'https://api.shoonya.com/'
        masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip']
        for i in range(2): # we only try to update files two times
            time.sleep(1)
            try:
                url = root + masters[0]
                r = requests.get(url, allow_redirects=True)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                s = z.open(z.namelist()[0]).read().decode()
                assert len(s)>10000
                file = open('/app/Tokens/shoonya_tokens.csv', 'w')
                file.write(s)
                file.close()
                break
            except Exception as e:
                self.logger.error('Error in updating shoonya token files: ' + str(e))
                time.sleep(30)

        time.sleep(1)
        for i in range(2): # we only try to update files two times
            try:
                url = root + masters[1]
                r = requests.get(url, allow_redirects=True)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                s = z.open(z.namelist()[0]).read().decode()
                assert len(s)>10000
                file = open('/app/Tokens/shoonya_tokens_der.csv', 'w')
                file.write(s)
                file.close()
                break
            except Exception as e:
                self.logger.error('Error in updating shoonya token files: ' + str(e))
                time.sleep(30)

    def get_tokens_df_from_files(self) -> pd.DataFrame:
        ''' Returns a dataframe to be stored in RAM for quick access, 
            with tokens of: 
            1. NSE and NFO futures and options

            Returns
            -------
            tokens_df: dataframe with all columns as str dtype-
                ['instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']
                Example:
                    instrumentToken: '18568'
                    instrumentName: 'NIFTY'
                    expiry: '17JAN22' or '' for cash
                    strike: '4150.0' or '' for cash
                    optionType: "CE" / "PE" or '' for cash
                    lotSize: '100' or '' for cash
        '''
        token_shony = pd.read_csv('/app/Tokens/shoonya_tokens.csv')
        token_shony = token_shony[token_shony.Exchange == 'NSE']
        token_shony = token_shony[['Token', 'TradingSymbol', 'Symbol']]

        token_shony.loc[token_shony.Symbol.str.upper() == self.index_diff_[0], 'Symbol'] = self.index_[0]
        token_shony.loc[token_shony.Symbol.str.upper() == self.index_diff_[1], 'Symbol'] = self.index_[1]
        token_shony.loc[token_shony.Symbol.str.upper() == self.index_diff_[2], 'Symbol'] = self.index_[2]
        token_shony = token_shony[token_shony.TradingSymbol.str.endswith('-EQ') | token_shony.Symbol.str.upper().isin(self.index_)]

        token_shony.columns = ['instrumentToken_', 'instrumentToken', 'instrumentName']
        token_shony['expiry'] = ''
        token_shony['strike'] = ''
        token_shony['optionType'] = ''
        token_shony['lotSize'] = ''

        token_shony_der = pd.read_csv('/app/Tokens/shoonya_tokens_der.csv')
        token_shony_der = token_shony_der[['Token', 'TradingSymbol', 'Symbol', 'Expiry', 'StrikePrice', 'OptionType', 'LotSize']]
        token_shony_der.columns = ['instrumentToken_', 'instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']
        token_shony_der['expiry'] = pd.to_datetime(token_shony_der.expiry).dt.strftime("%d")+pd.to_datetime(token_shony_der.expiry).dt.strftime("%b").str.upper()+pd.to_datetime(token_shony_der.expiry).dt.strftime("%y")
        token_shony_der.loc[token_shony_der['optionType'] == 'XX', ['optionType', 'strike']] = ['FUT', '']

        self.token_symbol_mapping = token_shony.append(token_shony_der)
        self.token_symbol_mapping.reset_index(inplace=True, drop=True)

        self.token_symbol_mapping = self.token_symbol_mapping[['instrumentToken_', 'instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']].astype(str)

        self.tokens_df = self.token_symbol_mapping[['instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']].copy()

        return self.tokens_df