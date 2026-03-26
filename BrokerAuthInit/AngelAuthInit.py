import ssl
import time
import json
import pyotp
import numpy as np
import pandas as pd
import urllib.request
from typing import Any
from SmartApi import SmartConnect
from BrokerAuthInit import BrokerAuthInit

class AngelAuthInit(BrokerAuthInit):
    def __init__(self) -> None:
        super().__init__()

    def login(self, file:dict, verbose:bool = True, single_try:bool = False) -> Any:
        ''' Login in the angel account for the given file.

            Parameters
            ----------
            file: dict
                dictionary containing the user's file information
            verbose: boolean
                True will enable print statements to inform if the login was successful

            Returns
            -------
            broker_obj: angel object with session generated
        '''
        for tries in range(self.try_fin): # Trying to login until successful
            try:
                broker_obj = SmartConnect(api_key=file["angel_api_k"])
                time.sleep(1)
                totp = pyotp.TOTP(file["otp_key"])
                data = broker_obj.generateSession(file["angel_user_id"], file["pin"], str(totp.now()))
                time.sleep(1)
                if verbose: self.logger.info('Angel Login Successsful: ' + data['data']['name'])
                user_data = {
                    'username': file['username'],
                    'file': file,
                    'auth': {'api_key': broker_obj.api_key,
                             'access_token': broker_obj.access_token,
                             'feed_token': broker_obj.feed_token,
                             'refresh_token': broker_obj.refresh_token,
                             'userId': broker_obj.userId}
                }
                self.red.set(file['username'], str(user_data))
                return broker_obj
            except Exception as e:
                self.logger.error(file['username'] + ': ATTENTION: The error in ANGEL-login is ' + str(e) + ' Retrying.....')
                self.print_to_chat(file['username'], 'ATTENTION: The error in ANGEL-login is ' + str(e) + ' Retrying.....')
                if single_try:
                    return 0
                time.sleep(1)
        return 0

    def update_token_files(self) -> None:
        ''' Fetch latest token files of angel and save. '''
        time.sleep(1)
        for i in range(2): # we only try to update files two times
            try:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                new_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
                uh = urllib.request.urlopen(new_url, context=ctx)
                data = uh.read().decode()
                js = json.loads(data)
                token=pd.DataFrame(js)
                assert token.shape[0] > 10
                token.to_csv('/app/Tokens/angel_tokens.csv',index=False)
                break
            except Exception as e:
                self.logger.error('Error in updating angel token files: ' + str(e))
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
                    instrumentToken: '20930'
                    instrumentName: 'NIFTY'
                    expiry: '17JAN22' or '' for cash
                    strike: '4150.0' or '' for cash
                    optionType: "CE" / "PE" or '' for cash
                    lotSize: '100' or '' for cash
        '''
        tokens=pd.read_csv('/app/Tokens/angel_tokens.csv')
        tokens.token = tokens.token.apply(lambda x: str(int(float(x))) if pd.notna(pd.to_numeric(x, errors='coerce')) else x)
        self.tokens_df = tokens[tokens.exch_seg.isin(['NSE','NFO'])].drop_duplicates() # (tokens.name.isin(self.index_ + self.all))
        self.tokens_df = self.tokens_df[ self.tokens_df.symbol.str.endswith('-EQ') | self.tokens_df.symbol.str.upper().isin(self.index_diff_) | 
                            self.tokens_df.symbol.str.endswith('CE') | self.tokens_df.symbol.str.endswith('PE') | self.tokens_df.symbol.str.endswith('FUT')] # filtering equity, indices & options (some others are also there which we want to exclude)
        self.tokens_df['optionType'] = np.where(self.tokens_df.exch_seg=='NFO', self.tokens_df.symbol.str[-2:], np.nan) # BANKNIFTY24FEB2240100PE: last two letters indicate optionType
        self.token_symbol_mapping = self.tokens_df[['token', 'symbol']].astype(str).copy()
        self.tokens_df = self.tokens_df[['token', 'name', 'expiry', 'strike', 'optionType', 'lotsize']] # required columns as of angel
        self.tokens_df.columns = ['instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize'] # renaming as of angel
        self.tokens_df.expiry = self.tokens_df.expiry.str[:-4] + self.tokens_df.expiry.str[-2:] # correction: expiry format from 27JAN2022 to 27JAN22
        self.tokens_df.strike = self.tokens_df.strike/100 # correction: strike prices are multiplied by 100 hence divided by 100
        self.tokens_df.loc[self.tokens_df['optionType'] == 'UT', ['optionType', 'strike']] = ['FUT', '']
        self.tokens_df.lotSize = self.tokens_df.lotSize.astype(int) # lotSize should not be in float format
        self.tokens_df = self.tokens_df[['instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']].astype(str)
        self.tokens_df.loc[self.tokens_df.optionType=='nan', ['expiry','strike','optionType', 'lotSize']] = ''
        return self.tokens_df