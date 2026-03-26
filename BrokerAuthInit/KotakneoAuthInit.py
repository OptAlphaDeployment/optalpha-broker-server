import time
import pyotp
import base64
import requests
import datetime
import pandas as pd
from typing import Any
from BrokerAuthInit import BrokerAuthInit

class KotakneoAuthInit(BrokerAuthInit):
    def __init__(self) -> None:
        super().__init__()

    def login(self, file:dict, verbose:bool = True, single_try:bool = False) -> Any:
        ''' Login in the kotakneo account for the given file.

            Parameters
            ----------
            file: dict
                dictionary containing the user's file information
            verbose: boolean
                True will enable logger statements to inform if the login was successful

            Returns
            -------
            broker_obj: kotakneo object with session generated
        '''
        info_1 = ''
        info_2 = ''
        for tries in range(self.try_fin): # Trying to login until successful
            try:
                totp = pyotp.TOTP(file["otp_key"])
                data = {
                    'mobileNumber': file["mobile_number"],
                    'ucc': file["id"],
                    'totp': str(totp.now())
                }
                headers = {
                    'Authorization': file["token"],
                    'neo-fin-key': 'neotradeapi'
                }
                info_1 = requests.post('https://mis.kotaksecurities.com/login/1.0/tradeApiLogin', json=data, headers=headers).json()
                auth = info_1['data']['token']
                sid = info_1['data']['sid']

                data = {
                    'mpin': file["mpin"]
                }
                headers = {
                    'Auth': auth,
                    'sid': sid,
                    'Authorization': file["token"],
                    'neo-fin-key': 'neotradeapi'
                }

                info_2 = requests.post('https://mis.kotaksecurities.com/login/1.0/tradeApiValidate', json=data, headers=headers).json()

                if verbose: self.logger.info('KOTAKNEO Login Successsful: ' + info_2['data']['greetingName'])

                user_data = {
                    'username': file['username'],
                    'file': file,
                    'auth': {'token': info_2['data']['token'],
                             'sid': info_2['data']['sid'],
                             'base_url': info_2['data']['baseUrl']
                    }
                }
                self.red.set(file['username'], str(user_data))
                return user_data
            except Exception as e:
                self.logger.error(file['username'] + ': ATTENTION: The error in KOTAKNEO-login is ' + str(e) + ' Retrying.....')
                self.print_to_chat(file['username'], 'ATTENTION: The error in KOTAKNEO-login is ' + str(e) + ' Retrying.....')
                try:
                    if (single_try) or (info_1['error'][0]['code'] in ['10552', '10525']):
                        self.logger.error(file['username'] + ': ATTENTION: The error in KOTAKNEO-login please reset manually.')
                        self.print_to_chat(file['username'], 'ATTENTION: The error in KOTAKNEO-login please reset manually.')
                        return 0
                except:
                    try:
                        if (info_2['error'][0]['code'] in ['10522']):
                            time.sleep(60)
                    except: pass
                time.sleep(1)
        return 0

    def update_token_files(self) -> None:
        ''' Fetch latest token files of kotakneo and save. '''
        for i in range(2): # we only try to update files two times
            try:
                time.sleep(1)
                nse_cm = pd.read_csv(f'https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{str(datetime.datetime.now().date())}/transformed-v1/nse_cm-v1.csv', low_memory=False)
                nse_cm.to_csv('/app/Tokens/kotakneo_tokens.csv')
                break
            except Exception as e:
                self.logger.error('Error in updating kotakneo token files: ' + str(e))
                time.sleep(30)

        time.sleep(1)
        for i in range(2): # we only try to update files two times
            try:
                nse_fo = pd.read_csv(f'https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{str(datetime.datetime.now().date())}/transformed/nse_fo.csv', low_memory=False)
                nse_fo.to_csv('/app/Tokens/kotakneo_tokens_der.csv')
                break
            except Exception as e:
                self.logger.error('Error in updating kotakneo token files: ' + str(e))
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

        tokens = pd.read_csv('/app/Tokens/kotakneo_tokens.csv', low_memory=False)

        token_kot = tokens[(tokens.pGroup=='EQ') | (tokens.pGroup.isna() & tokens.pSymbolName.isin(self.index_))]
        # token_kot = token_kot[(token_kot.pSymbolName.isin(self.all)) | (token_kot.pSymbolName.isin(self.index_))]
        token_kot = token_kot[['pSymbol', 'pTrdSymbol', 'pSymbolName']]
        token_kot.columns = ['instrumentToken_', 'instrumentToken', 'instrumentName']
        for column in ['expiry', 'strike', 'optionType', 'lotSize']:
            token_kot[column] = ''

        tokens = pd.read_csv('/app/Tokens/kotakneo_tokens_der.csv', low_memory=False)

        token_kot_der = tokens[['pSymbol', 'pTrdSymbol', 'pSymbolName', 'lExpiryDate ', 'dStrikePrice;', 'pOptionType', 'lLotSize']]
        # token_kot_der = token_kot_der[(token_kot_der.pSymbolName.isin(self.all)) | (token_kot_der.pSymbolName.isin(self.index_))]
        token_kot_der.columns = ['instrumentToken_', 'instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']
        token_kot_der = token_kot_der[token_kot_der.optionType.isin(['CE', 'PE', 'XX'])]
        # token_kot_der['expiry'] = (pd.to_datetime(token_kot_der.expiry, unit='s') + pd.DateOffset(years=10)).dt.date
        token_kot_der['expiry'] = (pd.to_datetime(token_kot_der.expiry + 315511200, unit='s')).dt.date
        token_kot_der['expiry'] = pd.to_datetime(token_kot_der.expiry).dt.strftime("%d") + pd.to_datetime(token_kot_der.expiry).dt.strftime("%b").str.upper() + pd.to_datetime(token_kot_der.expiry).dt.strftime("%y")
        token_kot_der['strike'] = token_kot_der.strike/100
        token_kot_der.loc[token_kot_der['optionType'] == 'XX', ['optionType', 'strike']] = ['FUT', '']

        self.tokens_df = token_kot.append(token_kot_der)
        self.tokens_df = self.tokens_df[['instrumentToken_', 'instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']].astype(str)

        self.token_symbol_mapping = self.tokens_df.copy()

        self.tokens_df = self.tokens_df[['instrumentToken', 'instrumentName', 'expiry', 'strike', 'optionType', 'lotSize']]

        return self.tokens_df