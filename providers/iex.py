__author__ = 'Antonio Orsini'
__doc__    = 'Module to request data from IEX'

import requests
import pandas as pd
from   marketdata.logger    import setLogger

class IEXRequest( object ):
    ''' class to handle requests to iex api ''' 

    def __init__( self, env = 'sandbox',  token = None  ):
        ''' set env and generic vars '''
        if   env == 'sandbox':
            self.base_url = 'https://sandbox.iexapis.com/'
            self.version  = 'stable/'

        elif env == 'prod':
            self.base_url = ''
            self.version  = ''

        if token is None:
            from marketdata.keys import iex_secret_token
            self.token = iex_secret_token
        self.query_params  = f'?token={self.token}'
        self.logger = setLogger('IEXCloud')

    def qChart( self, symbol, range = '1w' ):
        self.endpoint_path = f'stock/{symbol}/chart/{range}'
        self.call()
        self.data = pd.DataFrame(self.data)
        return self.data

    def call( self ):
        api_call = f'{self.base_url}{self.version}{self.endpoint_path}{self.query_params}'
        self.logger.info(f'API Calls: {api_call}')
        r = requests.get(api_call)
        self.data = r.json()
        return self.data


