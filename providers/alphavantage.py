__author__ = 'Antonio Orsini'
__doc__    = 'Module to request data from Alpha Vantage'

from   urllib.parse import urlencode
import requests
import pandas as pd
import numpy as np

class AlphaVantageRequest():
    ''' Class to request output from Alpha Vantage API ''' 

    def __init__( self, apikey = None, statements_freq = 'quarterly' ):

        if apikey is None:
            from   marketdata.keys    import alpha_vantage_api_key
            self.apikey     = alpha_vantage_api_key 

        self.data_boxes = {
            'TIME_SERIES_DAILY_ADJUSTED'  : 'Time Series (Daily)',
            'TIME_SERIES_DAILY'           : 'Time Series (Daily)',
            'TIME_SERIES_WEEKLY'          : 'Weekly Time Series',
            'TIME_SERIES_WEEKLY_ADJUSTED' : 'Weekly Adjusted Time Series',
            'TIME_SERIES_MONTHLY'         : 'Monthly Time Series',
            'TIME_SERIES_MONTHLY_ADJUSTED': 'Monthly Adjusted Time Series',
            'TIME_SERIES_INTRADAY'        : 'To be updated on request',
            'INCOME_STATEMENT'            : statements_freq + 'Reports',
            'BALANCE_SHEET'               : statements_freq + 'Reports' ,
            'CASH_FLOW'                   : statements_freq + 'Reports',
            'EARNINGS'                    : statements_freq + 'Earnings',
            'OVERVIEW'                    : '',
            }

    def request( self,
                 function   = 'TIME_SERIES_DAILY_ADJUSTED',
                 symbol     = 'AAPL',
                 outputsize = 'full',
                 datatype   = None,
                 interval   = None):
        ''' Method to call a function from the API offered solutions '''
        
        # define parameters for encoding in the url
        self.params = { 'apikey'     : self.apikey,
                        'function'   : function,
                        'symbol'     : symbol,
                        'outputsize' : outputsize,
                        'datatype'   : datatype,
                        'interval'   : interval }

        if function == 'TIME_SERIES_INTRADAY':
            self.data_boxes['TIME_SERIES_INTRADAY'] :'Time Series ' + '(' + str(self.params['interval']) + ')'

        # delete the parameters that are set to none to avoid encoding mistakes
        encoded_params = dict((key, value) for key, value in self.params.items() if value is not None)

        # now chain up the encoded parameters with the right url
        encoded_params=urlencode(encoded_params)
        url = 'https://www.alphavantage.co/query?' + encoded_params

        # request, read the java format and return
        # req=requests.get(url,auth=(username, password))
        req=requests.get(url)
        
        # define error 404
        if str(req) == '<Response [404]>': raise ValueError('response is 404')

        self.requestdoc=req.json()

        return self.parse( self.requestdoc )

    def parse( self ):
        ''' Method to parse the output based on the funcion used during the request '''
 
        # choose the data box appropriate for each use depending on the function
        data_box = self.data_boxes[self.params['function']]

        if 'TIME_SERIES' in self.params['function']:
            df = pd.DataFrame().from_dict( self.requestdoc[data_box] ).transpose()[::-1]
            df.columns = [x[3:] for x in df.columns]
        
        if self.params['function'] == 'OVERVIEW':
            df = pd.DataFrame(self.requestdoc, index = [0])

        else:
            df = pd.DataFrame().from_dict( self.requestdoc[data_box] )

        self.data = df
        return(df)









