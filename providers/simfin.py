__author__ = 'Antonio Orsini'
__doc__    = 'Module to request data from simfin'

import requests
import pandas as pd

from functools import reduce

import time
import gc
import os


from marketdata.storage   import getProviderDataPath

class SimFinRequest():
    
    def __init__( self, api_key = None ):
        if api_key is None:
            from marketdata.keys import simfin_api_key
            self.api_key = simfin_api_key
        else:
            self.api_key = api_key

        self.path_fundamental = getProviderDataPath( 'simfin', 'fundamentals' )
        self.path_ohlc        = getProviderDataPath( 'simfin', 'ohlc' )

    def getFundamentals( self, ticker, year_start = 2019, year_end = 2020, periods = None, fundamental_datasets = None ):
        ''' get fundamentals data in one of those dataset: "pl" "bs" "cf" "derived" "all" '''
        
        del_cols   = ['SimFinId','TTM','Value Check', 'Source']
        
        if periods is None:
            periods =  ["q1", "q2", "q3", "q4"]

        if fundamental_datasets is None:
            fundamental_datasets = ['pl', 'bs', 'cf']

        # request url for all financial statements
        request_url = 'https://simfin.com/api/v2/companies/statements'

        output  = []

        for year in range( year_start, year_end + 1):
            for period in periods:
                
                fundamentals_dfs_list = []

                for fundamental in fundamental_datasets:
                    parameters = {
                        "statement": fundamental, 
                        "ticker": ticker, 
                        "period": period, 
                        "fyear": year,
                        "api-key": self.api_key
                        }
                    request = requests.get(request_url, parameters)

                    print(request.json())

                    data = request.json()[0]

                    if data['found'] and len(data['data']) > 0:
                        df = pd.DataFrame(data['data'], columns = data['columns'])
                        for col in del_cols: del df[col]
                        fundamentals_dfs_list.append(df)

                if   len(fundamentals_dfs_list) == 0:
                    continue

                elif len( fundamentals_dfs_list ) > 1:
                    df_fm = reduce(
                        lambda x,y: pd.merge(x,y[y.columns.difference(x.columns)],
                        left_index=True, right_index=True, how='outer'),
                        fundamentals_dfs_list)
                else:
                    df_fm = fundamentals_dfs_list[0]

                output.append(df_fm)
                time.sleep(1)


        df = pd.concat(output, axis = 0)
        df.dropna( inplace = True, axis = 1 )
        df.to_csv( self.path_fundamental + '/' + ticker + '.csv', index = False )


def splitManualData():

    path_fundamentals_to_agg   = r'C:\Users\anton\OneDrive\Datasets\projects\marketdata\providers\simfin\manual_downloads\fundamentals_to_aggregate'
    path_files_to_split        = r'C:\Users\anton\OneDrive\Datasets\projects\marketdata\providers\simfin\manual_downloads\files_to_operate'
    path_ohlc                  = getProviderDataPath( 'simfin', 'ohlc' )
    path_fundamentals          = getProviderDataPath( 'simfin', 'fundamentals' )

    fundms = []
    for filename in os.listdir( path_fundamentals_to_agg ):
        if filename.endswith('.csv'):
            dtm = pd.read_csv( path_fundamentals_to_agg + '/' + filename, sep = ';' )
            fundms.append(dtm)
    fundms = reduce(
                    lambda x,y: pd.merge(x,y[y.columns.difference(x.columns)],
                    left_index=True, right_index=True, how='outer'),
                    fundms)
    fundms.to_csv( path_files_to_split + '/fundamentals.csv', sep = ';', index = False )

    for filename in os.listdir( path_files_to_split ):
        print(filename)
        if filename.endswith('.csv'):
            if 'shareprices' in filename:
                destination = path_ohlc
            if 'fundamentals' in filename:
                destination = path_fundamentals

            df = pd.read_csv( path_files_to_split + '/' + filename, sep = ';' )
            tickers = df['Ticker'].unique()

            for ticker in tickers:
                dfx = df[df['Ticker'] == ticker]
                del dfx['Ticker']
                del dfx['SimFinId']
                dfx.to_csv(destination+'/'+ticker+'.csv', index = False)
                del dfx
                gc.collect()
# def main():
#     #sfr = SimFinRequest()
#     #sfr.getFundamentals(ticker='MSFT', year_start=2000, year_end=2020)
#     splitManualData()

# if __name__ == '__main__':
#     main()