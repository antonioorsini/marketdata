__author__ = 'Antonio Orsini'
__doc__    = 'Module to regroup different market data sources into a single class.'

import logging
import pandas as pd
import numpy as np
import pickle
import time
import os
import shutil

from misc.utils                           import setLogger
from marketdata.storage                   import getProviderDataPath, path_tickers
from marketdata.keys                      import *
from marketdata.providers.alpha_vantage   import AlphaVantageRequest
from marketdata.providers.simfin          import SimFinRequest
from marketdata.providers.yahoofinance    import YahooFinanceRequest

class Archiver( object ):
    ''' class enabling the archiving through different market data api '''

    def __init__( self, tickers, pause_time = 1, pause_every_n_queries = None ):        
        self.logger = setLogger('MarketDataArchiver')

        self.tickers        = tickers
        self.n_ticker       = len(tickers)
 
        self.pause_time            = pause_time
        self.pause_every_n_queries = pause_every_n_queries
        self.try_retrieve_n_times  = 10

        self.count_progress    = 0
        self.count_for_pausing = 0
        self.missing_data_ticker_list = []

        self.requestFunction = None
        self.path_archive    = None

    @staticmethod
    def adjustSavingSymbol( symbol ):
        if symbol in ['CON', 'PRN', 'NUL']:
                symbol = symbol + '_'
        return symbol

    def handlePausing( self ):
        # Pausing otherwise the data provider may kick you out
        if self.pause_every_n_queries != None:
            self.count_for_pausing += 1
            if self.count_for_pausing >= self.pause_every_n_queries:
                self.logger.info(f'pausing for {self.pause_time} sec')
                time.sleep( self.pause_time )
                self.count_for_pausing = 0

    def save( self, data, symbol ):
        # This is for windows users, as the os does not let you create files with these names        
        symbol = self.adjustSavingSymbol( symbol )
        data.to_csv( self.path_archive + '/' + symbol + '.csv' )
 
    def emptyFolder( self ):
        '''Delete all series archived in the folder when you start afresh'''
        self.logger.info('Emptying archive folder...')
        for filename in os.listdir(self.path_archive):
            file_path = os.path.join( self.path_archive, filename )
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                self.logger.debug('Failed to delete %s. Reason: %s' % (file_path, e))

    def setArchivingAlphaVantageOHLCA( self ):
        avr = AlphaVantageRequest()
        self.requestFunction = lambda x: avr.request( 
            symbol   = x,
            function = 'TIME_SERIES_DAILY_ADJUSTED' 
            )
        self.path_archive = getProviderDataPath( provider = 'alphavantage', dtype = 'ohlc' )

    def setArchivingYahooFinanceOHLCA( self ):
        self.requestFunction = lambda x: YahooFinanceRequest( symbol = x )
        self.path_archive = getProviderDataPath( provider = 'yahoofinance', dtype = 'ohlc' )

    def setArchivingSimFimFundamentals( self ):
        sfr = SimFinRequest()
        self.requestFunction = lambda x: sfr.getFundamentals(
            ticker = x,
            year_start = 2000,
            year_end   = 2020,
            periods = ["q1", "q2", "q3", "q4"],
            fundamental_datasets = ['pl', 'bs', 'cf']
            )
        self.path_archive = getProviderDataPath( provider = 'simfin', dtype = 'fundamentals' )

    def download( self, overwrite = False ):
        ''' yes, I know, it downloads stuff right? '''

        assert self.requestFunction != None, "run a 'setArchiving' first" 

        for symbol in self.tickers:
            skip_to_next_ticker = False
            # check whether a file has already been downloaded
            if overwrite == False:        
                if os.path.exists(self.path_archive + '/' + symbol + '.pickle'):
                    self.count_progress += 1
                    continue

            retry_count = 1

            while retry_count <= self.try_retrieve_n_times: # try to retrieve multiple times

                try:     
                    df = self.requestFunction( symbol )

                    if df.empty:
                        self.logger.warning('Empty data for symbol %s, skipping to next', symbol)
                        skip_to_next_ticker = True

                    retry_count = self.try_retrieve_n_times + 1 # this will continue

                except Exception:
                    self.logger.warning( '%s Error in retrieving %s', retry_count, symbol )

                    time.sleep(2)

                    retry_count += 1

                    if retry_count == self.try_retrieve_n_times: # if retriving too many times, still fails, go on
                        self.logger.warning('Giving up on %s', symbol)
                        self.missing_data_ticker_list.append(symbol)
                        skip_to_next_ticker = True
                    continue

            if skip_to_next_ticker:
                continue

            self.save( df, symbol )    
            self.handlePausing()

            self.count_progress += 1
            progress = self.count_progress/self.n_ticker
            self.logger.info('%s processing... %s', '{:.1%}'.format( progress ), symbol )

        # When finished
        self.logger.info( 'Archiving Completed' )
        self.logger.info( 'No data was found for %s tickers', str(len( self.missing_data_ticker_list )) )


def main():

    tickers = [ 'MSFT', 'AMZN' ]

    archiver = Archiver( tickers )

    #archiver.setArchiving_SimFimFundamentals()
    #archiver.download()

    archiver.setArchiving_YahooFinanceOHLCA()
    archiver.download()

if __name__ == '__main__':
    main()