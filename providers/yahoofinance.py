__author__ = 'Antonio Orsini'
__doc__    = 'Yahoo finance'

from yfinance import Ticker

def YahooFinanceRequest( symbol ):
    return Ticker( symbol ).history( period = '10y', auto_adjust = True )
