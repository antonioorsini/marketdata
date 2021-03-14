from keys.corepaths import path_marektdata

path_single_series        = path_marketdata + '/single_series'
path_indicators           = path_marketdata + '/indicators'
path_datasets             = path_marketdata + '/datasets'
path_complete_datasets    = path_marketdata + '/datasets'   
path_tickers              = path_marketdata + '/tickers'

def getProviderDataPath( provider, dtype ):
    return path_marketdata + '/providers'+ '/' + provider + '/' + dtype



