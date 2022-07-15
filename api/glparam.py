"""
This file contains some global parameters such as api endpoints
"""

_TIME_DIVERGENCE_ALLOWED = 2
_DELAY_ALLOWED = 0.6
IS_TESTNET = False

URLSTR={
    'db':'https://data.binance.vision/data',
    'spot': {
        'rest':'https://api.binance.com',
        'ws':'wss://stream.binance.com:9443',
        'ping':'/api/v3/ping',
        'time':'/api/v3/time',
        'info':'/api/v3/exchangeInfo',
        'depth':'/api/v3/depth',
        'trades':'/api/v3/trades',
        'htrades':'/api/v3/historicalTrades',
        'aggtrades':'/api/v3/aggTrades',
        'kline':'/api/v3/klines',
        'avgprice':'/api/v3/avgPrice',
        'price':'/api/v3/price',
    },
    'um': {
        'rest':'https://fapi.binance.com',
        'ws':'wss://fstream.binance.com',
        'ping':'/fapi/v1/ping',
        'time':'/fapi/v1/time',
        'info':'/fapi/v1/exchangeInfo',
        'depth':'/fapi/v1/depth',
        'trades':'/fapi/v1/trades',
        'htrades':'/fapi/v1/historicalTrades',
        'aggtrades':'/fapi/v1/aggTrades',
        'kline':'/fapi/v1/klines',
        'oinst':'/fapi/v1/openInterest',
        'oinsts':'/futures/data/openInterestHist',
        'lsra':'/futures/data/globalLongShortAccountRatio',
        'lsrta':'/futures/data/topLongShortAccountRatio',
        'lsrtp':'/futures/data/topLongShortPositionRatio'
    },
    'cm': {
        'rest':'https://dapi.binance.com',
        'ws':'wss://dstream.binance.com',
        'ping':'/dapi/v1/ping',
        'time':'/dapi/v1/time',
        'info':'/dapi/v1/exchangeInfo',
        'depth':'/dapi/v1/depth',
        'trades':'/dapi/v1/trades',
        'htrades':'/dapi/v1/historicalTrades',
        'aggtrades':'/dapi/v1/aggTrades',
        'kline':'/dapi/v1/klines',
        'oinst':'/dapi/v1/openInterest',
        'oinsts':'/futures/data/openInterestHist',
        'lsra':'/futures/data/globalLongShortAccountRatio'
    }
}

URLSTRT={
    'spot':{
        
    },
    'um':{
        'order':'/fapi/v1/order',
        'border':'/fapi/v1/batchOrders'
    }
}

if IS_TESTNET:
    URLSTR['spot']['rest']=''

TIMEFORMAT='%Y-%m-%d %H:%M:%S'
