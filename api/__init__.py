"""
Binance data and trade api
===========================
Provides advanced api for data fetching and trading operations.

Currently, only two modules are suggested to use: mdata1 and misc.

mdata is the deprecated module to fetch data from binance server.

mdata1 is the new module to fetch data from binance api.
Results are cached in a mysql database for fast query.

misc contains many useful functions.
"""

from tracemalloc import start
import warnings
import time

from pandas import test
from .misc import binance_req
from .glparam import URLSTR, _TIME_DIVERGENCE_ALLOWED, _DELAY_ALLOWED

from .mdata import exchangeinfo
from .mdata1 import create_table, query_series

def test_connectivity(market):
    """Test the connectivity of 'market' api server"""
    urldict=URLSTR[market]
    starttime=time.time()
    req = binance_req(urldict['rest']+urldict['ping'],{})
    if req=={}:
        return time.time()-starttime
    return 9999

if max((test_connectivity('spot'), test_connectivity('um'),
test_connectivity('cm')))>_DELAY_ALLOWED:
    raise RuntimeError("Network delay too long")

def server_time(market):
    """Get server time fo 'market' api server"""
    urldict=URLSTR[market]
    req = binance_req(urldict['rest']+urldict['time'],{})
    return req['serverTime']

if abs(time.time()-server_time('spot')/1000)>_TIME_DIVERGENCE_ALLOWED:
    raise RuntimeError("Inaccurate local time")

__all__ = ['exchangeinfo','create_table', 'query_series']
