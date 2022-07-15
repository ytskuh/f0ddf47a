"""
Messy but important functions
"""

import time
import hmac
import hashlib
import requests
from . import lcp
from .glparam import TIMEFORMAT

def i2ms(tstr):
    """
    Convert binance's form of interval to milliseconds.

    Examples:
        >>> i2ms('4h')
        14400000
    """
    if not tstr:
        return 0
    mst={'s':1000,'m':60000,'h':3600000,'d':86400000,'w':604800000}
    return int(tstr[:-1])*mst[tstr[-1]]

def t2ts(timestr):
    '''
    Convert timestring in TIMEFORMAT to UTC timestamp in milliseconds.
    '''
    return int(time.mktime(time.strptime(timestr,TIMEFORMAT))
    -time.mktime(time.gmtime(0)))*1000

def ts2t(times):
    '''
    Convert UTC timestamp in milliseconds to timestring in TIMEFORMAT.
    '''
    return time.strftime(TIMEFORMAT,time.gmtime(times/1000))

def binance_req(url, body):
    '''Binance RESTful API without authorization

    Args:
        url (str): URL to request
        body (dict): Request body

    Examples:
        >>> print(binance_req("https://api.binance.com/api/v3/time"))
        {'serverTime': 1657363937756}
    '''
#    print(url, body)
    req=requests.get(url, params=body, proxies=lcp.PROXY)
    return req.json()

def _hmac_sha256(data, secret):
    signature = hmac.new(bytes(secret, encoding='utf-8'), bytes(data,
    encoding='utf-8'), digestmod=hashlib.sha256).digest()
    return signature.hex()

def __btimestamp():
    return int(time.time()*1000)

def __formreqbody(para):
    return "&".join([str(x)+"="+str(para[x]) for x in para])

def binance_auth_req(url,method,requestbody):
    '''Binance RESTful API requires authorization

    API used to send trading related requests.

    Args:
        url (str): URL to request
        method (function): Request method to use
        requestbody (dict): Request body without timestamp and signature

    Examples:
        >>> binance_auth_req('https://fapi.binance.com/fapi/v1/order',
        requests.post, {'symbol':'USDCUSDT', 'side': 'BUY', 'type':
        'MARKET', 'quantity':11})
    '''
    header={"X-MBX-APIKEY": lcp.APIKEY}
    requestbody["timestamp"]=__btimestamp()
    requestbody=__formreqbody(requestbody)
    sign=_hmac_sha256(requestbody,lcp.SECRETKEY)
    rurl=url+"?"+requestbody+"&signature="+sign
    req=method(rurl, headers=header, proxies=lcp.PROXY)
    return req.json()
