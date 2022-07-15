"""Temporary data api

The module is used to fetch large amounts of data from binance vision.

"""

import os
import zipfile
import datetime
import warnings
import pandas as pd
from dateutil.relativedelta import relativedelta

from .glparam import URLSTR
from .lcp import STORE_PATH
from .lcp import PROXY_SERVER
from .misc import binance_req

COLUMN_NAME={
    'spot':{
        'kline':['t','o','h','l','c','v','ct','qv','n','tbv','tqv','i'],
        'aggtrades':['a','p','q','f','l','t','m','M'],
        'trades':['id','p','q','Q','t','m','M'],
    },
    'um':{
        'kline':['t','o','h','l','c','v','ct','qv','n','tbv','tqv','i'],
        'aggtrades':['a','p','q','f','l','t','m'],
        'trades':['id','p','q','Q','t','m'],
        'oinsts':['i','iv','t'],
        'lsra':['r','l','s','t']
    },
    'cm':{
        'kline':['t','o','h','l','c','v','ct','bv','n','tv','tbv','i'],
        'aggtrades':['a','p','q','f','l','t','m'],
        'trades':['id','p','q','bQ','t','m'],
        'oinsts':['i','iv','t'],
        'lsra':['r','l','s','t']
    }
}

strptime=datetime.datetime.strptime
strftime=datetime.datetime.strftime

def _monthspilt(begin, end):
    utc=datetime.timezone.utc
    a=datetime.datetime.utcfromtimestamp(begin).replace(tzinfo=utc)
    b=datetime.datetime.utcfromtimestamp(end).replace(tzinfo=utc)
    c=datetime.datetime.utcnow().replace(tzinfo=utc)

    day=[]
    if (c-b).days<31:
        d=b-relativedelta(days=31)
        sd=strptime(strftime(max(a,d),'%Y%m%d'),'%Y%m%d').replace(tzinfo=utc)
        ed=strptime(strftime(b,'%Y%m%d'),'%Y%m%d').replace(tzinfo=utc)

        while sd<=ed:
            ts=max(int(max(a,d).timestamp()*1000),int(sd.timestamp()*1000))
            te=min(int(b.timestamp()*1000),int((sd+relativedelta(days=1)).timestamp()*1000))
            day.append((strftime(sd,'%Y-%m-%d'),ts,te))
            sd+=relativedelta(days=1)
        b=d

    sm=strptime(strftime(a,'%Y%m'),'%Y%m').replace(tzinfo=utc)
    em=strptime(strftime(b,'%Y%m'),'%Y%m').replace(tzinfo=utc)
    month=[]
    while sm<=em:
        ts=max(int(a.timestamp()*1000),int(sm.timestamp()*1000))
        te=min(int(b.timestamp()*1000),int((sm+relativedelta(months=1)).timestamp()*1000))
        month.append((strftime(sm,'%Y-%m'),ts,te))
        sm+=relativedelta(months=1)
    return month,day

class Trades:
    '''Package for market endpoint apis

    A trade object refers to a trading pair in either spot, um or cm market.

    Attributes:
        type (str): market type, 'spot', 'cm', or 'um'.

    Examples:
        To query about the BTCUSDT pair in USD-M futures market

        >>> btcusdtperp=api.Trades('BTCUSDT', 'um')
        >>> data=btcusdtperp._kline_stored_daily('2021-01-01')
    '''

    def __init__(self, _symbol, _arg='spot'):
        """Initialize a Trades object

        """
        warnings.warn("Trades class is deprecated", DeprecationWarning)
        self.type=_arg

        if 'spot' == _arg:
            self.typath='/'+self.type
        if 'cm' == _arg:
            self.typath='/futures/'+self.type
        if 'um' == _arg:
            self.typath='/futures/'+self.type

        self.baseurl=URLSTR[self.type]['rest']
        self.db=STORE_PATH+'/data'+self.typath
        self.symbol=_symbol
        self.visionurl=URLSTR['db']+self.typath

    def _kline_short(self, sts, ets, interval):
        '''
        Request the raw kline from binance api.
        The starttime and endtime are ms timestamp.
        '''
        requestbody={'symbol':self.symbol,'interval':interval,'limit':1000}
        if sts and ets:
            requestbody['startTime']=sts
            requestbody['endTime']=ets

        data=binance_req(self.baseurl+URLSTR[self.type]['kline'],requestbody)
#        print(data)
        odata=pd.DataFrame(data,columns=COLUMN_NAME[self.type]['kline'], dtype=float)
        return odata

    def __binance_vision(self, folder, filename):
        download_url=self.visionurl+folder+filename+'.zip'
        save_path=self.db+folder+filename+'.zip'
        csv_path=self.db+folder+filename+'.csv'
        save_folder=self.db+folder
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)
        if not os.path.exists(csv_path):
#            print('curl -s {} -o "{}" --proxy {}'.format(download_url, save_path, proxy['https']))
            os.system(f'curl -s {download_url} -o "{save_path}" --proxy {PROXY_SERVER}')
            with zipfile.ZipFile(save_path) as zfile:
                zfile.extractall(save_folder)
            return True
        return False

    def _kline_stored_monthly(self, mo, interval, mask=None):
        # Fetch data from data.binance.vision and downloaded files
        # mo='YYYY-MM'
        folder=f'/monthly/klines/{self.symbol}/{interval}'
        filename=f'/{self.symbol}-{interval}-{mo}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['kline'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def _aggtrade_stored_monthly(self, mo, mask=None):
        folder=f'/monthly/aggTrades/{self.symbol}'
        filename=f'/{self.symbol}-aggTrades-{mo}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['aggtrades'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def _trades_stored_monthly(self, mo, mask=None):
        folder=f'/monthly/trades/{self.symbol}'
        filename=f'/{self.symbol}-trades-{mo}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['trades'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def _kline_stored_daily(self, day, interval, mask=None):
        # Fetch data from data.binance.vision and downloaded files
        # day='YYYY-MM-DD'
        folder=f'/daily/klines/{self.symbol}/{interval}'
        filename=f'/{self.symbol}-{interval}-{day}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['kline'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def _aggtrade_stored_daily(self, day, mask=None):
        folder=f'/daily/aggTrades/{self.symbol}'
        filename=f'/{self.symbol}-aggTrades-{day}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['aggtrades'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def _trades_stored_daily(self, day, mask=None):
        folder=f'/daily/trades/{self.symbol}'
        filename=f'/{self.symbol}-trades-{day}'
        self.__binance_vision(folder, filename)
        data=pd.read_csv(self.db+folder+filename+'.csv', names=COLUMN_NAME[self.type]['trades'])
        if not mask:
            return data
        return data[(mask[0]<=data['t'])*(data['t']<mask[1])]

    def kline(self, start, end, interval):
        data=[]
        month,day=_monthspilt(start,end)
        for m in month:
            data.append(self._kline_stored_monthly(m[0],interval,(m[1],m[2])))
        for d in day:
            data.append(self._kline_stored_daily(d[0],interval,(d[1],d[2])))
        return pd.concat(data).reset_index(drop=True)

    def aggtrade(self, start, end):
        data=[]
        month,day=_monthspilt(start,end)
        for m in month:
            data.append(self._aggtrade_stored_monthly(m[0],(m[1],m[2])))
        for d in day:
            data.append(self._aggtrade_stored_daily(d[0],(d[1],d[2])))
        return pd.concat(data).reset_index(drop=True)

    def trades(self, start, end):
        data=[]
        month,day=_monthspilt(start,end)
        for m in month:
            data.append(self._trades_stored_monthly(m[0],(m[1],m[2])))
        for d in day:
            data.append(self._trades_stored_daily(d[0],(d[1],d[2])))
        return pd.concat(data).reset_index(drop=True)

    def _openinst_short(self, sts, ets, interval):
        requestbody={'symbol':self.symbol, 'period':interval, 'limit':500}
        if sts and ets:
            requestbody['startTime']=sts
            requestbody['endTime']=ets
#        print(requestbody)
        data=binance_req(self.baseurl+'/futures/data/openInterestHist', requestbody)
        odata=[]
        for block in data:
            odata.append([
                float(block['sumOpenInterest']),
                float(block['sumOpenInterestValue']),
                block['timestamp']])
        return pd.DataFrame(odata,columns=['i','iv','t'])

    def _lsr_short(self, sts, ets, interval, top=False, position=False):
        requestbody={'symbol':self.symbol, 'period':interval, 'limit':500}
        if sts and ets:
            requestbody['startTime']=sts
            requestbody['endTime']=ets
        if top:
            if position:
                tail='/futures/data/topLongShortPositionRatio'
            else: tail='/futures/data/topLongShortAccountRatio'
        else: tail='/futures/data/globalLongShortAccountRatio'
        data=binance_req(self.baseurl+tail, requestbody)
        odata=[]
        for block in data:
            odata.append([
                float(block['longShortRatio']),
                float(block['longAccount']),
                float(block['shortAccount']),
                block['timestamp']
            ])
        return pd.DataFrame(odata, columns=['r','l','s','t'])

def exchangeinfo(market='spot'):
    """Returns exchangesInfo.

    Args:
        market (str): 'spot', 'cm' or 'um'.

    Returns:
        Current exchange trading rules and symbol information
    """
    return binance_req(URLSTR[market]['rest']+URLSTR[market]['info'], {})
