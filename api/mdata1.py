"""
Mysql cached database api

Time series data is stored in its original format plus a PRIMARY KEY named
'id'. id = int(t/interval). The setting ensures that id is unique and
incremental.

todo:
    Event series data such as trades
"""
#%%
import math
import time
import pandas as pd
import polars as pl
import mysql.connector
from api import lcp, misc
from api.mdata import COLUMN_NAME
from api.glparam import URLSTR, _TIME_DIVERGENCE_ALLOWED

INDEX_NAME="id"

LIMIT_MAX={
    'kline': 1000,
    'oinsts': 500,
    'lsra': 500
}

def query_series_local(table, start=0, end=0):
    """Query time series data from local database

    select from table where start <= INDEX_NAME < end
    """
    if(start and end):
        data=pl.read_sql(f"SELECT * FROM {table} WHERE {INDEX_NAME} BETWEEN {str(start)} AND {str(end-1)}", lcp.conn)
    else:
        data=pl.read_sql(f"SELECT * FROM {table}", lcp.conn)
    return data

def _reindex(df, col, interval):
    index = (df[col]/interval).values.astype(int)
    df.index = index
    df.index.name = INDEX_NAME
    return df

def _table_name(kw):
    return  f'{kw["market"]}_{kw["endpoint"]}_{kw["symbol"]}_{kw["interval"]}'

TIME_THRESHOLD=3*_TIME_DIVERGENCE_ALLOWED

def _binance_req_series(start, end, **kwargs):
    if end/1000>time.time()-TIME_THRESHOLD:
        raise RuntimeError("Querying for future data")
    if 0<start<1325347200000:
        raise RuntimeError("Querying too old data")
    market=kwargs['market']
    symbol=kwargs['symbol']
    interval=kwargs['interval']
    endpoint=kwargs['endpoint']
    iname=kwargs['iname']
    requestbody={'symbol':symbol, iname:interval, 'limit':LIMIT_MAX[endpoint]}
    urldict=URLSTR[market]
    if start>0 and end>0:
        requestbody['startTime']=start
        requestbody['endTime']=end
    data=misc.binance_req(urldict['rest']+urldict[endpoint], requestbody)
    odata=[kwargs['bprep'](block) for block in data]
    odata=pd.DataFrame(odata, columns=COLUMN_NAME[market][endpoint], dtype=float)
    odata[kwargs['intcol']]=odata[kwargs['intcol']].astype(int)
    return odata

def _query_series_short_remote(start, end, **kwargs):
    """Aggregates series query from legency api mdata"""
    endpoint = kwargs['endpoint']
    dt=misc.i2ms(kwargs['interval'])

    if end-start >= (LIMIT_MAX[endpoint]+1)*dt:
        raise RuntimeError("Query too long")

    if endpoint == 'kline':
        data = _binance_req_series(start, end, **kwargs, iname='interval',
        bprep=lambda x:x, intcol=['t','ct','n'])
    if endpoint == 'oinsts':
        data = _binance_req_series(start, end-dt, **kwargs, iname='period',
        bprep=lambda x:[x['sumOpenInterest'], x['sumOpenInterestValue'],
        x['timestamp']], intcol=['t'])
    if endpoint == 'lsra':
        data = _binance_req_series(start, end-dt, **kwargs, iname='period',
        bprep=lambda x:[x['longShortRatio'], x['longAccount'],
        x['shortAccount'], x['timestamp']], intcol=['t'])
    _reindex(data, 't', dt)
    if start and end:
        si = int(math.ceil(start/dt))
        ei = int(end/dt)
        if len(data) < ei-si:
            data=data.reindex(list(range(si,ei)))
    return data

def create_table(**kwargs):
    """Initialize table for first query"""
    data = _query_series_short_remote(0,0,**kwargs)
    table = _table_name(kwargs)
    data.to_sql(table, lcp.conn, if_exists='fail')
    mydb = mysql.connector.connect(**lcp.mysql_db)
    mycursor = mydb.cursor()
    mycursor.execute(f'ALTER TABLE {table} ADD PRIMARY KEY({INDEX_NAME})')
    mydb.commit()
    mydb.close()

#%%
def query_series(starttime, endtime, **kwargs):
    """The final time series data query api with cache

    Note: endtime mustn't be newer than current time. Otherwire future data will be contaminated.

    This api is used to query time series data - kline, open interest and long short ratio.

    Queryed data will be stored in the mysql database.

    Table description example:

    ::

        MariaDB [biandb]> desc um_oinsts_BTCUSDT_5m;
        +-------+------------+------+-----+---------+-------+
        | Field | Type       | Null | Key | Default | Extra |
        +-------+------------+------+-----+---------+-------+
        | id    | bigint(20) | NO   | PRI | NULL    |       |
        | i     | double     | YES  |     | NULL    |       |
        | iv    | double     | YES  |     | NULL    |       |
        | t     | bigint(20) | YES  |     | NULL    |       |
        +-------+------------+------+-----+---------+-------+
        4 rows in set (0.000 sec)

    Args:
        starttime (int): Start time in milliseconds timestamp.
        endtime (int): End time in milliseconds timestamp.
        kwargs (dict): Graph arguments.
        market (str): 'spot', 'um' or 'cm'.
        endpoint (str): 'kline', 'oinsts', 'lsra'
        symbol (str): symbol of querying pair
        interval (str): Interval or period

    Return:
        Polars dataframe of the data with the 't' key (Open time or time) satisfying
        starttime <= t < endtime

    Examples:
        >>> graph={'market':'spot', 'endpoint':'kline', 'symbol':'BTCUSDT',
        'interval':5m}
        >>> data=api.query_series(0, 0, **graph)
        >>> # When 0, 0 start and time is given, local data is returned

    todo:
        Support for Index price, Marked price and top account long short
        ratio.
    """
    table = _table_name(kwargs)
    dt=misc.i2ms(kwargs["interval"])
    si=int(math.ceil(starttime/dt))
    ei=int(endtime/dt)
    data = query_series_local(table, si, ei)
    if len(data) == ei-si:
        return data
    lm = LIMIT_MAX[kwargs["endpoint"]]
    p=si
    while p < ei:
#        print(p)
        q=min(p+lm, ei)
        data = query_series_local(table, p, q)
        if len(data) != q-p:
            data1 = _query_series_short_remote(p*dt, q*dt+dt-1, **kwargs)
            local=set(data['id'])
            counter=list(set(range(p, q)).difference(local))
            data2=data1.loc[counter]
            data2.to_sql(table, lcp.conn, if_exists='append')
        p+=lm
    return query_series_local(table, si, ei)

# %%
