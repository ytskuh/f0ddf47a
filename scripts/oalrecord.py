"""Openinterest and Long Short Rate (Account) table recorder

Run it under project folder to record data in the past month.
"""

#%%
import sys
sys.path.append("..")
import requests
import api

end=api.misc.__btimestamp()-300000
start=end-7*86400*1000

info=api.exchangeinfo('um')
umsymbollist=[x['symbol'] for x in info['symbols']]

#%%
for symbol in umsymbollist:
    count=3
    while(count>0):
        try:
            api.query_series(start, end, market='um', endpoint='oinsts',
            symbol=symbol, interval='5m')
            api.query_series(start, end, market='um', endpoint='lsra',
            symbol=symbol, interval='5m')
            break
        except requests.exceptions.SSLError:
            print("Error")
            count-=1
    print(symbol)
# %%
