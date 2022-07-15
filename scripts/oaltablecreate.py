"""Openinterest and Long Short Rate (Account) table initializor

Run it under project folder once to create the tables
"""

#%%
import sys
sys.path.append("..")
import requests
import api

info=api.exchangeinfo('um')
umsymbollist=[x['symbol'] for x in info['symbols']]

#%%
for symbol in umsymbollist:
    count=3
    while(count>0):
        try:
            api.create_table(market='um', endpoint='oinsts', symbol=symbol, interval='5m')
            api.create_table(market='um', endpoint='lsra', symbol=symbol, interval='5m')
            break
        except requests.exceptions.SSLError:
            count-=1
    print(symbol)

# %%
