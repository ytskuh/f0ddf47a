#%%
import time
import api
graph={
    'market':'um',
    'endpoint':'kline',
    'symbol':'BTCUSDT',
    'interval':'5m'
    }
api.create_table(**graph)
start=api.misc.t2ts("2022-07-01 00:00:00")
end=api.misc.t2ts("2022-07-11 00:00:00")

print("First query starts")
a=time.time()
d=api.query_series(start, end, **graph)
b=time.time()
print("First query ends")
print(f"Time consumed: {b-a} seconds")
#%%
print("Second query starts")
a=time.time()
d=api.query_series(start, end, **graph)
b=time.time()
print("Second query ends")
print(f"Time consumed: {b-a} seconds")
# %%
