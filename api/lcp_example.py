# pylint: skip-file

"""
An example of lcp.py

Rename it to lcp.py and fill it with your own value
"""

PROXY_SERVER='http://localhost:7890'
'''Your local proxy server'''

PROXY={'https':PROXY_SERVER,'http':PROXY_SERVER}

STORE_PATH='/run/media/fred/storage/bian'
'''Path of local binance database'''

mysql_db={
    'host':'localhost',
    'user':'bdadmin',
    'password':'91f3i',
#    'password':'FeyTLpat',
    'database':'biandb'
}
'''Mysql connector'''

conn=f"mysql://{mysql_db['user']}:{mysql_db['password']}@{mysql_db['host']}:3306/{mysql_db['database']}"
'''Sql connection URI'''

APIKEY_TESTNET='lBgsc3ga3X4jmxJgYBbClclZvPQgmhw4WqZ9mZMhnpQh4sS6zKphtJAoPulZ7hRj'
SECRETKEY_TESTNET='nP3I8Q1Qtoc7SAv9Ubl33XG0JeKieWDiBJOjIZA81NmsxD2saPM8ov7V2bo57Q5Z'

APIKEY=APIKEY_TESTNET
'''Your api key'''
SECRETKEY=SECRETKEY_TESTNET
'''Your secret key'''
