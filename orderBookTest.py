import os
import time
import datetime
import requests
import pandas as pd

SLEEPDELAY  = 5

testcount = 0
while(testcount < 2):
    testcount += 1
    #response = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=10')
    #print (response.text)

    book = {}
    response = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book = response.json()
    #print (response.status_code)

    data = book['data']

    print (data)

    epochTimestamp = data['timestamp']  #milliseconds
    convertedTimestamp = datetime.datetime.fromtimestamp(float(epochTimestamp)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True)
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0
    bids['timestamp'] = convertedTimestamp
    
    
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1
    asks['timestamp'] = convertedTimestamp
    
    # Pandas lib issue. change to concat
    # df = bids.append(asks)
    df = pd.concat([bids, asks])

    print (df)

    # Remove data frame index. If file exists skip header
    filename = convertedTimestamp[:10] + "-bithumb-BTC-orderbook.csv"
    if not os.path.isfile(filename):
        df.to_csv(filename, index=False)
    else:
        df.to_csv(filename, index=False, mode='a', header=False)
    
    time.sleep(SLEEPDELAY)
