import os
import time
import datetime
import requests
import pandas as pd

SLEEPDELAY  = 5 #seconds


def main():
    testcount = 0
    testInfinite = True

    while(testcount < 18000):
        testcount += 1

        recordOderbook('BTC')
        time.sleep(1)
        recordOderbook('ETH')

        time.sleep(SLEEPDELAY-1)


def fetchOrderbook(targetSymbol):
    book = {}
    response = requests.get ('https://api.bithumb.com/public/orderbook/' + targetSymbol + '/?count=5')
    if response.status_code == 200:
        book = response.json()
        print(targetSymbol + ' get result: ' + str(response.status_code))
        return book
    else:
        return ""


def recordOderbook(coinName):
    symbolName = coinName + '_KRW'
    bookResult = fetchOrderbook(symbolName)
    if bookResult == "":
        print('Data get fail')
        return
    #print (bookResult)
    data = bookResult['data']

    #print (data)

    epochTimestamp = data['timestamp']  #milliseconds
    convertedTimestamp = datetime.datetime.fromtimestamp(float(epochTimestamp)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')

    #bid(buy) order book
    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
    bids.sort_values('price', ascending=False, inplace=True)
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0
    bids['timestamp'] = convertedTimestamp

    #ask(sell) order book
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1
    asks['timestamp'] = convertedTimestamp

    # Pandas lib version changes.use concat
    # df = bids.append(asks)
    df = pd.concat([bids, asks])
    #print (df)

    # Remove data frame index. If file exists skip header
    filename = convertedTimestamp[:10] + '-bithumb-' + coinName + '-orderbook.csv'
    if not os.path.isfile(filename):
        df.to_csv(filename, index=False)
    else:
        df.to_csv(filename, index=False, mode='a', header=False)


main()
