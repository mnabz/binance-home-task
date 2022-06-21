#!/usr/bin/env python

import requests
import pandas as pd
import time
from prometheus_client import start_http_server, Gauge

TOP5_URL = 'https://api.binance.com/api/v3/ticker/24hr'
req1 = requests.get(TOP5_URL)

NOTIONAL_URL = 'https://api.binance.com/api/v3/depth'

SPREAD_URL = 'https://api.binance.com/api/v3/ticker/bookTicker'

ABSOLUTE_DELTA = Gauge('absolute_delta',
                        'Absolute Delta',
                         ['symbol'])

# Question 1
def five_symbols_btc_highest_volume_descending(printResult):

    df = pd.DataFrame(req1.json())
    df = df[['symbol', 'volume']]
    df = df[df.symbol.str.contains(r'(?!$){}$'.format('BTC'))]
    df['volume'] = pd.to_numeric(df['volume'], downcast='float')
    result = df.sort_values(by=['volume'], ascending=False)
    result = result.head(5)
    if printResult:
        print("\n Question 1\n")
        print("Top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order\n ")
        print(result.to_dict('records'))
        print("\n")
    return result


five_symbols_btc_highest_volume_descending(True)


# Question 2
def five_symbols_usdt_highest_trades_descending(printResult):

    df = pd.DataFrame(req1.json())
    df = df[['symbol', 'count']]
    df = df[df.symbol.str.contains(r'(?!$){}$'.format('USDT'))]
    df['count'] = pd.to_numeric(df['count'], downcast='float')
    result = df.sort_values(by=['count'], ascending=False)
    result = result.head(5)
    if printResult:
        print("\n Question 2\n")
        print("Top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order\n ")
        print(result.to_dict('records'))
        print("\n")
    return result

five_symbols_usdt_highest_trades_descending(True)


# Question 3
def total_notional_value_top_200_bids_asks():

    pairs = five_symbols_btc_highest_volume_descending(False).to_dict('records')
    result = {}
    for i in pairs:
        args = { 'symbol' : i['symbol'] }
        req2 = requests.get(NOTIONAL_URL, params=args)
        symbols = req2.json()
        for y in ["bids", "asks"]:
            df = pd.DataFrame(data=symbols[y], columns=["price", "quantity"], dtype=float)
            df = df.sort_values(by=['price'], ascending=False).head(200)
            df['notional'] = df['price'] * df['quantity']
            result[i['symbol']+'_'+y] = df['notional'].sum()
    print("\n Question 3\n")
    print("Total Notional value of the top 200 bids-and-asks\n ")
    print(result)
    print("\n")
    return result

total_notional_value_top_200_bids_asks()


# Question 4
def price_spread(printResult):

    pairs = five_symbols_usdt_highest_trades_descending(False)
    result = {}
    for i in pairs['symbol']:
        args = { 'symbol' : i }
        req3 = requests.get(SPREAD_URL, params=args)
        price_spread = req3.json()
        result[i] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])
    if printResult:
        print("\n Question 4\n")
        print("Price Spread for Q2 pairs\n ")
        print(result)
        print("\n")
    return result

price_spread(True)


# Question 5
def absolute_delta(printResult):

    old_spread = price_spread(False)
    time.sleep(10)
    new_spread = price_spread(False)
    result = {}
    for i in old_spread:
        result[i] = abs(old_spread[i]-new_spread[i])
    if printResult:
        print("\n Question 5\n")
        print("Absolute Delta from Q4 pairs\n ")
        print(result)
        print("\n")
    return result

absolute_delta(True)


# Question 6
if __name__ == "__main__":
    start_http_server(8080)
    print("\n Question 6\n")
    print("Prometheus metrics available on port 8080\n")
    while True:
        metrics = absolute_delta(False)
        for j in metrics:
            ABSOLUTE_DELTA.labels(j).set(metrics[j])