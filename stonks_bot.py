import re
import arrow
from pymongo import MongoClient
from datetime import datetime, date, timedelta
from datetime import time as dtime
import time
import requests
import json
from os import environ
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf

webhook_url = environ['WEBHOOK_URL']
alpha_vantage_key = environ['ALPHA_VANTAGE_KEY']
MONGO_DB = environ["MONGO_DB"]
a = arrow.now('US/Central')
minutes = int(a.format('mm'))
hours = int(a.format('HH'))
day = int(a.format('d'))
weekday = day > 1 and day < 7
less_than_ten = minutes > 58 or minutes < 8
thirty_ish = minutes > 28 and minutes < 38
after_nine = hours > 8
before_three = hours < 15

if (less_than_ten or thirty_ish) and after_nine and before_three and weekday:
    ts = TimeSeries(key=alpha_vantage_key, output_format='pandas')
    cluster = MongoClient(MONGO_DB)
    db = cluster["wsb_momentum"]
    collection = db["daily_mentions"]
    todays_date = date.today()
    yesterday_date = todays_date - timedelta(days = 1)
    todays_data = collection.find_one({"date": str(todays_date)})
    yesterdays_data = collection.find_one({"date": str(yesterday_date)})
    yesterdays_tickers = yesterdays_data['tickers']
    todays_tickers = todays_data['tickers']
    todays_formatted_data = []
    yesterdays_formatted_data = {}
    difference = []

    for ticker in todays_tickers:
        todays_formatted_data.append({'ticker': ticker['ticker'], 'mentions': ticker['mentions'][0]})

    for ticker in yesterdays_tickers:
        yesterdays_formatted_data[ticker['ticker']] = ticker['mentions'][0]

    for item in todays_formatted_data:
        ticker = item['ticker']
        try:
            difference.append({"ticker": ticker, "difference": round(item['mentions'] * (24 / hours), 0) - yesterdays_formatted_data[ticker]})
        except:
            continue

    difference.sort(key=lambda x: x.get('difference'), reverse=True)
    todays_formatted_data.sort(key=lambda x: x.get('mentions'), reverse=True)
    top_by_mentions = todays_formatted_data[0:9]
    top_by_increase = difference[0:19]
    top_by_increase.sort(key=lambda x: x.get('difference'))

    for ticker in top_by_increase:
        yahoo_data = yf.Ticker(ticker["ticker"])
        list_yahoo_data = yahoo_data.history(period="5d").values.tolist()
        data, meta_data = ts.get_intraday(symbol=ticker['ticker'], interval='30min', outputsize='compact')
        list_data = data.values.tolist()
        ticker["open"] = str(round(list_yahoo_data[4][0], 2))
        ticker["change_since_open"] = str(round(list_data[0][3] - list_yahoo_data[4][0], 2))
        ticker["change_since_open_percentage"] = str(round((list_data[0][3] - list_yahoo_data[4][0]) / list_yahoo_data[4][0], 2))
        ticker["yesterday_open"] = str(round(list_yahoo_data[3][0], 2))
        ticker["yesterday_high"] =str(round(list_yahoo_data[3][1], 2))
        ticker["yesterday_low"] = str(round(list_yahoo_data[3][2], 2))
        ticker["yesterday_close"] = str(round(list_yahoo_data[3][3], 2))
        ticker["volume"] = str(int(round(list_data[0][4], 0)))
        ticker["volume_change"] = str(int(round(list_data[0][4] - list_data[1][4], 0)))
        ticker["volume_change_percentage"] = str(round(((list_data[0][4] - list_data[1][4]) / list_data[1][4]) * 100, 2))
        ticker["thirty_min_change"] = str(round(list_data[0][3] - list_data[0][0], 2))
        ticker["one_hour_change"] = str(round(list_data[0][3] - list_data[1][0], 2))
        ticker["thirty_min_change_percentage"] = str(round(((list_data[0][3] - list_data[0][0]) / list_data[0][0]) * 100, 2))
        ticker["one_hour_change_percentage"] = str(round(((list_data[0][3] - list_data[1][0]) / list_data[1][0]) * 100, 2))
        ticker["price"] = str(round(list_data[0][3], 2))
        time.sleep(13)
        

    message = 'Top Twenty Tickers By Increase In Mentions Since Yesterday: \n'
    message_two = ''
    website = "\n <https://wsb-data.vercel.app/|Investigate these tickers further> \n"
    for ticker in top_by_increase[0:9]:
        try:
            link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
            mentions = "```On pace for " + str(int(ticker["difference"])) + " more mentions than yesterday. \n"
            price = "Currently $" + ticker["price"] + "\n"
            opened = "Opened at $" + ticker["open"] + "\n"
            open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
            price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
            price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
            volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
            volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
            yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
            message += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
        except:
            message += "Error fetching info for " + ticker["ticker"] + ". \n"

    for ticker in top_by_increase[10:19]:
        try:
            link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
            mentions = "```On pace for " + str(int(ticker["difference"])) + " more mentions than yesterday. \n"
            price = "Currently $" + ticker["price"] + "\n"
            opened = "Opened at $" + ticker["open"] + "\n"
            open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
            price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
            price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
            volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
            volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
            yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
            message_two += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
        except:
            message_two += "Error fetching for " + ticker["ticker"] + ". \n"

    slack_data = {'text': message}
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
    )
    time.sleep(1)
    slack_data = {'text': message_two + website}
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
    )

else:
    print('not running')
    print(str(hours) + ":" + str(minutes))
    quit()



