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

webhook_url = environ['WEBHOOK_URL']
alpha_vantage_key = environ['ALPHA_VANTAGE_KEY']
MONGO_DB = environ["MONGO_DB"]
a = arrow.now('US/Central')
minutes = int(a.format('mm'))
hours = int(a.format('HH'))
less_than_ten = minutes > 58 and minutes < 8
thirty_ish = minutes > 28 and minutes < 38
after_nine = hours > 8
before_three = hours < 3

if (less_than_ten or thirty_ish) and after_nine and before_three:
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
            difference.append({"ticker": ticker, "difference": item['mentions'] - yesterdays_formatted_data[ticker]})
        except:
            continue

    difference.sort(key=lambda x: x.get('difference'), reverse=True)
    todays_formatted_data.sort(key=lambda x: x.get('mentions'), reverse=True)
    top_by_mentions = todays_formatted_data[0:9]
    top_by_increase = difference[0:19]

    for ticker in top_by_increase:
        try:
            data, meta_data = ts.get_intraday(symbol=ticker['ticker'], interval='30min', outputsize='compact')
            list_data = data.values.tolist()
            ticker["volume"] = str(int(round(list_data[0][4], 0)))
            ticker["volume_change"] = str(int(round(list_data[0][4] - list_data[1][4], 0)))
            ticker["volume_change_percentage"] = str(round(((list_data[0][4] - list_data[1][4]) / list_data[1][4]) * 100, 2))
            ticker["thirty_min_change"] = str(round(list_data[0][3] - list_data[0][0], 2))
            ticker["one_hour_change"] = str(round(list_data[0][3] - list_data[1][0], 2))
            ticker["thirty_min_change_percentage"] = str(round(((list_data[0][3] - list_data[0][0]) / list_data[0][0]) * 100, 2))
            ticker["one_hour_change_percentage"] = str(round(((list_data[0][3] - list_data[1][0]) / list_data[1][0]) * 100, 2))
            ticker["price"] = str(round(list_data[0][3], 2))
            time.sleep(13)
        except: 
            continue

    message = 'Top Twenty Tickers By Increase In Mentions Since Yesterday: \n'
    message_two = ''
    for ticker in top_by_increase[0:9]:
        try:
            link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
            mentions = "```" + str(ticker["difference"]) + " more mentions than yesterday. \n"
            price = "Currently $" + ticker["price"] + "\n"
            price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
            price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
            volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
            volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. ``` \n" 
            message += link + mentions + price + price_thirty + price_sixty + volume + volume_change
        except:
            message += "Error fetching info for " + ticker["ticker"] + ". \n"
        
    for ticker in top_by_increase[10:19]:
        try:
            link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
            mentions = "```" + str(ticker["difference"]) + " more mentions than yesterday. \n"
            price = "Currently $" + ticker["price"] + ". \n"
            price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
            price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
            volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
            volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. ``` \n" 
            message_two += link + mentions + price + price_thirty + price_sixty + volume + volume_change
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
    slack_data = {'text': message_two}
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



