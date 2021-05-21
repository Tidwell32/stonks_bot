import re
import arrow
from pymongo import MongoClient
from datetime import datetime, date, timedelta
from datetime import time as dtime
import time
import requests
import json
from os import environ
import yfinance as yf
from twelvedata import TDClient

webhook_url = environ['WEBHOOK_URL']
MONGO_DB = environ["MONGO_DB"]
TWELVE_DATA_API_KEY = environ["TWELVE_DATA_API_KEY"]

def run(hours):
    start = time.process_time()
    td = TDClient(apikey=TWELVE_DATA_API_KEY)
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
    coef_list = [
        51.6,
        27.2,
        21.1,
        16.8,
        14.7,
        9.4,
        7.9,
        6.3,
        4.6,
        3.6,
        3,
        2.6,
        2.2,
        2,
        1.6,
        1.5,
        1.4,
        1.35,
        1.3,
        1.25,
        1.2,
        1.15,
        1.1,
        1,
        1
    ]

    for ticker in todays_tickers:
        todays_formatted_data.append({'ticker': ticker['ticker'], 'mentions': ticker['mentions'][0]})

    for ticker in yesterdays_tickers:
        yesterdays_formatted_data[ticker['ticker']] = ticker['mentions'][0]

    for item in todays_formatted_data:
        ticker = item['ticker']
        try:
            difference.append({"ticker": ticker, "mentions": item['mentions'], "difference": round(item['mentions'] * coef_list[hours], 0) - yesterdays_formatted_data[ticker], "difference_percentage": round((((item['mentions'] * coef_list[hours]) - yesterdays_formatted_data[ticker]) / yesterdays_formatted_data[ticker]) * 100, 2)})
        except:
            pass

    difference.sort(key=lambda x: x.get('difference'), reverse=True)
    todays_formatted_data.sort(key=lambda x: x.get('mentions'), reverse=True)
    top_by_mentions = todays_formatted_data[0:9]
    top_by_increase = difference[0:19]
    top_by_increase.sort(key=lambda x: x.get('difference_percentage'))

    for ticker in top_by_increase:
        volume_last_thirty = 0
        volume_previous_thirty = 0
        yahoo_data = yf.Ticker(ticker["ticker"])
        list_yahoo_data = yahoo_data.history(period="5d").values.tolist()
        try:
            ticker["open"] = str(round(list_yahoo_data[4][0], 2))
            ticker["yesterday_open"] = str(round(list_yahoo_data[3][0], 2))
            ticker["yesterday_high"] =str(round(list_yahoo_data[3][1], 2))
            ticker["yesterday_low"] = str(round(list_yahoo_data[3][2], 2))
            ticker["yesterday_close"] = str(round(list_yahoo_data[3][3], 2))
        except:
            print('excepted 2', ticker)
            try:
                twelve_data = td.time_series(
                    symbol=ticker["ticker"],
                    interval="1day",
                    outputsize=2,
                    timezone="America/New_York",
                )
                list_twelve_data = twelve_data.as_pandas().values.tolist()
                ticker["open"] = str(round(list_twelve_data[0][0], 2))
                ticker["yesterday_open"] = str(round(list_twelve_data[1][0], 2))
                ticker["yesterday_high"] = str(round(list_twelve_data[1][1], 2))
                ticker["yesterday_low"] = str(round(list_twelve_data[1][2], 2))
                ticker["yesterday_close"] = str(round(list_twelve_data[1][3], 2))
                time.sleep(8)
            except:
                ticker["thirty_min_change_percentage"] = 0.00
                ticker["one_hour_change_percentage"] = 0.00
                print('excepted 3', ticker)
                time.sleep(8)
        try:          
            data = td.time_series(
                symbol=ticker["ticker"],
                interval="1min",
                outputsize=60,
                timezone="America/New_York",
            )
            list_data = data.as_pandas().values.tolist()
            for interval in list_data[0:29]:
                volume_last_thirty += interval[4]
            for interval in list_data[30:59]:
                volume_previous_thirty += interval[4]
            ticker["change_since_open"] = str(round(list_data[0][3] - float(ticker["open"]), 2))
            ticker["change_since_open_percentage"] = str(round((list_data[0][3] - float(ticker["open"])) / float(ticker["open"]) * 100 , 2))
            ticker["volume"] = str(int(volume_last_thirty))
            ticker["volume_change"] = str(int(volume_last_thirty - volume_previous_thirty))
            ticker["volume_change_percentage"] = str(round(((volume_last_thirty - volume_previous_thirty) / volume_previous_thirty) * 100, 2))
            ticker["thirty_min_change"] = str(round(list_data[0][3] - list_data[29][0], 2))
            ticker["one_hour_change"] = str(round(list_data[0][3] - list_data[59][0], 2))
            ticker["thirty_min_change_percentage"] = str(round(((list_data[0][3] - list_data[29][0]) / list_data[29][0]) * 100, 2))
            ticker["one_hour_change_percentage"] = str(round(((list_data[0][3] - list_data[59][0]) / list_data[59][0]) * 100, 2))
            ticker["price"] = str(round(list_data[0][3], 2))
        except: 
            ticker["thirty_min_change_percentage"] = 0.00
            ticker["one_hour_change_percentage"] = 0.00
            pass
        time.sleep(8)

    message = '*Top Twenty Tickers By Increase In Mentions Since Yesterday:* \n'
    message_two = ''
    message_three = ''
    website = "\n <https://wsb-data.vercel.app/|Investigate these tickers further> \n"
    alert = False
    alert_message = '@channel *High acceleration detected* \n'
    for ticker in top_by_increase[0:6]:
        small_increase_high_mentions = float(ticker["difference_percentage"]) > 100 and int(ticker["mentions"]) > 100
        big_increase_moderate_mentions = float(ticker["difference_percentage"]) > 500 and int(ticker["mentions"]) > 50
        huge_increase_low_mentions = float(ticker["difference_percentage"]) > 2000
        price_increase = float(ticker["thirty_min_change_percentage"]) > 5 or float(ticker["one_hour_change_percentage"]) > 10

        link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
        if (small_increase_high_mentions or big_increase_moderate_mentions or huge_increase_low_mentions) and price_increase:
            alert = True
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
                price = "Currently $" + ticker["price"] + "\n"
                opened = "Opened at $" + ticker["open"] + "\n"
                open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
                price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
                price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
                volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
                volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
                yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
                alert_message += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
            except:
                alert_message += "Error fetching data for high acceleration ticker: " + link
        else:
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
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
                message += "Error fetching data for " + link


    for ticker in top_by_increase[6:14]:
        small_increase_high_mentions = float(ticker["difference_percentage"]) > 100 and int(ticker["mentions"]) > 100
        big_increase_moderate_mentions = float(ticker["difference_percentage"]) > 500 and int(ticker["mentions"]) > 50
        huge_increase_low_mentions = float(ticker["difference_percentage"]) > 2000
        price_increase = float(ticker["thirty_min_change_percentage"]) > 5 or float(ticker["one_hour_change_percentage"]) > 10

        link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
        if (small_increase_high_mentions or big_increase_moderate_mentions or huge_increase_low_mentions) and price_increase:
            alert = True
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
                price = "Currently $" + ticker["price"] + "\n"
                opened = "Opened at $" + ticker["open"] + "\n"
                open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
                price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
                price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
                volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
                volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
                yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
                alert_message += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
            except:
                alert_message += "Error fetching data for high acceleration ticker: " + link
        else:
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
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
                message_two += "Error fetching data for " + link

    for ticker in top_by_increase[14:]:
        small_increase_high_mentions = float(ticker["difference_percentage"]) > 100 and int(ticker["mentions"]) > 100
        big_increase_moderate_mentions = float(ticker["difference_percentage"]) > 500 and int(ticker["mentions"]) > 50
        huge_increase_low_mentions = float(ticker["difference_percentage"]) > 2000
        price_increase = float(ticker["thirty_min_change_percentage"]) > 5 or float(ticker["one_hour_change_percentage"]) > 10

        link = '<https://finance.yahoo.com/quote/' + ticker["ticker"] + '|*' + ticker["ticker"] + ":*> \n"
        if (small_increase_high_mentions or big_increase_moderate_mentions or huge_increase_low_mentions) and price_increase:
            alert = True
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
                price = "Currently $" + ticker["price"] + "\n"
                opened = "Opened at $" + ticker["open"] + "\n"
                open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
                price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
                price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
                volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
                volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
                yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
                alert_message += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
            except:
                alert_message += "Error fetching data for high acceleration ticker: " + link
        else:
            try:
                mentions = "```On pace for " + str(int(ticker["difference"])) + " (" + str(ticker["difference_percentage"]) + "%) more mentions than yesterday. \n"
                price = "Currently $" + ticker["price"] + "\n"
                opened = "Opened at $" + ticker["open"] + "\n"
                open_change = "Change of $" + ticker["change_since_open"] + " (" + ticker["change_since_open_percentage"] + "%) since open. \n"
                price_thirty = "Change of $" + ticker["thirty_min_change"] + " (" + ticker["thirty_min_change_percentage"] + "%) over thirty minutes. \n"
                price_sixty = "Change of $" + ticker["one_hour_change"] + " (" + ticker["one_hour_change_percentage"] + "%) over one hour. \n"   
                volume = ticker["volume"] + " volume traded in the past thirty minutes. \n"
                volume_change =  "Change of " + ticker["volume_change"] + " (" + ticker["volume_change_percentage"] + "%) from the previous thirty minutes. \n" 
                yesterday = "Yesterday: Open: $" + ticker["yesterday_open"] + ". High: $" + ticker["yesterday_high"] + ". Low: $" + ticker["yesterday_low"] + ". Close: $" + ticker["yesterday_close"] + ". ``` \n"
                message_three += link + mentions + price + opened + open_change + price_thirty + price_sixty + volume + volume_change + yesterday
            except:
                message_three += "Error fetching data for " + link

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
    time.sleep(1)
    slack_data = {'text': message_three + website}
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
    )
    if alert:
        time.sleep(1)
        slack_data = {'text': alert_message, "link_names": True}
        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
        )

a = arrow.now('US/Central')
hours = int(a.format('HH'))
day = int(a.format('d'))
while(hours < 15 and day < 6):
    a = arrow.now('US/Central')
    hours = int(a.format('HH'))
    start = round(time.time(), 0)
    run(hours)
    duration = round(time.time(), 0) - start
    start = round(time.time(), 0)
    print('sleeping')
    time.sleep(1785 - duration)

print('quitting')
quit()