
# coding: utf-8

# Integrated by Haitian, on 05-01-2017.
# The code is for creating the securities_master database for storing
# financial data. The database contains two tables, for tickers symbol 
# and daily price. The ticker symbols are scraped from Wikipedia, and 
# the daily price downloaded from Yahoo finance.

# Web
import bs4 as bs
import requests

# Database
import sqlite3

# Financial data downloading and processing
import pandas as pd
import pandas_datareader.data as web

# Database
import sqlite3

# Date
import datetime as dt
import time

#----------------------------------
# Part I: Creating tables in sqlite
#----------------------------------

conn = sqlite3.connect("securities_master.db")
c = conn.cursor()

# Creating the symbol table

c.execute('''CREATE TABLE IF NOT EXISTS symbol (
             ticker TEXT NOT NULL PRIMARY KEY, 
             name TEXT,
             sector TEXT,
             last_updated_date TEXT
             );'''
         )
conn.commit()

# Creating the daily price table

c.execute('''CREATE TABLE IF NOT EXISTS daily_price (
             ticker TEXT,
             price_date TEXT,
             open_price REAL,
             high_price REAL,
             low_price REAL,
             close_price REAL,
             volume BIGINT,
             adj_close_price REAL,
             last_updated_date TEXT,
             FOREIGN KEY (ticker) REFERENCES symbol (ticker)
             );'''
         )
conn.commit()


#------------------------------
# Part II: Filling symbol table
#------------------------------

# Webscraping from Wikipedia

resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'class': 'wikitable sortable'})

unix = int(time.time())
now = str(dt.datetime.fromtimestamp(unix).strftime('%Y-%m-%d'))

symbols_data = []
for row in table.findAll('tr')[1:]:
    ticker = row.findAll('td')[0].text
    name = row.findAll('td')[1].text
    sector = row.findAll('td')[3].text
    current_symbol = (ticker, name, sector, now)
    symbols_data.append(current_symbol)

c.executemany("INSERT INTO symbol VALUES (?, ?, ?, ?)", symbols_data)
conn.commit()

#------------------------------------
# Part III: Filling daily_price table
#------------------------------------

# Retrieve S&P500 symbols from the symbol table 

c.execute('SELECT ticker FROM symbol')
tickers = c.fetchall()
tickers = [ticker[0] for ticker in tickers] # Converting a list of tuples into a list of str

start = dt.datetime(2007, 1, 1)
end = dt.datetime(2016, 12, 31)

# Download daily price data from Yahoo finance

for ticker in tickers:
    price_df = web.DataReader(ticker.replace(".","-"), "yahoo", start, end)
    for index,row in price_df.iterrows():
        current_data = (ticker, str(row.name), row[0], row[1], row[2], row[3], row[4], row[5], now)
        c.execute("INSERT INTO daily_price VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", current_data)
        conn.commit()
    print("Data of %s successfully downloaded." % (ticker))

# Close the connection

c.close()
conn.close()

