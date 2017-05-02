import sqlite3
import pandas as pd

def retrieve_ohlc_data_from_db(start, end, variable = "adj_close_price"):

	# Initializing the result dataframe
	df = pd.DataFrame()

	conn = sqlite3.connect("securities_master.db")
	c = conn.cursor()

	# Retrieve ticker symbols
	c.execute("SELECT ticker FROM symbol;")
	tickers = c.fetchall()
	tickers = [ticker[0] for ticker in tickers]

	# Retrieve the date of price
	c.execute("""SELECT price_date from daily_price 
		         WHERE price_date BETWEEN ? AND ?
		         AND ticker = ? ;""", (start, end, tickers[0])) 
	date_range = c.fetchall()
	date_range = [row[0] for row in date_range]
	df["Date"] = date_range

	# Retrieve price data for every symbol iteratively
	for ticker in tickers:
		c.execute("""SELECT {} FROM daily_price 
			         WHERE ticker = ? 
			         AND price_date BETWEEN ? AND ? ;""".format(variable), (ticker, start, end))
		current_data = c.fetchall()
		current_data = pd.Series([row[0] for row in current_data])
		df[ticker] = current_data

	return df