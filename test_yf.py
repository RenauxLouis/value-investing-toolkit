# from rtstock.stock import Stock
# import yfinance as yf
# from datetime import date, timedelta
from yahoo_fin import stock_info
print(stock_info.get_live_price('msft'))


# date_today = date.today()
# ticker_price_df = yf.download(
#     "AAPL", date_today - timedelta(days=7), date_today)
# print(ticker_price_df)

# stock = Stock("AAPL")
# print(stock.get_latest_price())
