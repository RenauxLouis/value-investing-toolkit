import multiprocessing as mp

import numpy as np
import pandas as pd
import yfinance as yf
from get_all_tickers import get_tickers as gt
from sklearn.linear_model import LinearRegression


def get_stock_data(ticket):
    print(ticket)
    ticket_info = yf.Ticker(ticket)
    hist = ticket_info.history(start="2019-01-01")
    hist["Date"] = hist.index
    hist["Date_int"] = range(len(hist))
    end_date = "2020-02-18"
    if "2020-04-29" not in hist.index:
        return []
    last_row = hist.loc["2020-04-29"]
    X_test = np.array(last_row["Date_int"]).reshape(1, -1)
    y_true = last_row["Close"]
    mask = hist["Date"] < end_date
    hist = hist.loc[mask]
    if hist.empty:
        return []
    X, y = hist["Date_int"].values.reshape(-1, 1), hist["Close"].values
    regressor = LinearRegression()
    try:
        regressor.fit(X, y)
    except:
        return []

    y_pred = regressor.predict(X_test)
    diff = y_pred - y_true
    row = [ticket, y_pred[0], y_true, diff[0], last_row["Volume"],
           last_row["Dividends"], last_row["Stock Splits"]]
    return row


if __name__ == "__main__":

    list_of_tickers = sorted(gt.get_tickers())

    pool = mp.Pool(mp.cpu_count())
    data = pool.map(get_stock_data, [stock for stock in list_of_tickers])
    pool.close()

    columns = ["Company", "y_pred", "y_true", "Diff", "current_volume",
               "current_dividends", "current_stock_splits"]
    pd_csv = pd.DataFrame(data, columns=columns)
    pd_csv["Diff_perc"] = pd_csv["Diff"] / pd_csv["y_true"]
    pd_csv.to_csv("diff.csv")
