# Make sure that you have all these libaries available to run the code successfully
from pandas_datareader import data
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt
import urllib.request, json
import os
import numpy as np
# import tensorflow as tf # This code has been tested with TensorFlow 1.6
from sklearn.preprocessing import MinMaxScaler

API_KEY = "I1QK2YJLYH295D4I"


def get_data_alpha_vantage():
    # ====================== Loading Data from Alpha Vantage ==================================

    # American Airlines stock market prices
    ticker = "AAL"

    # JSON file with all the stock market data for AAL from the last 20 years
    url_string = ("https://www.alphavantage.co/query?function="
                  "TIME_SERIES_DAILY&symbol=%s&outputsize=full&"
                  "apikey=%s"%(ticker,API_KEY))

    # Save data to this file
    file_to_save = "stock_market_data-%s.csv"%ticker

    # If you haven"t already saved data,
    # Go ahead and grab the data from the url
    # And store date, low, high, volume, close, open values to a Pandas DataFrame
    if not os.path.exists(file_to_save):
        with urllib.request.urlopen(url_string) as url:
            data = json.loads(url.read().decode())
            # extract stock market data
            data = data["Time Series (Daily)"]
            df = pd.DataFrame(columns=["Date","Low","High","Close","Open"])
            for k,v in data.items():
                date = dt.datetime.strptime(k, "%Y-%m-%d")
                data_row = [date.date(),float(v["3. low"]),float(v["2. high"]),
                            float(v["4. close"]),float(v["1. open"])]
                df.loc[-1,:] = data_row
                df.index = df.index + 1
        print("Data saved to : %s"%file_to_save)
        df.to_csv(file_to_save)

    # If the data is already there, just load it from the CSV
    else:
        print("File already exists. Loading data from CSV")
        df = pd.read_csv(file_to_save)
    return df


def get_data_kaggle():
    # ====================== Loading Data from Kaggle ==================================
    # You will be using HP"s data. Feel free to experiment with other data.
    # But while doing so, be careful to have a large enough dataset and also pay attention to the data normalization
    df = pd.read_csv(os.path.join("Stocks", "hpq.us.txt"), delimiter=",",
                     usecols=["Date", "Open", "High", "Low", "Close"])
    print("Loaded data from the Kaggle repository")
    return df


def get_data(data_source):
    if data_source == "alphavantage":
        df = get_data_alpha_vantage()
    else:
        df = get_data_kaggle()
    return df


def data_viz(df):
    plt.figure(figsize = (15,7))
    plt.plot(range(df.shape[0]),(df["Low"]+df["High"])/2.0)
    plt.xticks(range(0,df.shape[0],500),df["Date"].loc[::500],rotation=45)
    plt.xlabel("Date",fontsize=18)
    plt.ylabel("Mid Price",fontsize=18)
    plt.show()


def preprocessing(df):
    # First calculate the mid prices from the highest and lowest
    df["Mid"] = df.apply(lambda row: (row["High"] + row["Low"]) / 2, axis=1)
    train_data = df.loc[:11000, "Mid"].values
    test_data = df.loc[11000:, "Mid"].values

    scaler = MinMaxScaler()
    train_data = train_data.reshape(-1,1)
    test_data = test_data.reshape(-1,1)

    # Train the Scaler with training data and smooth data
    smoothing_window_size = 2500
    for di in range(0, 10000, smoothing_window_size):
        scaler.fit(train_data[di:di + smoothing_window_size, :])
        train_data[di:di + smoothing_window_size, :] = scaler.transform(
            train_data[di:di+smoothing_window_size,:])

    # You normalize the last bit of remaining data
    scaler.fit(train_data[di+smoothing_window_size:, :])
    train_data[di+smoothing_window_size:,:] = scaler.transform(
        train_data[di+smoothing_window_size:,:])


if __name__ == "__main__":
    data_source = "kaggle" # alphavantage or kaggle
    df = get_data(data_source)

    df = df.sort_values("Date")
    print(df.head())

    # data_viz(df)
    preprocessing(df)


