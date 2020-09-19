import argparse
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import date


def plot_price(ticker, ticker_price_df):
    pass


def main(csv_fpath, start_date, end_date):

    df = pd.read_csv(csv_fpath)
    tickers_to_follow = df["tickers"].values
    for ticker in tickers_to_follow:
        ticker_price_df = yf.download(ticker, start_date, end_date)
        print(ticker_price_df)
        plot_price(ticker, ticker_price_df)


def parse_args():
    parser = argparse.ArgumentParser()
    date_today = date.today()
    date_1_year_ago = date_today - datetime.timedelta(days=365)
    parser.add_argument("--csv_fpath", default="price_follow.csv", type=str)
    parser.add_argument("--start_date", default=date_1_year_ago, type=str)
    parser.add_argument("--end_date", default=date_today, type=str)
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()
    main(csv_fpath=args.csv_fpath,
         start_date=args.start_date,
         end_date=args.end_date)
