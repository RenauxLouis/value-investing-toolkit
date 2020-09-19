import argparse
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import date, timedelta
from mplfinance import plot
from matplotlib import patches


def print_prices(ticker, ticker_price_df):
    print("\n")
    print("#"*22, ticker, "#"*22)

    price = ticker_price_df.iloc[-1]["Close"]
    print("#"*20, round(price, 1), "$ ", "#"*20, "\n")


def main(csv_fpath, start_date, end_date):

    df = pd.read_csv(csv_fpath)
    tickers_to_follow = df["tickers"].values
    for ticker in tickers_to_follow:
        ticker_price_df = yf.download(ticker, start_date, end_date)
        print_prices(ticker, ticker_price_df)


def parse_args():
    parser = argparse.ArgumentParser()
    date_today = date.today()
    date_1_year_ago = date_today - timedelta(days=365)
    parser.add_argument(
        "--csv_fpath", default="list_price_follow.csv", type=str)
    parser.add_argument("--start_date", default=date_1_year_ago, type=str)
    parser.add_argument("--end_date", default=date_today, type=str)
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()
    main(csv_fpath=args.csv_fpath,
         start_date=args.start_date,
         end_date=args.end_date)
