import argparse
from datetime import date, timedelta
from warnings import warn

import pandas as pd
import yfinance as yf


def print_prices(ticker, ticker_price_df):
    print("\n")
    print("#"*22, ticker, "#"*22)

    price = ticker_price_df.iloc[-1]["Close"]
    print("#"*20, round(price, 1), "$ ", "#"*20, "\n")


def add_tickers_to_db(tickers_to_add, df):

    if tickers_to_add:
        for ticker in tickers_to_add:
            if ticker in df["ticker"].values:
                msg = (f"Cannot add ticker '{ticker}' as it is already in the"
                       " database")
                warn(msg)
                continue

            strike_price = input("What is the strike price you want to set for"
                                 f" ticker: {ticker}?: ")
            df = df.append(
                {"ticker": ticker, "strike_price": strike_price},
                ignore_index=True)

    return df


def main(csv_fpath, start_date, end_date, tickers_to_add):

    df = pd.read_csv(csv_fpath)
    tickers_to_follow = df["ticker"].values
    for ticker in tickers_to_follow:
        ticker_price_df = yf.download(ticker, start_date, end_date)
        print_prices(ticker, ticker_price_df)

    df = add_tickers_to_db(tickers_to_add, df)

    df.to_csv(csv_fpath)


def parse_args():
    parser = argparse.ArgumentParser()
    date_today = date.today()
    date_1_year_ago = date_today - timedelta(days=365)
    parser.add_argument(
        "--csv_fpath", default="list_price_follow.csv", type=str)
    parser.add_argument("--start_date", default=date_1_year_ago, type=str)
    parser.add_argument("--end_date", default=date_today, type=str)
    parser.add_argument("--tickers_to_add", nargs="+")
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()
    main(csv_fpath=args.csv_fpath,
         start_date=args.start_date,
         end_date=args.end_date,
         tickers_to_add=args.tickers_to_add)
