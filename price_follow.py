import argparse
from datetime import date, timedelta
from warnings import warn

import pandas as pd
import yfinance as yf
import smtplib
import ssl


def create_secure_connection_and_send_mail(ticker, most_recent_price,
                                           strike_price):
    port = 465
    password = input("Type your password and press enter: ")
    sender_email = "strike.price.notification@gmail.com"
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, password)
        send_mail(ticker, most_recent_price,
                  strike_price, server, sender_email)


def send_mail(ticker, most_recent_price, strike_price, server, sender_email):
    receiver_email = "renauxlouis@gmail.com"
    message = f"""\
    Subject: STRIKE PRICE {ticker.upper()}

    Today's closing price on '{ticker}' was ${int(round(most_recent_price, 0))} which is below the strike price you set at ${strike_price}"""
    server.sendmail(sender_email, receiver_email, message)


def compare_current_to_strike_prices(csv_fpath):
    df = pd.read_csv(csv_fpath)
    for ticker, strike_price in zip(df["ticker"], df["strike_price"]):
        date_today = date.today()
        ticker_price_df = yf.download(
            ticker, date_today - timedelta(days=7), date_today)
        most_recent_price = ticker_price_df.iloc[-1]["Close"]
        print(most_recent_price)
        if most_recent_price < strike_price:
            create_secure_connection_and_send_mail(
                ticker, most_recent_price, strike_price)


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

    compare_current_to_strike_prices(csv_fpath)


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
