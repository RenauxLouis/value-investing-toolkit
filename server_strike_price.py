import smtplib
import ssl
from datetime import date, timedelta
from warnings import warn
import json

import pandas as pd
import yfinance as yf
from flask import Flask, Response, request
from waitress import serve

CSV_FPATH = "list_price_follow.csv"


def create_secure_connection_and_send_mail(ticker, most_recent_price,
                                           strike_price, sender_email,
                                           receiver_email):
    port = 465
    password = input("Type your password and press enter: ")
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, password)
        send_mail(ticker, most_recent_price,
                  strike_price, server, sender_email, receiver_email)


def send_mail(ticker, most_recent_price, strike_price, server,
              sender_email, receiver_email):
    message = f"""\
    Subject: STRIKE PRICE {ticker.upper()}

    Today's closing price on '{ticker}' was ${int(round(most_recent_price, 0))} which is below the strike price you set at ${strike_price}"""
    server.sendmail(sender_email, receiver_email, message)


def compare_current_to_strike_prices(csv_fpath, sender_email, receiver_email):
    df = pd.read_csv(csv_fpath)
    for ticker, strike_price in zip(df["ticker"], df["strike_price"]):
        date_today = date.today()
        ticker_price_df = yf.download(
            ticker, date_today - timedelta(days=7), date_today)
        most_recent_price = ticker_price_df.iloc[-1]["Close"]
        print(most_recent_price)
        if most_recent_price < strike_price:
            create_secure_connection_and_send_mail(
                ticker, most_recent_price, strike_price, sender_email,
                receiver_email)


def add_tickers_to_db(tickers_to_add, df):

    if tickers_to_add:
        for ticker in tickers_to_add:
            add_one_ticker_to_db(ticker, df)

    return df


def add_one_ticker_to_db(ticker, strike_price, df):

    if ticker in df["ticker"].values:
        return Response(f"Ticker {ticker} already in the database",
                        status=401, mimetype="application/json")

    df = df.append(
        {"ticker": ticker, "strike_price": strike_price},
        ignore_index=True)

    df.to_csv(CSV_FPATH)

    return Response(json.dumps({'success': True}), status=200,
                    mimetype="application/json")


app = Flask(__name__)


@app.route("/is_alive", methods=["GET"])
def is_alive():
    return "OK"


@app.route("/add_ticker", methods=["POST"])
def infer():
    try:

        df = pd.read_csv(CSV_FPATH)
        ticker_to_add = request.json["ticker_to_add"]
        strike_price = request.json["strike_price"]

        if "ticker_to_add" not in request.json:
            return Response("{'Error':'Ticker missing. Please provide as"
                            " the ticker_to_add field'}", status=401,
                            mimetype="application/json")
        if "strike_price" not in request.json:
            return Response("{'Error':'Strike price missing. Please provide as"
                            " the strike_price field'}", status=401,
                            mimetype="application/json")

        return add_one_ticker_to_db(ticker_to_add, strike_price, df)

    except Exception as e:
        error_to_send = "Error: " + str(e)
        print(error_to_send)
        return Response(error_to_send, status=401, mimetype="application/json")


if __name__ == "__main__":
    serve(app, port=8080)
