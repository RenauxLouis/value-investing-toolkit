from datetime import date, timedelta
from warnings import warn
from flask import Flask, Response, request
from waitress import serve
import pandas as pd
import yfinance as yf
import smtplib
import ssl


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
        msg = (f"Cannot add ticker '{ticker}' as it is already in the"
               " database")
        warn(msg)

    df = df.append(
        {"ticker": ticker, "strike_price": strike_price},
        ignore_index=True)

    return df


app = Flask(__name__)


@app.route('/isalive', methods=["GET"])
def is_alive():
    return "OK"


@app.route('/add_ticker', methods=["POST"])
def infer():
    try:

        df = pd.read_csv(CSV_FPATH)
        ticker_to_add = request.json["ticker_to_add"]
        strike_price = request.json["strike_price"]
        df = add_tickers_to_db(ticker_to_add, strike_price, df)
        if "fname" not in request.json:
            return Response("{'Error':'S3 File path missing. Please provide as"
                            " the fname field'}", status=401,
                            mimetype="application/json")

        return Response(f"'Error':'Ticker {ticker_to_add} correctly added to"
                        " the database'", status=200,
                        mimetype="application/json")

    except Exception as e:
        error_to_send = "Error: " + str(e)
        print(error_to_send)
        return Response(error_to_send, status=401, mimetype="application/json")


if __name__ == "__main__":
    serve(app, port=8080)
