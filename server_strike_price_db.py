import json
import os
import pandas as pd
from flask import Flask, Response, request
from waitress import serve

from utils import get_current_date_time_as_prefix

from constants import CSV_FPATH

app = Flask(__name__)


def add_tickers_to_db(tickers_to_add, df):

    if tickers_to_add:
        for ticker in tickers_to_add:
            add_one_ticker_to_db(ticker, df)

    return df


def add_one_ticker_to_db(ticker, strike_price, df):

    if ticker in df["ticker"].values:
        # Add a way to cleanly edit the strike price
        return Response(f"Ticker {ticker} already in the database",
                        status=401, mimetype="application/json")

    df = df.append(
        {"ticker": ticker, "strike_price": strike_price},
        ignore_index=True)

    df.to_csv(CSV_FPATH, index=False)

    return Response(json.dumps({"Success": f"Ticker {ticker} added to the db"}),
                    status=200, mimetype="application/json")


@app.route("/is_alive", methods=["GET"])
def is_alive():
    return "OK"


@app.route("/read_db", methods=["GET"])
def read_db():
    df = pd.read_csv(CSV_FPATH, index_col=False)
    df_as_dict = dict(zip(df["ticker"], df["strike_price"]))
    return Response(json.dumps(df_as_dict), status=200,
                    mimetype="application/json")


@app.route("/reset_db", methods=["GET"])
def reset_db():
    time_now = get_current_date_time_as_prefix()
    saved_db_folder = "saved_db"
    os.makedirs(saved_db_folder, exist_ok=True)
    os.rename(CSV_FPATH, os.path.join(saved_db_folder, time_now + CSV_FPATH))
    df_empty = pd.DataFrame({"ticker": [], "strike_price": []})
    df_empty.to_csv(CSV_FPATH, index=False)

    return Response(json.dumps({"Success": "Database reset"}), status=200,
                    mimetype="application/json")


@app.route("/add_ticker", methods=["POST"])
def infer():
    try:

        df = pd.read_csv(CSV_FPATH, index_col=False)
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
