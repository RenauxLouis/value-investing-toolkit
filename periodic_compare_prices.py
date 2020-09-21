from datetime import date, timedelta
import smtplib
import ssl
import pandas as pd
import yfinance as yf
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from constants import CSV_FPATH


def create_secure_connection_and_send_mail(ticker, most_recent_price,
                                           strike_price, sender_email,
                                           sender_password):
    print("In create secure connection")
    port = 465
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, sender_password)
        send_mail(ticker, most_recent_price, strike_price,
                  server, sender_email)


def send_mail(ticker, most_recent_price, strike_price, server, sender_email):
    print("In send mail")
    receiver_email = "renauxlouis@gmail.com"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"STRIKE PRICE {ticker.upper()}"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = """\
    Hi,
    How are you?
    Real Python has many great tutorials:
    www.realpython.com"""
    html = f"""\
    <html>
    <body>
        <p> Today's closing price on '{ticker}' was ${int(round(most_recent_price, 0))} which is below the strike price you set at ${strike_price}
        </p>
    </body>
    </html>
    """
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    message.attach(part1)
    message.attach(part2)

    server.sendmail(sender_email, receiver_email, message.as_string())


def remove_tickers_db(tickers_to_remove, df):
    len_df_before_removal = len(df)
    df = df.loc[~df["ticker"].isin(tickers_to_remove)]
    assert len(df) + len(tickers_to_remove) == len_df_before_removal

    df.to_csv(CSV_FPATH, index=False)


def compare_current_to_strike_prices(sender_email, sender_password):
    df = pd.read_csv(CSV_FPATH)
    tickers_to_remove = []
    for ticker, strike_price in zip(df["ticker"], df["strike_price"]):
        date_today = date.today()
        ticker_price_df = yf.download(
            ticker, date_today - timedelta(days=7), date_today)
        most_recent_price = ticker_price_df.iloc[-1]["Close"]
        print(ticker, strike_price, most_recent_price)
        if most_recent_price < strike_price:
            create_secure_connection_and_send_mail(
                ticker, most_recent_price, strike_price,
                sender_email, sender_password)
            tickers_to_remove.append(ticker)

    remove_tickers_db(tickers_to_remove, df)
