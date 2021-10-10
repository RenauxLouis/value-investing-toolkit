import requests
from bs4 import BeautifulSoup
import requests_html
import lxml.html as lh
import pandas as pd
import re
from datetime import datetime
from datetime import timedelta
import time
import sys

import unidecode  # used to convert accented words
config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "stockdb",
}

ticker_list = ["AAPL", "MSFT", "GOOGL"]
### Extract from Yahoo Link ###
for ticker in ticker_list:
    print(ticker)
    url = 'https://in.finance.yahoo.com/quote/' + ticker[0]
    session = requests_html.HTMLSession()
    r = session.get(url)
    content = BeautifulSoup(r.content, 'lxml')
    print(str(content).split('data-reactid="34"')[4].split('</span>')[0])
    sys.exit()
    try:
        price = str(content).split(
            'data-reactid="34"')[4].split('</span>')[0].replace('>', '')
    except IndexError as e:
        price = 0.00
    price = price or "0"
    try:
        price = float(price.replace(',', ''))
    except ValueError as e:
        price = 0.00

    print(price)

print('completed...')
