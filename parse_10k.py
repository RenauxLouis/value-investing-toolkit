from datetime import datetime

import pandas as pd
import requests

from utils import create_document_list, get_cik, save_in_directory

tickers_df = pd.read_csv("list_tickers.csv")
tickers = tickers_df["ticker"]
ciks = get_cik(tickers)

url = r"https://www.sec.gov/Archives/edgar/data"
filing_type = "10-K"
count = 5
priorto = datetime.today().strftime("%Y%m%d")
last_year = int(priorto[:4]) - 1
years = range(last_year-4, last_year+1)

for ticker, cik in ciks.items():
    # Get the 10k

    base_url = "http://www.sec.gov/cgi-bin/browse-edgar"
    params = {"action": "getcompany", "owner": "exclude", "output": "xml",
              "CIK": cik, "type": filing_type, "dateb": priorto,
              "count": count}
    r = requests.get(base_url, params=params)
    if r.status_code == 200:
        data = r.text
        urls = create_document_list(data)

        url_fr_per_year = {}
        for year in years:
            url_fr_per_year[year] = urls[year]

        try:
            save_in_directory(ticker, cik, priorto, url_fr_per_year)
        except Exception as e:
            print(str(e))  # Need to use str for Python 2.5
