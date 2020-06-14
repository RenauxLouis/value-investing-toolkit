from datetime import datetime
import argparse

import pandas as pd
import requests

from utils import create_document_list, get_cik, save_in_directory


def download_10(ciks):

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


def main(tickers_csv_fpath):

    tickers_df = pd.read_csv(tickers_csv_fpath)
    tickers = tickers_df["ticker"]
    ciks = get_cik(tickers)

    download_10(ciks)


def parse_args():
    # Parse command line
    parser = argparse.ArgumentParser(
        description="Preprocessing Pipeline")
    parser.add_argument(
        "--tickers_csv_fpath",
        type=str,
        default="list_tickers.csv")
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()
    main(args.tickers_csv_fpath)
