import argparse
from datetime import datetime
import os
import sys
import re
import pandas as pd
import requests

from utils import create_document_list, get_cik, save_in_directory


def download_10k(ciks, priorto, years):

    filing_type = "10-K"
    count = 5

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


def select_data(tickers, years):

    for ticker in tickers:
        columns = ["total assets", "total liabilities",
                   "cash and cash equivalents", "interest rate",
                   "operating income", "lease expense current year",
                   "depreciation", "cash from operating activities",
                   "purchases of property and equipement",
                   "interest and other financial charges",
                   "annual effective tax rate", "total equity",
                   "non-controlling interest", "goodwill",
                   "other intangible assets", "net earning / share diluted",
                   "weighted average shares diluted", "shares outstanding",
                   "highest director compensation",
                   "highest board member compensation", "net income",
                   "shares of insiders", "dividend/share", "preferred stock",
                   "debt"]
        dict_data_year = []
        _10k_fpaths = [os.path.join(ticker, fname)
                       for fname in os.listdir(ticker)]
        for _10k_fpath in _10k_fpaths:
            print(_10k_fpath)
            year = _10k_fpath.split(".")[0][-4:]
            dict_data = {}

            df_10k = pd.read_excel(_10k_fpath, sheet_name=None)

            # Get Balance Sheet data
            # balance_sheet_infos = ["total assets", "total liabilities",
            #                        "cash and cash equivalents", "total equity",
            #                        "goodwill", "intangible assets", "debt"]
            balance_sheet_infos = ["total assets", "total liabilities",
                                   "cash and cash equivalents", "equity",
                                   "goodwill", "intangible assets", "debt"]

            r = re.compile(".*balance sheet")
            keys_couples = [(key, key.lower()) for key in df_10k.keys()]
            match_back = dict(zip([keys_couple[
                1] for keys_couple in keys_couples], [keys_couple[
                    0] for keys_couple in keys_couples]))
            output = list(filter(r.match, [keys_couple[
                1] for keys_couple in keys_couples]))
            if output:
                # TODO: Find a correct way to select the sheet
                balance_sheet_df = df_10k[match_back[output[0]]]
                print(balance_sheet_df)

                r = re.compile(".*" + year)
                year_column_list = list(
                    filter(r.match, balance_sheet_df.columns))
                assert len(year_column_list) == 1
                year_column = year_column_list[0]

                # Get multiplier
                # TODO: Check other CSVs to make sure that works
                title = balance_sheet_df.columns[0]
                if "million" in title.lower():
                    multiplier = 1000000
                elif "thousands" in title.lower():
                    multiplier = 1000

                balance_sheet_df[title] = balance_sheet_df[title].str.lower()

                for balance_sheet_info in balance_sheet_infos:
                    mask = balance_sheet_df[title].str.contains(
                        balance_sheet_info, regex=True)
                    selected_df = balance_sheet_df[mask]
                    selected_df_year = selected_df[[title, year_column]]
                    if len(selected_df_year) == 0:
                        print(balance_sheet_info, " not found")

                    # TODO
                    # Make sure the . with decimal values are parse correctly
                    values = [
                        value*multiplier for value in selected_df_year[
                            year_column].values]
                    dict_data.update(
                        zip(selected_df_year[title].values, values))

                print(dict_data)
                new_columns = dict_data.keys()
                print(new_columns)
                sys.exit()

            sys.exit()
            dict_data_year[year] = dict_data

        df_output = pd.DataFrame(data=dict_data_year, columns=new_columns).T
        df_output.to_csv(os.path.join(ticker, "selected_data.csv"))


def main(tickers_csv_fpath):

    tickers_df = pd.read_csv(tickers_csv_fpath)
    tickers = tickers_df["ticker"]
    ciks = get_cik(tickers)

    priorto = datetime.today().strftime("%Y%m%d")
    last_year = int(priorto[: 4]) - 1
    years = range(last_year-4, last_year+1)

    # download_10k(ciks, priorto, years)
    select_data(tickers, years)


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
