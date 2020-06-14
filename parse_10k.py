import argparse
from datetime import datetime
import os
import sys
import re
import pandas as pd
import requests
import pprint

from utils import create_document_list, get_cik, save_in_directory
pp = pprint.PrettyPrinter(indent=4)


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
        all_new_columns = []
        _10k_fpaths = [os.path.join(ticker, fname)
                       for fname in os.listdir(ticker)]
        for _10k_fpath in _10k_fpaths:
            print(_10k_fpath)
            year = _10k_fpath.split(".")[0][-4:]

            df_10k_per_sheet = pd.read_excel(_10k_fpath, sheet_name=None)
            df_10k_per_sheet = clean_df(df_10k_per_sheet, year)

            # TODO
            # Find if that split makes sense for other tickers
            # May need to pull first column instead of sheet title

            # TODO
            # Find regex per word

            # TODO
            # Make sure the currency is correct

            # TODO
            # Fix hack weightedaverage
            data_per_sheet = {
                "balance sheet": ["total assets", "total liabilities",
                                  "cash and cash equivalents",
                                  "property equipment", "equity",
                                  "goodwill", "intangible assets", "debt"],
                "statements of operations": ["operating income",
                                             "weightedaverage diluted",
                                             "net income"],
                "statements of cash flows": ["cash operating activities"]
            }
            all_dict_data = {}
            for sheet, data_list in data_per_sheet.items():

                dict_data = parse_data_from_sheet(
                    df_10k_per_sheet, sheet, data_list, year)
                new_columns = dict_data.keys()
                all_dict_data.update(dict_data)
                all_new_columns.extend(new_columns)
            pp.pprint(all_dict_data)
            # pp.pprint(all_new_columns)

            sys.exit()
            all_new_columns.append(new_columns)
            dict_data_year[year] = dict_data

        df_output = pd.DataFrame(data=dict_data_year, columns=new_columns).T
        df_output.to_csv(os.path.join(ticker, "selected_data.csv"))


def clean_df(df_per_sheet, year):

    # Put years in columns if in first row
    r = re.compile(".*" + year)
    for sheet, df in df_per_sheet.items():
        title = df.columns[0]
        year_column_list = list(
            filter(r.match, df.columns))
        if not year_column_list:
            df.iloc[0] = df.iloc[0].fillna("")
            first_row = [str(value) for value in df.iloc[0].values[1:]]
            year_first_row = list(
                filter(r.match, first_row))
            if year_first_row:
                new_columns = [title] + list(first_row)
                columns_renaming = dict(zip(df.columns, new_columns))
                cleaned_df = df.rename(columns=columns_renaming)
                df_per_sheet[sheet] = cleaned_df

    # Put 'title' as sheet name and lower
    df_per_sheet_title = {}
    for sheet, df in df_per_sheet.items():
        title = df.columns[0].lower()
        df_per_sheet_title[title] = df

    return df_per_sheet_title


def parse_data_from_sheet(df_10k_per_sheet, sheet, data_list, year):

    dict_data = {}
    r = re.compile(".*" + sheet)
    keys_couples = [(key, key.lower()) for key in df_10k_per_sheet.keys()]
    match_back = dict(zip([keys_couple[
        1] for keys_couple in keys_couples], [keys_couple[
            0] for keys_couple in keys_couples]))
    output = list(filter(r.match, [keys_couple[
        1] for keys_couple in keys_couples]))
    if output:
        # TODO: Find a correct way to select the sheet
        sheet_df = df_10k_per_sheet[match_back[output[0]]]
        print(sheet_df)

        r = re.compile(".*" + year)
        year_column_list = list(
            filter(r.match, sheet_df.columns))
        assert len(year_column_list) == 1
        year_column = year_column_list[0]

        # Get multiplier
        # TODO
        # Check other CSVs to make sure that works

        # TODO
        # What happens if both million and thousands in title
        title = sheet_df.columns[0]
        if "million" in title.lower():
            multiplier = 1000000
        elif "thousands" in title.lower():
            multiplier = 1000

        sheet_df[title] = sheet_df[title].str.lower()

        for data_point in data_list:
            list_r = data_point.split(" ")
            sheet_df = sheet_df.dropna(subset=[title])
            sheet_df["mask_col"] = sheet_df[title].apply(
                lambda match: regex_per_word(list_r, match.split(" ")))
            # sheet_df[title].str.contains(
            #    data_point, regex=True).fillna(False)
            selected_df = sheet_df[sheet_df["mask_col"]]
            print(selected_df)
            selected_df_year = selected_df[[title, year_column]]
            if len(selected_df_year) == 0:
                print(data_point, " not found")

            # TODO
            # Make sure the . with decimal values are parse correctly
            values = []
            for i, value in enumerate(selected_df_year[title].values):
                if "000" in value or "thousand" in value:
                    values.append(selected_df_year[year_column].values[i]*1000)
                else:
                    values.append(
                        selected_df_year[year_column].values[i]*multiplier)

            dict_data.update(
                zip(selected_df_year[title].values, values))
    else:
        print("Sheet {} not found".format(sheet))

    return dict_data


def regex_per_word(list_r, match):
    print(list_r, match)
    match = ["".join(filter(str.isalnum, item)) for item in match]
    print(len(set(list_r).intersection(set(match))), len(set(list_r)))
    if len(set(list_r).intersection(set(match))) == len(set(list_r)):
        return True
    return False


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
