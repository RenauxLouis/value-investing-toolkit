import argparse
import functools
import os
import pprint
import re
import sys
from datetime import datetime
from shutil import rmtree
import numpy as np
import pandas as pd
import requests

from utils import create_document_list, get_cik, save_in_directory

pp = pprint.PrettyPrinter(indent=4)


def download_10k(ciks, priorto, years):

    filing_type = "10-K"
    count = 5

    for ticker, cik in ciks.items():
        if os.path.exists(ticker):
            rmtree(ticker)
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
        # columns = ["total assets", "total liabilities",
        #            "cash and cash equivalents", "interest rate",
        #            "operating income", "lease expense current year",
        #            "depreciation", "cash from operating activities",
        #            "purchases of property and equipement",
        #            "interest and other financial charges",
        #            "annual effective tax rate", "total equity",
        #            "non-controlling interest", "goodwill",
        #            "other intangible assets", "net earning / share diluted",
        #            "weighted average shares diluted", "shares outstanding",
        #            "highest director compensation",
        #            "highest board member compensation", "net income",
        #            "shares of insiders", "dividend/share", "preferred stock",
        #            "debt"]
        dict_data_year = {}
        all_new_columns = []
        all_lease_df = {}
        all_current_liabilities_df = {}
        _10k_fpaths = [os.path.join(ticker, fname)
                       for fname in os.listdir(ticker)]
        for _10k_fpath in _10k_fpaths:
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

            # TODO
            # Are positive/negative values ok
            data_per_sheet = {
                "balance sheet": ["total assets", "total liabilities",
                                  "cash and cash equivalents",
                                  "property equipment", "equity",
                                  "goodwill", "intangible assets", "debt"],
                "statements of operations": ["operating income",
                                             "weightedaverage diluted",
                                             "net income", "interest expense",
                                             "income per diluted share"],
                "statements of cash flows": ["cash operating activities"]
            }
            all_df_data = []
            for sheet, data_list in data_per_sheet.items():

                df_data = parse_data_from_sheet(
                    df_10k_per_sheet, sheet, data_list, year)
                new_columns = df_data.index.values
                all_df_data.append(df_data)
                all_new_columns.extend(new_columns)
            pp.pprint(all_df_data)
            all_df_data_concat = pd.concat(all_df_data)
            lease_df = get_lease_df(df_10k_per_sheet, year)
            all_lease_df[year] = lease_df
            current_liabilities_df = get_current_liabilities_df(
                df_10k_per_sheet, year)
            all_current_liabilities_df[year] = current_liabilities_df

            dict_data_year[year] = all_df_data_concat

        list_data_year = []
        for year in sorted(dict_data_year.keys(), reverse=True):
            print(year)
            list_data_year.append(dict_data_year[year])

        df_output = pd.concat(list_data_year, axis=1, join="outer")
        df_output.columns = list(years)[::-1]

        # columns_renaming = dict(zip(df_output.columns, years))
        # df_output = df_output.rename(columns=columns_renaming)
        df_output.to_csv(os.path.join(ticker, "selected_data.csv"))
        with pd.ExcelWriter(os.path.join(
                ticker, "all_current_liabilities_df.xlsx")) as writer:
            for year, df in all_current_liabilities_df.items():
                df.to_excel(writer, sheet_name=year)
        with pd.ExcelWriter(os.path.join(ticker,
                                         "all_lease_df.xlsx")) as writer:
            for year, df in all_lease_df.items():
                if df is not None:
                    df.to_excel(writer, sheet_name=year)


def get_lease_df(df_10k_per_sheet, year):

    list_r = ["operating", "lease"]
    keys_array = np.array(list(df_10k_per_sheet.keys()))
    keys = [key.split(" ") for key in df_10k_per_sheet.keys()]
    match_keys = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_key = keys_array[match_keys]
    if selected_key:
        return df_10k_per_sheet[selected_key[0]]
    else:
        return None
    """
    all_selected = []
    for sheet, df in df_10k_per_sheet.items():
        r = re.compile(".*" + year)
        year_col_list = list(filter(r.match, df.columns))
        if len(year_col_list) > 0:
            year_col = year_col_list[0]

            first_col = df.columns[0]
            df[first_col] = df[first_col].str.lower()
            list_r = ["operating", "leases"]

            df = df.dropna(subset=[first_col])
            df["mask_col"] = df[first_col].apply(
                lambda match: regex_per_word(match.split(" "), list_r))
            selected_df = df[df["mask_col"]]
            selected_df_year = selected_df[[first_col, year_col]]
            if len(selected_df_year):
                print(selected_df_year)
                all_selected.append((selected_df_year[first_col].values,
                                     selected_df_year[year_col].values))
    sys.exit(all_selected)
    """


def get_current_liabilities_df(df_10k_per_sheet, year):
    sheet_df = find_sheet(df_10k_per_sheet, "balance sheet")
    sheet_df, first_col, year_col, multiplier = clean_col_and_multiplier(
        sheet_df, year)
    list_r = ["current", "liabilities"]
    sheet_df = sheet_df.dropna(subset=[first_col])
    sheet_df["mask_col"] = sheet_df[first_col].apply(
        lambda match: regex_per_word(match.split(" "), list_r))
    first_current_liabilities_row = sheet_df[sheet_df["mask_col"]
                                             ][[first_col, year_col]].iloc[0]
    assert np.isnan(first_current_liabilities_row[year_col])
    first_i = first_current_liabilities_row.name

    list_r = ["total", "current", "liabilities"]
    sheet_df = sheet_df.dropna(subset=[first_col])
    sheet_df["mask_col"] = sheet_df[first_col].apply(
        lambda match: regex_per_word(match.split(" "), list_r))
    last_current_liabilities_row = sheet_df[sheet_df["mask_col"]
                                            ][[first_col, year_col]].iloc[0]
    last_i = last_current_liabilities_row.name
    """
    sum_current_liabilities = 0
    start = False
    for i, row in sheet_df.iterrows():
        if row[first_col] == first_current_liabilities_row[first_col]:
            start = True
            first_i = i
        if start:
            # TODO
            # ROUNDING ERROR
            if row[year_col] == sum_current_liabilities:
                last_i = i
                break
            sum_current_liabilities += row[year_col]*start
    """
    selected_sheet = sheet_df.iloc[first_i:last_i+1]
    return selected_sheet[[first_col, year_col]]


def clean_df(df_per_sheet, year):

    # Put years in columns if in first row
    r = re.compile(".*" + year)
    for sheet, df in df_per_sheet.items():
        title = df.columns[0]
        year_col_list = list(
            filter(r.match, df.columns))
        if not year_col_list:
            df.iloc[0] = df.iloc[0].fillna("")
            first_row = [str(value) for value in df.iloc[0].values[1:]]
            year_first_row = list(
                filter(r.match, first_row))
            if year_first_row:
                new_columns = [title] + list(first_row)
                columns_renaming = dict(zip(df.columns, new_columns))
                cleaned_df = df.rename(columns=columns_renaming)
                df_per_sheet[sheet] = cleaned_df

    # Put "title" as sheet name and lower
    df_per_sheet_title = {}
    for sheet, df in df_per_sheet.items():
        title = df.columns[0].lower()
        df_per_sheet_title[title] = df

    return df_per_sheet_title


def find_sheet(df_10k_per_sheet, sheet):

    r = re.compile(".*" + sheet)
    keys_couples = [(key, key.lower()) for key in df_10k_per_sheet.keys()]
    match_back = dict(zip([keys_couple[
        1] for keys_couple in keys_couples], [keys_couple[
            0] for keys_couple in keys_couples]))
    output = list(filter(r.match, [keys_couple[
        1] for keys_couple in keys_couples]))
    # TODO: Find a correct way to select the sheet
    sheet_df = df_10k_per_sheet[match_back[output[0]]]

    return sheet_df


def clean_col_and_multiplier(sheet_df, year):

    r = re.compile(".*" + year)
    year_col_list = list(
        filter(r.match, sheet_df.columns))
    assert len(year_col_list) == 1
    year_col = year_col_list[0]

    # Get multiplier
    # TODO
    # Check other CSVs to make sure that works

    # TODO
    # What happens if both million and thousands in title
    first_col = sheet_df.columns[0]
    if "million" in first_col.lower():
        multiplier = 1000000
    elif "thousands" in first_col.lower():
        multiplier = 1000

    sheet_df[first_col] = sheet_df[first_col].str.lower()
    return sheet_df, first_col, year_col, multiplier


def parse_data_from_sheet(df_10k_per_sheet, sheet, data_list, year):

    dict_data = {}
    sheet_df = find_sheet(df_10k_per_sheet, sheet)
    sheet_df, first_col, year_col, multiplier = clean_col_and_multiplier(
        sheet_df, year)

    for data_point in data_list:
        list_r = data_point.split(" ")
        sheet_df = sheet_df.dropna(subset=[first_col])
        sheet_df["mask_col"] = sheet_df[first_col].apply(
            lambda match: regex_per_word(match.split(" "), list_r))
        # sheet_df[first_col].str.contains(
        #    data_point, regex=True).fillna(False)
        selected_df = sheet_df[sheet_df["mask_col"]]
        selected_df_year = selected_df[[first_col, year_col]]
        if len(selected_df_year) == 0:
            print(data_point, " not found")

        # TODO
        # Make sure the . with decimal values are parse correctly
        values = []
        for i, value in enumerate(selected_df_year[first_col].values):
            if "000" in value or "thousand" in value:
                values.append(selected_df_year[year_col].values[i]*1000)
            elif "per" in value and "share" in value:
                values.append(selected_df_year[year_col].values[i])
            else:
                values.append(
                    selected_df_year[year_col].values[i]*multiplier)

        dict_data.update(
            zip(selected_df_year[first_col].values, values))

    df_data = pd.DataFrame.from_dict(dict_data, orient='index')

    return df_data


def regex_per_word(match, list_r):
    match = ["".join(filter(str.isalnum, item)) for item in match]
    list_r_no_s = []
    for item in list_r:
        if item:
            if item[-1] == "s":
                list_r_no_s.append(item[:-1])
            else:
                list_r_no_s.append(item)

    match_no_s = []
    for item in match:
        if item:
            if item[-1] == "s":
                match_no_s.append(item[:-1])
            else:
                match_no_s.append(item)

    if len(set(list_r_no_s).intersection(set(match_no_s))) == len(
            set(list_r_no_s)):
        return True
    return False


def main(tickers_csv_fpath):

    tickers_df = pd.read_csv(tickers_csv_fpath)
    tickers = tickers_df["ticker"]
    ciks = get_cik(tickers)

    priorto = datetime.today().strftime("%Y%m%d")
    last_year = int(priorto[: 4]) - 1
    years = range(last_year-4, last_year+1)

    download_10k(ciks, priorto, years)
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
