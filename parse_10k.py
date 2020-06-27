import argparse
import datetime
import functools
import os
import pprint
import re
import sys
from functools import reduce
from shutil import rmtree

import nltk.data
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from yahoo_fin import stock_info as si

from utils import get_cik, save_in_directory

pp = pprint.PrettyPrinter(indent=4)
nltk.download('punkt')
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
BASE_URL = "http://www.sec.gov/cgi-bin/browse-edgar"
BASE_EDGAR_URL = "https://www.sec.gov/Archives/edgar/data"


def download_10k(ciks_per_ticker, priorto, years, dl_folder):

    filing_types = ["10-K", "DEF 14A"]
    count = 5
    valid_years_per_ticker = {}
    for ticker, cik in ciks_per_ticker.items():
        print("Ticker: ", ticker)
        ticker_folder = os.path.join(dl_folder, ticker)
        if os.path.exists(ticker_folder):
            rmtree(ticker_folder)

        os.makedirs(ticker_folder)
        full_urls_per_type = {}
        for filing_type in filing_types:
            params = {"action": "getcompany", "owner": "exclude",
                      "output": "xml", "CIK": cik, "type": filing_type,
                      "dateb": priorto, "count": count}
            r = requests.get(BASE_URL, params=params)
            if r.status_code == 200:
                data = r.text

                soup = BeautifulSoup(data, features="lxml")
                urls = [link.string for link in soup.find_all(
                    "filinghref")]

                if filing_type == "10-K":
                    types = [link.string for link in soup.find_all(
                        "type")]

                    _10k = []
                    amended_10k = []
                    for i, type_file in enumerate(types):
                        if type_file == "10-K":
                            if len(_10k) == 5:
                                break
                            _10k.append(urls[i])
                        elif type_file == "10-K/A":
                            amended_10k.append(urls[i])
                        else:
                            sys.exit("Unknown file type: ", type_file)

                    accession_numbers_10k = [
                        link.split("/")[-2] for link in _10k]
                    accession_numbers_10k_amended = [
                        link.split("/")[-2] for link in amended_10k]
                    # List of url to the text documents

                    full_urls_per_type["10-K_htm"] = get_files_url(
                        cik, accession_numbers_10k, "htm", "10-k", "10k")
                    if accession_numbers_10k_amended:
                        input(
                            "THERE ARE AMENDED 10-K FILES, PLEASE CHECK THEM "
                            "(<enter> to continue) ")
                        full_urls_per_type["10-K_amended_htm"] = get_files_url(
                            cik, accession_numbers_10k_amended, "htm", "10-ka",
                            "10ka")
                    full_urls_per_type["10-K_xlsx"] = get_files_url(
                        cik, accession_numbers_10k, "xlsx", "Financial_Report",
                        "Financial_Report")
                elif filing_type == "DEF 14A":
                    accession_numbers = [
                        link.split("/")[-2] for link in urls[:5]]
                    full_urls_per_type["10-K_def_14a_htm"] = get_files_url(
                        cik, accession_numbers, "htm", "", "")
            else:
                sys.exit("Ticker data not found")

        for file_type, urls in full_urls_per_type.items():
            ext = file_type.split("_")[-1]
            url_fr_per_year = {}
            if file_type == "10-K_amended_htm":
                url_fr_per_year = {str(i): url for i, url in enumerate(urls)}
            else:
                for i, year in enumerate(years[:: -1]):
                    url_fr_per_year[year] = urls[i]
            try:
                valid_years = save_in_directory(ticker_folder, cik,
                                                priorto, ext, file_type,
                                                url_fr_per_year)
                valid_years_per_ticker[ticker] = valid_years
            except Exception as e:
                sys.exit(e)

    return valid_years_per_ticker


def get_files_url(cik, accession_numbers, ext, if_1, if_2):

    return_urls = []
    for accession_number in accession_numbers:
        accession_number_url = os.path.join(
            BASE_EDGAR_URL, cik, accession_number).replace("\\", "/")
        r = requests.get(accession_number_url)
        if r.status_code == 200:
            data = r.text
            soup = BeautifulSoup(data, features="lxml")
            links = [link.get("href") for link in soup.findAll("a")]
            urls = [link for link in links if (link.split(".")[-1] == ext and (
                if_1 in link or if_2 in link))
            ]
            urls = [url for url in urls if accession_number in url]
            # TODO
            # Find a better method to pick the correct file (can't get cik V)
            fname = os.path.basename(urls[0])
            url = os.path.join(accession_number_url, fname).replace("\\", "/")
            return_urls.append(url)
    return return_urls


def find_income_statement(df_10k_per_sheet):
    df_10k_keys = df_10k_per_sheet.keys()
    list_possibles = ["statement income", "statement earning",
                      "statement operation"]
    list_indexes = []
    sheet_per_index = {}
    for possible in list_possibles:
        selected_sheets = regex_per_word_wrapper(
            possible, df_10k_keys)
        if selected_sheets is not None:
            selected_sheet = selected_sheets[0]
            index = list(df_10k_keys).index(selected_sheet)
            list_indexes.append(index)
            sheet_per_index[index] = selected_sheet

    selected_index = min(list_indexes)
    return sheet_per_index[selected_index]


def get_lease_df(df_10k_per_sheet, year):
    list_r = ["operating", "lease"]
    keys_array = np.array(list(df_10k_per_sheet.keys()))
    keys = [key.split(" ") for key in df_10k_per_sheet.keys()]
    match_keys = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_key = keys_array[match_keys]
    if selected_key.size > 0:
        df = df_10k_per_sheet[selected_key[0]]
        df, first_col, year_col, multiplier = clean_col_and_multiplier(
            df, year)
        df[year_col] = df[year_col].apply(
            lambda x: x*multiplier if (isinstance(
                x, float) or isinstance(x, int)) else x)
        return df[[first_col, year_col]]
    else:
        return find_lease_commitments_and_contingencies(
            df_10k_per_sheet, year)


def find_lease_commitments_and_contingencies(df_10k_per_sheet, year):
    list_r = ["commitments", "contingencies"]
    keys_array = np.array(list(df_10k_per_sheet.keys()))
    keys = [key.split(" ") for key in df_10k_per_sheet.keys()]
    match_keys = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_key = keys_array[match_keys]
    if selected_key.size > 0:
        sheet_df = df_10k_per_sheet[selected_key[0]]
        selected_df, first_col, year_col, multiplier = (
            clean_col_and_multiplier(sheet_df, year))

        full_texts = selected_df[year_col].fillna("").values
        select_text = max(full_texts, key=len)
        sentences = tokenizer.tokenize(select_text)
        sentences_lease = [
            sentence for sentence in sentences if (
                "lease" in sentence or year in sentence)]

        data = [[" ".join(sentences_lease)]]
        output_df = pd.DataFrame(data, columns=[year])

        return output_df
    return None


def get_current_liabilities_df(df_10k_per_sheet, year):
    selected_key = regex_per_word_wrapper(
        "balance sheet", df_10k_per_sheet.keys())[0]
    sheet_df = df_10k_per_sheet[selected_key]
    sheet_df, first_col, year_col, multiplier = clean_col_and_multiplier(
        sheet_df, year)
    list_r = ["current", "liabilities"]
    sheet_df = sheet_df.dropna(subset=[first_col])
    sheet_df["mask_col"] = sheet_df[first_col].apply(
        lambda match: regex_per_word(match.split(" "), list_r))
    if not sheet_df["mask_col"].any():
        return None
    first_current_liabilities_row = sheet_df[sheet_df["mask_col"]
                                             ][[first_col, year_col]].iloc[0]
    assert np.isnan(first_current_liabilities_row[year_col])
    first_i = first_current_liabilities_row.name

    list_r = ["current", "liabilities"]
    sheet_df = sheet_df.dropna(subset=[first_col])
    sheet_df["mask_col"] = sheet_df[first_col].apply(
        lambda match: regex_per_word(match.split(" "), list_r))
    if not sheet_df["mask_col"].any():
        return None
    last_current_liabilities_row = sheet_df[
        sheet_df["mask_col"]][[first_col, year_col]].iloc[-1]
    last_i = last_current_liabilities_row.name

    selected_sheet = sheet_df.iloc[first_i:last_i+1]
    return_sheet = selected_sheet[[first_col, year_col]]

    multiplier = 1
    if "million" in return_sheet.columns[0].lower():
        multiplier = 1000000
    elif "thousands" in return_sheet.columns[0].lower():
        multiplier = 1000

    return_sheet[year_col] = return_sheet[year_col]*multiplier
    return_sheet = return_sheet.rename(columns={first_col: "title"})

    return return_sheet


def clean_df(df_per_sheet, year):

    # Put years in columns if in first row
    for sheet, df in df_per_sheet.items():
        title = df.columns[0]

        # Kill the columns of X month ended X < 12
        columns_to_keep = []
        for column in df.columns:
            clean_col = column.lower(
            )[:-1] if column.lower()[-1] == "s" else column.lower()
            if "month" in clean_col:
                months_duration = int(
                    "".join([char for char in column if char.isdigit()]))
                if months_duration == 12:
                    columns_to_keep.append(column)

        if columns_to_keep:
            df = df[[title, *columns_to_keep]]

        for year_i in [str(int(year) + 1), year]:
            r = re.compile(".*" + year_i)
            year_col_list = list(
                filter(r.match, df.columns))
            if year_col_list:
                cleaned_df = df[[title, year_col_list[0]]]
                df_per_sheet[sheet] = cleaned_df
                break
            else:
                df.iloc[0] = df.iloc[0].fillna("")
                first_row = [str(value) for value in df.iloc[0].values[1:]]
                year_first_row = list(
                    filter(r.match, first_row))
                if year_first_row:
                    new_columns = [title] + list(first_row)
                    columns_renaming = dict(zip(df.columns, new_columns))
                    cleaned_df = df.rename(columns=columns_renaming)
                    cleaned_df = cleaned_df[[title, year_first_row[0]]]
                    df_per_sheet[sheet] = cleaned_df
                    break

    # Put "title" as sheet name and lower
    df_per_sheet_title = {}
    for sheet, df in df_per_sheet.items():
        title = df.columns[0].lower()
        df_per_sheet_title[title] = df

    # Add parts titles within following lines
    for sheet, df in df_per_sheet.items():
        title = df.columns[0]
        year_col = df.columns[1]
        if year in year_col:
            try:
                df["isnull"] = df[year_col].isnull()
            except Exception:
                continue
            str_add = ""
            titles = [df.loc[0, title]]
            for i, row in df.iterrows():
                if i == 0:
                    continue
                row_v = row[title]
                if row["isnull"]:
                    str_add = ""
                    if isinstance(row_v, str) and row_v[-1] == ":":
                        str_add = row_v[:-1]
                        titles.append(row_v)
                        continue
                if str_add:
                    titles.append("(" + str_add + ") " + row_v)
                    if isinstance(row_v, str) and (
                            "total" in row_v.lower()):
                        str_add = ""
                else:
                    titles.append(row_v)
            df[title] = titles

    return df_per_sheet_title


def clean_col_and_multiplier(sheet_df, year):
    r = re.compile(".*" + year)
    year_col_list = list(
        filter(r.match, sheet_df.columns))
    if len(year_col_list) == 0:
        next_year = str(int(year)+1)
        r = re.compile(".*" + next_year)
        year_col_list_one = list(
            filter(r.match, sheet_df.columns))
        if len(year_col_list_one):
            # print(f"End of year {year} in the start of year {next_year}")
            year_col = year_col_list_one[0]
        else:
            sys.exit("Correct year {year} not found")
    elif len(year_col_list) >= 1:
        year_col = year_col_list[0]

    # Get multiplier
    # TODO
    # Check other CSVs to make sure that works

    # TODO
    # What happens if both million and thousands in title
    first_col = sheet_df.columns[0]
    multiplier = 1
    if "million" in first_col.lower():
        multiplier = 1000000
    elif "thousands" in first_col.lower():
        multiplier = 1000

    sheet_df[first_col] = sheet_df[first_col].apply(
        lambda x: x.lower() if isinstance(x, str) else x)
    return sheet_df, first_col, year_col, multiplier


def parse_data_from_sheet(income_statement_name, df_10k_per_sheet,
                          target_sheet, data_list, year):

    year_col_return = None
    dict_data = {}
    if target_sheet == income_statement_name:
        sheet_key = target_sheet
    else:
        sheet_key = regex_per_word_wrapper(
            target_sheet, df_10k_per_sheet.keys())[0]
    sheet_df = df_10k_per_sheet[sheet_key]

    sheet_df, first_col, year_col, multiplier = clean_col_and_multiplier(
        sheet_df, year)

    if target_sheet == "balance sheet":
        year_col_return = year_col
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
            print("####", data_point, " not found")
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

    print(f"\n#### Target sheet : {target_sheet} used the year column "
          f"{year_col}\n")
    df_data = pd.DataFrame.from_dict(dict_data, orient="index")

    return df_data, year_col_return


def regex_per_word_wrapper(input_words, target_list):

    list_r = input_words.lower().split(" ")
    target_list = [target.lower() for target in target_list]
    keys_array = np.array(list(target_list))
    keys = [key.split(" ") for key in target_list]

    match_key = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_keys = keys_array[match_key]
    if len(selected_keys):
        return selected_keys
    return None


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


def main(tickers):

    dl_folder = "10k_data"
    os.makedirs(dl_folder, exist_ok=True)
    print("Parsing the last 5 10K documents from tickers:",
          " ".join(tickers))
    ciks = get_cik(tickers)

    today_stock_per_ticker = {}
    for ticker in tickers:
        today_stock_per_ticker[ticker] = round(si.get_live_price(ticker), 2)
    priorto = datetime.datetime.today().strftime("%Y%m%d")

    last_year = int(priorto[:4]) - 1
    years = range(last_year-4, last_year+1)

    valid_years_per_ticker = download_10k(ciks, priorto, years, dl_folder)
    select_data(tickers, valid_years_per_ticker,
                dl_folder, today_stock_per_ticker)


def select_data(tickers, valid_years_per_ticker, dl_folder,
                today_stock_per_ticker):

    for ticker in tickers:
        years = valid_years_per_ticker[ticker.lower()]
        dir_ticker = os.path.join(dl_folder, ticker)
        dict_data_year = {}
        all_lease_dfs = {}
        current_liabilities_dfs = {}
        stock_price_per_year = {}
        _10k_fpaths = [os.path.join(dir_ticker, fname)
                       for fname in os.listdir(dir_ticker) if fname.split(".")[
                           -1] == "xlsx"]
        for _10k_fpath in _10k_fpaths:
            print(_10k_fpath)
            year = _10k_fpath.split(".")[0][-4:]
            print("\n" + " "*8 + "#"*44)
            print(" "*8 + f"######### Selecting data from {year} #########")
            print(" "*8 + "#"*44 + "\n")

            df_10k_per_sheet = pd.read_excel(_10k_fpath, sheet_name=None)
            df_10k_per_sheet = clean_df(df_10k_per_sheet, year)

            # TODO
            # Find if that split makes sense for other tickers
            # May need to pull first column instead of sheet title

            # TODO
            # Make sure the currency is correct

            # TODO
            # Fix hack weightedaverage

            # TODO
            # Are positive/negative values ok
            # "statements of operations" or "income statement"

            income_statement_name = find_income_statement(df_10k_per_sheet)
            data_per_sheet = {
                "balance sheet": ["total assets", "total liabilities",
                                  "cash", "property equipment", "equity",
                                  "goodwill", "intangible", "debt"],
                income_statement_name: ["operating income", "operating profit",
                                        "weightedaverage", "weighted average",
                                        "income tax", "income taxes",
                                        "net income", "interest expense",
                                        "per share", "dividend"],
                "statements cash flows": ["cash operating", "cash operation"]
            }

            # Check if all match
            for sheet, data_list in data_per_sheet.items():
                if sheet == income_statement_name:
                    continue
                selected_sheets = regex_per_word_wrapper(
                    sheet, df_10k_per_sheet.keys())
                if not len(selected_sheets):
                    print("####", sheet, "not found")
                    sys.exit()

            all_df_data = []
            for target_sheet, data_list in data_per_sheet.items():
                df_data, year_col = parse_data_from_sheet(
                    income_statement_name, df_10k_per_sheet, target_sheet,
                    data_list, year)
                all_df_data.append(df_data)
            lease_df = get_lease_df(df_10k_per_sheet, year)
            all_lease_dfs[year] = lease_df
            current_liabilities_df = get_current_liabilities_df(
                df_10k_per_sheet, year)
            all_df_data_concat = pd.concat(all_df_data)
            # NO YEAR COL
            if year_col:
                stock_price_per_year[year] = get_stock_price(year_col, ticker)
            if current_liabilities_df is not None:
                current_liabilities_dfs[year] = current_liabilities_df
            dict_data_year[year] = all_df_data_concat

        list_data_year = []
        for year in sorted(dict_data_year.keys(), reverse=True):
            list_data_year.append(dict_data_year[year])
        df_output = pd.concat(list_data_year, axis=1, join="outer")
        df_output.columns = years

        stock_price_per_year["now"] = today_stock_per_ticker[ticker]

        list_current_liabilities = []
        for year in sorted(current_liabilities_dfs.keys(), reverse=True):
            list_current_liabilities.append(current_liabilities_dfs[year])
        if list_current_liabilities:
            merged_current_liabilities_df = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["title"], how="outer"),
                list_current_liabilities)
            merged_current_liabilities_df.to_csv(os.path.join(
                dir_ticker, "current_liabilities.csv"))
        list_future_lease = []
        for year in sorted(all_lease_dfs.keys(), reverse=True):
            list_future_lease.append(all_lease_dfs[year])
        df_output_lease = pd.concat(list_future_lease, axis=1, join="outer")

        df_stock_prices = pd.DataFrame.from_dict(
            stock_price_per_year, orient="index").T
        cols = list(df_stock_prices.columns)
        cols.remove("now")
        int_col_ordered = sorted(cols, reverse=True)
        df_stock_prices = df_stock_prices[["now", *int_col_ordered]]

        df_output.to_csv(os.path.join(dir_ticker, "selected_data.csv"))
        df_output_lease.to_csv(os.path.join(dir_ticker, "future_lease.csv"))
        df_stock_prices.to_csv(os.path.join(dir_ticker, "stock_prices.csv"))


def get_stock_price(year_col, ticker):
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    year = year_col[-4:]
    rest = "".join(filter(str.isalnum, year_col.replace(year, "")))
    match_month = [month for month in months if month in rest]
    assert len(match_month) == 1
    month_name = "".join(filter(str.isalnum, match_month[0]))
    day = rest.replace(month_name, "")
    month = str(months.index(month_name) + 1)
    start_date = "-".join([year, month, day])

    one_day = datetime.timedelta(days=1)
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = start_date + one_day

    stock_price = 0
    while not stock_price:
        stock_df = yf.download(ticker, start_date, end_date)
        if len(stock_df):
            stock_price = round(stock_df["Close"].values[0], 2)
        else:
            start_date = start_date - one_day
            end_date = end_date - one_day

    return stock_price


def parse_args():
    # Parse command line
    parser = argparse.ArgumentParser(
        description="Preprocessing Pipeline")
    parser.add_argument(
        "--tickers",
        nargs="+")
    parser.add_argument(
        "--tickers_csv_fpath",
        type=str,
        default="list_tickers.csv")
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()
    if args.tickers:
        tickers = args.tickers
    else:
        tickers_df = pd.read_csv(args.tickers_csv_fpath)
        tickers = tickers_df["ticker"]

    main(tickers)
