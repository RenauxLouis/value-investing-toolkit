import argparse
import functools
import os
import pprint
import re
import sys
from datetime import datetime
from functools import reduce
from shutil import rmtree
import nltk.data
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils import create_document_list, get_cik, save_in_directory

pp = pprint.PrettyPrinter(indent=4)
nltk.download('punkt')
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')


def download_10k(ciks_per_ticker, priorto, years, dl_folder):

    filing_type = "10-K"
    count = 5
    valid_years_per_ticker = {}
    for ticker, cik in ciks_per_ticker.items():
        print("Ticker: ", ticker)
        ticker_folder = os.path.join(dl_folder, ticker)
        if os.path.exists(ticker_folder):
            rmtree(ticker_folder)
        # Get the 10k

        base_url = "http://www.sec.gov/cgi-bin/browse-edgar"
        params = {"action": "getcompany", "owner": "exclude", "output": "xml",
                  "CIK": cik, "type": filing_type, "dateb": priorto,
                  "count": count}
        r = requests.get(base_url, params=params)
        if r.status_code == 200:
            data = r.text
            urls, accession_numbers = create_document_list(data)
            url_fr_per_year = {}
            for i, year in enumerate(years[:: -1]):
                url_fr_per_year[year] = urls[i]

            try:
                valid_years = save_in_directory(ticker_folder, cik, priorto,
                                                url_fr_per_year)
                valid_years_per_ticker[ticker] = valid_years
            except Exception as e:
                sys.exit(e)

            for accession_number, year in zip(accession_numbers, years[::-1]):
                download_10k_htm(cik, accession_number,
                                 ticker_folder, year)

    return valid_years_per_ticker


def download_10k_htm(cik, accession_number, ticker_folder, year):
    year_str = str(year)
    base_edgar_url = "https://www.sec.gov/Archives/edgar/data"
    accession_number_url = os.path.join(base_edgar_url, cik, accession_number)
    r = requests.get(accession_number_url)
    if r.status_code == 200:
        data = r.text
        soup = BeautifulSoup(data, features="lxml")
        links = [link.get("href") for link in soup.findAll("a")]
        htm_urls = [link for link in links if (
            link.split(".")[-1] == "htm" and (
                "10-k" in link or "10k" in link))]

        # TODO
        # Find a better method to pick the correct htm (can't get cik V)
        htm_fname = os.path.basename(htm_urls[0])
        htm_url = os.path.join(accession_number_url, htm_fname)
        r_htm = requests.get(htm_url)
        fpath = os.path.join(ticker_folder, f"10k_{year_str}.htm")
        with open(fpath, "wb") as output:
            output.write(r_htm.content)


def find_income_statement(df_10k_per_sheet):
    df_10k_keys = df_10k_per_sheet.keys()
    list_possibles = ["statement income", "statement earning"]
    list_indexes = []
    sheet_per_index = {}
    for possible in list_possibles:
        selected_sheet = regex_per_word_wrapper(
            possible, df_10k_keys)
        if selected_sheet:
            index = list(df_10k_keys).index(selected_sheet)
            list_indexes.append(index)
            sheet_per_index[index] = selected_sheet

    selected_index = min(list_indexes)
    return sheet_per_index[selected_index]


def select_data(tickers, valid_years_per_ticker, dl_folder):

    for ticker in tickers:
        years = valid_years_per_ticker[ticker.lower()]
        dir_ticker = os.path.join(dl_folder, ticker)
        dict_data_year = {}
        all_lease_dfs = {}
        current_liabilities_dfs = {}
        _10k_fpaths = [os.path.join(dir_ticker, fname)
                       for fname in os.listdir(dir_ticker) if fname.split(".")[
                           -1] == "xlsx"]
        for _10k_fpath in _10k_fpaths:
            print(_10k_fpath)
            year = _10k_fpath.split(".")[0][-4:]
            print("Selecting data from", year)

            df_10k_per_sheet = pd.read_excel(_10k_fpath, sheet_name=None)
            df_10k_per_sheet = clean_df(df_10k_per_sheet, year)

            # pp.pprint(list(df_10k_per_sheet.keys()))
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
            print("###income_statement_name : ", income_statement_name)
            data_per_sheet = {
                "balance sheet": ["total assets", "total liabilities",
                                  "cash and cash equivalents",
                                  "property equipment", "equity",
                                  "goodwill", "intangible assets", "debt"],
                income_statement_name: ["operating income",
                                        "operating profit",
                                        "weightedaverage",
                                        "weighted average",
                                        "net income", "interest expense",
                                        "per share", "dividend"],
                "statements cash flows": ["cash operating", "cash operation"]
            }

            # Check if all match
            for sheet, data_list in data_per_sheet.items():
                if sheet == income_statement_name:
                    continue
                selected_sheet = regex_per_word_wrapper(
                    sheet, df_10k_per_sheet.keys())
                if not len(selected_sheet):
                    print("####", sheet, "not found")
                    sys.exit()
            print("All needed sheet are found in the 10k document")

            all_df_data = []
            for target_sheet, data_list in data_per_sheet.items():
                print(target_sheet)
                df_data = parse_data_from_sheet(income_statement_name,
                                                df_10k_per_sheet, target_sheet,
                                                data_list, year)
                all_df_data.append(df_data)
            all_df_data_concat = pd.concat(all_df_data)
            lease_df = get_lease_df(df_10k_per_sheet, year)
            all_lease_dfs[year] = lease_df
            current_liabilities_df = get_current_liabilities_df(
                df_10k_per_sheet, year)
            if current_liabilities_df is not None:
                current_liabilities_dfs[year] = current_liabilities_df
            dict_data_year[year] = all_df_data_concat

        list_data_year = []
        for year in sorted(dict_data_year.keys(), reverse=True):
            list_data_year.append(dict_data_year[year])
        df_output = pd.concat(list_data_year, axis=1, join="outer")
        df_output.columns = list(years)[::-1]

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

        df_output.to_csv(os.path.join(dir_ticker, "selected_data.csv"))
        df_output_lease.to_csv(os.path.join(dir_ticker, "future_lease.csv"))


def get_lease_df(df_10k_per_sheet, year):
    list_r = ["operating", "lease"]
    keys_array = np.array(list(df_10k_per_sheet.keys()))
    keys = [key.split(" ") for key in df_10k_per_sheet.keys()]
    match_keys = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_key = keys_array[match_keys]
    if selected_key.size > 0:
        return df_10k_per_sheet[selected_key[0]]
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
        selected_df = df_10k_per_sheet[selected_key[0]]
        r = re.compile(".*" + year)
        year_col_list = list(
            filter(r.match, selected_df.columns))
        assert len(year_col_list) == 1
        year_col = year_col_list[0]

        full_texts = selected_df[year_col].fillna("").values
        select_text = max(full_texts, key=len)
        sentences = tokenizer.tokenize(select_text)
        sentences_lease = [
            sentence for sentence in sentences if "lease" in sentence]

        data = [[" ".join(sentences_lease)]]
        output_df = pd.DataFrame(data, columns=[year])

        return output_df
    return None


def get_current_liabilities_df(df_10k_per_sheet, year):
    selected_key = regex_per_word_wrapper(
        "balance sheet", df_10k_per_sheet.keys())
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
    r = re.compile(".*" + year)
    r_2 = re.compile(".*" + str(int(year) + 1))
    for sheet, df in df_per_sheet.items():
        title = df.columns[0]
        year_col_list = list(
            filter(r.match, df.columns))
        if not year_col_list:
            df.iloc[0] = df.iloc[0].fillna("")
            first_row = [str(value) for value in df.iloc[0].values[1:]]
            year_first_row = list(
                filter(r.match, first_row))
            next_year_first_row = list(
                filter(r_2.match, first_row))
            if year_first_row or next_year_first_row:
                new_columns = [title] + list(first_row)
                columns_renaming = dict(zip(df.columns, new_columns))
                cleaned_df = df.rename(columns=columns_renaming)
                df_per_sheet[sheet] = cleaned_df

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
            print(f"End of year {year} in the start of year {next_year}")
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
    if "million" in first_col.lower():
        multiplier = 1000000
    elif "thousands" in first_col.lower():
        multiplier = 1000

    sheet_df[first_col] = sheet_df[first_col].str.lower()
    return sheet_df, first_col, year_col, multiplier


def parse_data_from_sheet(income_statement_name, df_10k_per_sheet,
                          target_sheet, data_list, year):

    dict_data = {}
    if target_sheet == income_statement_name:
        sheet_key = target_sheet
    else:
        sheet_key = regex_per_word_wrapper(
            target_sheet, df_10k_per_sheet.keys())
    sheet_df = df_10k_per_sheet[sheet_key]
    print(sheet_key)
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

    df_data = pd.DataFrame.from_dict(dict_data, orient="index")

    return df_data


def regex_per_word_wrapper(target_sheet, df_10k_keys):

    list_r = target_sheet.split(" ")
    keys_array = np.array(list(df_10k_keys))
    keys = [key.split(" ") for key in df_10k_keys]
    match_key = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    selected_key = keys_array[match_key]
    if len(selected_key):
        return selected_key[0]
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


def main(tickers_csv_fpath):

    dl_folder = "10k_data"
    tickers_df = pd.read_csv(tickers_csv_fpath)
    tickers = tickers_df["ticker"]
    print("Parsing the last 5 10K documents from tickers:",
          " ".join(tickers))
    ciks = get_cik(tickers)

    priorto = datetime.today().strftime("%Y%m%d")
    last_year = int(priorto[:4]) - 1
    years = range(last_year-4, last_year+1)

    valid_years_per_ticker = download_10k(ciks, priorto, years, dl_folder)
    select_data(tickers, valid_years_per_ticker, dl_folder)


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
