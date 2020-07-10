import tabula
import sys
import numpy as np
import functools
import argparse


def regex_per_word_wrapper(input_words, target_list):

    list_r = input_words.lower().split(" ")
    target_list = [target for target in target_list if isinstance(target, str)]
    target_list = [target.lower() for target in target_list]
    keys_array = np.array(list(target_list))
    keys = [key.split(" ") for key in target_list]

    match_key = np.array(list(map(functools.partial(
        regex_per_word, list_r=list_r), keys)))
    if match_key.any():
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


file = "../Downloads/2019-Full-year-Results.pdf"

tables = tabula.read_pdf(file, pages="all", multiple_tables=True)


def get_tables_from_pdf_file():
    map_target_table = {}
    for i, table in enumerate(tables):
        # print("#######", i)
        columns = table.columns
        # first_row = list(table.iloc[0].dropna().values)
        print(columns)
        # print(table)
        # if i == 8:
        #     print(table)
        #     table.to_csv("table_oui.csv")
        #     sys.exit()
        for input_words in ["income statement", "balance sheet",
                            "financial position", "cash flow"]:
            results = regex_per_word_wrapper(input_words, columns)
            # print(results)
            if results:
                map_target_table[results[0]] = table
    return map_target_table


def parse_args():
    # Parse command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_folder", nargs="+")
    parser.add_argument("--ticker_sec", type=str)
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
