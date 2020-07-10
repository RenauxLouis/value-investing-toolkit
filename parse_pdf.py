import argparse
import functools
import os
import sys
from collections import defaultdict

import numpy as np
import tabula
from tqdm import tqdm


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


def get_tables_from_pdf_file(fpath):

    tables = tabula.read_pdf(fpath, pages="all", multiple_tables=True)
    map_target_table = defaultdict(list)
    for i, table in enumerate(tables):
        columns = table.columns
        for input_words in ["income statement", "balance sheet",
                            "financial position", "cash flow"]:
            results = regex_per_word_wrapper(input_words, columns)
            if results:
                map_target_table[input_words].append(table)
    return map_target_table


def main(input_folder):

    map_target_table_per_year = {}
    pdf_fnames = os.listdir(input_folder)
    for fname in tqdm(pdf_fnames):
        fpath = os.path.join(input_folder, fname)
        year = fpath.split(".")[-2][-4:]
        print(year)
        map_target_table = get_tables_from_pdf_file(fpath)
        map_target_table_per_year[year] = map_target_table
    for year, value in map_target_table_per_year.items():
        for key, subvalues in value.items():
            for i, subvalue in enumerate(subvalues):
                subvalue.to_csv(str(year) + "_" + key +
                                "_" + str(i) + ".csv")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_folder", type=str)
    parser.add_argument("--ticker_sec", type=str)
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()
    if args.ticker_sec:
        input_folder = download_10k_ticker(ticker_sec)
        convert_htm_to_pdf(input_folder)
    else:
        input_folder = args.input_folder

    main(input_folder)
