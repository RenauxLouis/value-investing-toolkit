import argparse
import datetime
import os
import sys
from shutil import rmtree

import requests
from bs4 import BeautifulSoup

from utils import get_cik, save_in_directory

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

                    full_urls_per_type["10-K"] = get_files_url(
                        cik, accession_numbers_10k, "htm", "10-k", "10k")

                    if accession_numbers_10k_amended:
                        input(
                            "THERE ARE AMENDED 10-K FILES, PLEASE CHECK THEM "
                            "(<enter> to continue) ")
                        full_urls_per_type["10-K_amended"] = get_files_url(
                            cik, accession_numbers_10k_amended, "htm", "10-ka",
                            "10ka")

                elif filing_type == "DEF 14A":
                    accession_numbers = [
                        link.split("/")[-2] for link in urls[:5]]
                    full_urls_per_type["Proxy_Statement"] = get_files_url(
                        cik, accession_numbers, "htm", "", "")
            else:
                sys.exit("Ticker data not found")

        for file_type, urls in full_urls_per_type.items():
            print(file_type)
            ext = "htm"
            url_fr_per_year = {}
            if file_type == "10-K_amended":
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
            if urls:
                fname = os.path.basename(urls[0])
                url = os.path.join(accession_number_url, fname).replace(
                    "\\", "/")
                return_urls.append(url)
            else:
                return_urls.append("check_amended")
    return return_urls


def main(tickers, dl_folder_fpath):

    os.makedirs(dl_folder_fpath, exist_ok=True)
    print("Parsing the last 5 10K documents from tickers:",
          " ".join(tickers))
    ciks = get_cik(tickers)

    priorto = datetime.datetime.today().strftime("%Y%m%d")

    last_year = int(priorto[:4]) - 1
    years = range(last_year-4, last_year+1)

    download_10k(ciks, priorto, years, dl_folder_fpath)


def parse_args():
    # Parse command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--dl_folder_fpath", type=str)
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()
    main(args.tickers, args.dl_folder_fpath)
