import argparse
import datetime
import os
import re
import sys
from collections import defaultdict
from shutil import rmtree

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://www.sec.gov/cgi-bin/browse-edgar"
BASE_EDGAR_URL = "https://www.sec.gov/Archives/edgar/data"


def download_10k(ciks_per_ticker, priorto, years, dl_folder):

    # TODO
    # Allow for specific year selection
    count = 5
    ext = "htm"
    for ticker, cik in ciks_per_ticker.items():
        print("Ticker: ", ticker)
        ticker_folder = os.path.join(dl_folder, ticker)
        if os.path.exists(ticker_folder):
            rmtree(ticker_folder)

        os.makedirs(ticker_folder)

        filing_type = "10-K"
        params = {"action": "getcompany", "owner": "exclude",
                  "output": "xml", "CIK": cik, "type": filing_type,
                  "dateb": priorto, "count": count}
        r = requests.get(BASE_URL, params=params)
        if r.status_code != 200:
            sys.exit("Ticker data not found")
        else:
            data = r.text
            soup = BeautifulSoup(data, features="lxml")

            urls = [link.string for link in soup.find_all(
                "filinghref")]
            types = [link.string for link in soup.find_all(
                "type")]

            current_year = years[-1]
            urls_per_year = defaultdict(dict)
            N_10k = 0
            for type_file, url in zip(types, urls):
                if N_10k == 5:
                    break
                urls_per_year[current_year][type_file] = url

                if type_file == "10-K":
                    current_year -= 1
                    N_10k += 1

        filing_type = "DEF 14A"
        params = {"action": "getcompany", "owner": "exclude",
                  "output": "xml", "CIK": cik, "type": filing_type,
                  "dateb": priorto, "count": count}
        r = requests.get(BASE_URL, params=params)
        if r.status_code != 200:
            sys.exit("Ticker data not found")
        else:
            data = r.text
            soup = BeautifulSoup(data, features="lxml")
            urls = [link.string for link in soup.find_all(
                "filinghref")]

            current_year = years[-1]
            for url in urls[:5]:
                urls_per_year[current_year][filing_type] = url
                current_year -= 1

        map_regex = {
            "10-K": ["10-k", "10k"],
            "10-K/A": ["htm", "10-ka"],
            "DEF 14A": ["", ""],
        }
        map_prefix = {
            "10-K": "10K",
            "10-K/A": "10K_amended",
            "DEF 14A": "Proxy_Statement",
        }
        for year, urls in urls_per_year.items():
            for file_type, url in urls.items():
                prefix = map_prefix[file_type]
                accession_numbers = [url.split("/")[-2]]
                full_url = get_files_url(cik, accession_numbers,
                                         "htm", *map_regex[file_type])
                r = requests.get(full_url[0])
                if r.status_code == 200:
                    os.makedirs(ticker_folder, exist_ok=True)
                    fpath = os.path.join(
                        ticker_folder, f"{prefix}_{year}.{ext}")
                    with open(fpath, "wb") as output:
                        output.write(r.content)


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


def get_cik(tickers):
    URL = "http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner"
    "=exclude&action=getcompany"
    CIK_RE = re.compile(r".*CIK=(\d{10}).*")

    cik_dict = {}
    for ticker in tickers:
        f = requests.get(URL.format(ticker), stream=True)
        results = CIK_RE.findall(f.text)
        if len(results):
            cik_dict[str(ticker).lower()] = str(results[0])
    return cik_dict


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
