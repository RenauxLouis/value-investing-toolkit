import os
import re

import requests
from bs4 import BeautifulSoup


def get_cik(tickers):
    URL = "http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany"
    CIK_RE = re.compile(r".*CIK=(\d{10}).*")

    cik_dict = {}
    for ticker in tickers:
        f = requests.get(URL.format(ticker), stream=True)
        results = CIK_RE.findall(f.text)
        if len(results):
            cik_dict[str(ticker).lower()] = str(results[0])
    return cik_dict


def create_document_list(data):

    soup = BeautifulSoup(data, features="lxml")
    link_list = [link.string for link in soup.find_all("filinghref")][:5]

    accession_numbers = [link.split("/")[-2] for link in link_list]
    print("Number of files to download: {0}".format(len(link_list)))

    return link_list, accession_numbers


def save_in_directory(ticker_folder, cik, priorto, ext,
                      file_type, url_fr_per_year):

    valid_years = []
    for year, url in url_fr_per_year.items():
        year_str = str(year)
        r = requests.get(url)
        if r.status_code == 200:
            valid_years.append(year)
            os.makedirs(ticker_folder, exist_ok=True)
            fpath = os.path.join(
                ticker_folder, f"{file_type}_{year_str}.{ext}")
            with open(fpath, "wb") as output:
                output.write(r.content)
    return valid_years
