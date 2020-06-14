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
    """Create list of txt urls and doc names.
    Args:
            data (str): Raw HTML from SEC Edgar lookup.
    Returns:
            list: Zipped list with tuples of the form
            (<url for txt file>, <doc name>)
    """
    soup = BeautifulSoup(data, features="lxml")
    # store the link in the list
    link_list = [link.string for link in soup.find_all("filinghref")]
    date_list = [int(link.string[:4]) -
                 1 for link in soup.find_all("datefiled")]

    print("Number of files to download: {0}".format(len(link_list)))
    print("Starting download...")

    # List of url to the text documents
    fr_urls = [os.path.join(os.path.dirname(
        link), "Financial_Report.xlsx") for link in link_list]

    return dict(zip(date_list, fr_urls))


def save_in_directory(company_code, cik, priorto, url_fr_per_year):
    """Save in directory based on filing info.
    Args:
            company_code (str): Code used to help find company filings.
            Often the company"s ticker is used.
            cik (Union[str, int]): Central Index Key assigned by SEC.
            See https://www.sec.gov/edgar/searchedgar/cik.htm to search for
            a company"s CIK.
            priorto (Union[str, datetime.datetime]): Most recent report to consider.
            Must be in form "YYYYMMDD" or
            valid ``datetime.datetime`` object.
            urls (str): List of urls
    Returns:
            None
    """

    for year, url in url_fr_per_year.items():
        year_str = str(year)
        print(url)
        r = requests.get(url)

        os.makedirs(company_code, exist_ok=True)
        fpath = os.path.join(company_code, f"10k_{year_str}.xlsx")
        with open(fpath, "wb") as output:
            output.write(r.content)