from datetime import datetime
import os
import re

import requests


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


def get_current_date_time_as_prefix():
    now = datetime.utcnow()
    time_now = str(now)[:-7].replace(" ", "_").replace(
        ":", ""
    ).replace("-", "") + "_"

    return time_now
