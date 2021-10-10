import os

from sec_edgar_downloader import Downloader

df_f = "pdf"
os.makedirs(df_f, exist_ok=True)
dl = Downloader(download_folder=df_f)

coucou = dl.get("10-K", "MSFT", 5)
print(coucou)
