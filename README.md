# Value Investing toolkit 

Toolkit to gather data following the principles in Value Investing classic book [Good Stock Cheap](https://www.amazon.com/Good-Stocks-Cheap-Confidence-Outperformance/dp/125983607X) by Kenneth Jeffrey Marshall.

To grab all relevant data from a given company run
```
python3 download_10k.py --tickers AAPL MSFT --dl_folder_fpath "folder"
```
which will pull the 10K files from the past 5 years and save them as Excel files locally in the given folder
