import pandas as pd
import json
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse
# Load symbols

FO_SYMBOLS = pd.read_csv(r"C:\Users\SRI SAI\Desktop\trade-analyst\data\f&o data.csv")
symbols = FO_SYMBOLS["Symbol"].tolist()

s = ['M&M', 'M&MFIN']

# Date range
TODAY = datetime.today()
FROM_DATE = (TODAY - timedelta(days=30)).strftime("%d-%m-%Y")
TO_DATE = TODAY.strftime("%d-%m-%Y")

#urls = []
def get_data(symbol):
    try:

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("start-maximized")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        driver = webdriver.Chrome(options=options)
        driver.get("https://www.nseindia.com")
        time.sleep(2)

        # Construct the URL with the correct symbol
        if symbol not in s:
            url = (
                f"https://www.nseindia.com/api/historical/securityArchives"
                f"?from={FROM_DATE}&to={TO_DATE}&symbol={symbol.upper()}&dataType=priceVolumeDeliverable&series=ALL"
            )
            #urls.append(url)
        else:
            symbol = urllib.parse.quote(symbol)
            url = (
                f"https://www.nseindia.com/api/historical/securityArchives"
                f"?from={FROM_DATE}&to={TO_DATE}&symbol={symbol.upper()}&dataType=priceVolumeDeliverable&series=ALL"
            )
            #urls.append(url)           
        driver.get(url)
        time.sleep(2)

        text = driver.find_element("tag name", "pre").text
        driver.quit()

        response = json.loads(text)
        stock_data = response.get("data", [])

        if stock_data:
            data = pd.DataFrame(stock_data)
            data.rename(columns={
                "CH_SYMBOL": "symbol",
                "CH_TIMESTAMP": "date",
                "CH_OPENING_PRICE": "open",
                "CH_TRADE_HIGH_PRICE": "high",
                "CH_TRADE_LOW_PRICE": "low",
                "CH_CLOSING_PRICE": "close",
                "CH_PREVIOUS_CLS_PRICE": "prev_close",
                "CH_TOTAL_TRADES": "total_trade",
                "CH_TOT_TRADED_QTY": "volume",
                "COP_DELIV_QTY": "delivery_qty",
                "COP_DELIV_PERC": "delivery_per",
                "VWAP": "vwap"
            }, inplace=True)
            df = data[["symbol", "date", "open", "high", "low", "close", "prev_close", "total_trade", "volume", "delivery_qty", "delivery_per", "vwap"]]
            return symbol, df
        else:
            return symbol, pd.DataFrame()

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return symbol, pd.DataFrame()

# Main execution
if __name__ == "__main__":
    all_data = []
    start_time = time.time()
    a = []
    na = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_data, symbol): symbol for symbol in symbols}

        for future in as_completed(futures):
            symbol, df = future.result()
            if not df.empty:
                all_data.append(df)
                a.append(symbol)
            else:
                na.append(symbol)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(r"C:\Users\SRI SAI\Desktop\trade-analyst\data\fno_stocks_historic_data.csv", index=False)
        print("\nData successfully saved to fno_stocks_historic_data.csv ✅")
        print(f" successfully loaded : \n{a}")
        print(f" failed to load : \n{na}")
        #print(f"{urls}\n")
    else:
        print("\nNo data fetched")
        #print(f"{urls}\n")
'''
instead of running this everytime just delete past records from the stocks and append new records
'''