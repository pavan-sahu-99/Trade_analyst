import pandas as pd
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


data = pd.read_csv(r"C:\Users\SRI SAI\Desktop\trade-analyst\data\fno_stocks_historic_data.csv")
data["date"] = pd.to_datetime(data["date"])
#symbol,date,open,high,low,close,prev_close,total_trade,volume,delivery_qty,delivery_per,vwap
today_str = datetime.today().strftime("%Y-%m-%d")
 
# Function to fetch today's F&O data
def get_data():
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("start-maximized")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        driver = webdriver.Chrome(options=options)

        driver.get("https://www.nseindia.com")
        time.sleep(3)

        url = f"https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        
        driver.get(url)
        time.sleep(2)

        text = driver.find_element("tag name", "pre").text
        response = json.loads(text)
        stock_data = response.get("data", [])

        driver.quit()

        if stock_data:
            df = pd.DataFrame(stock_data)
            df = df[["symbol", "lastPrice", "previousClose", "dayHigh", "dayLow", "pChange", "totalTradedVolume"]]
            df.columns = ["Symbol", "Last Price", "Prev Close", "High", "Low", "% Change", "Volume"]
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

new_data = get_data()

if not new_data.empty:

    # Add today's date to new data
    new_data["date"] = today_str

    # Step 3: Remove the oldest date
    oldest_date = data["date"].min()
    data = data[data["date"] != oldest_date]
    print(f"🗑️ Removed data for oldest date: {oldest_date.date()}")

    # Step 4: Append new data
    updated_data = pd.concat([data, new_data], ignore_index=True)
    updated_data.to_csv(r"C:\Users\SRI SAI\Desktop\trade-analyst\data\fno_stocks_historic_data.csv", index=False)
    print(f"✅ Appended today's data for {today_str} and saved to file.")
else:
    print("⚠️ No new data fetched. Old data remains unchanged.")