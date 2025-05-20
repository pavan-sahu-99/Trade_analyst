from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import json

def most_active_eq():
    # Set up headless Chrome options
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

    #service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(options=options)

    try:
        # Step 1: Load NSE homepage to get cookies
        driver.get("https://www.nseindia.com")
        time.sleep(3)  # Let cookies load

        # Step 2: Hit actual API endpoint using Selenium cookies
        url = "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value"
        driver.get(url)
        time.sleep(2)

        # Step 3: Parse JSON response from the page source
        text = driver.find_element("tag name", "pre").text
        data = json.loads(text).get("data", [])

        df = pd.DataFrame(data)
        filtered_df = df[["symbol", "lastPrice", "pChange", "quantityTraded", "totalTradedValue", "lastUpdateTime"]].copy()
        filtered_df.columns = ["symbol", "lastPrice", "%Change", "volume", "totalTradedValue", "lastUpdateTime"]

    except Exception as e:
        print(f"Error occurred: {e}")
        filtered_df = pd.DataFrame()

    finally:
        driver.quit()

    return filtered_df


if __name__ == "__main__":
    data = most_active_eq()
    print(data.head())
