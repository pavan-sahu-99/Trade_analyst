## Test for OI data and analysis

import pandas as pd
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_data(symbol):
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

        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        
        driver.get(url)
        time.sleep(2)

        text = driver.find_element("tag name", "pre").text
        response = json.loads(text)
        data = response.get("records", {}).get("data", [])

        driver.quit()
        if data:
            df = pd.json_normalize(data)

            return df
        else:
            return pd.DataFrame()
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def analyze_option_chain(df, range_width=500):
    #Fill missing volume data
    df["CE.totalTradedVolume"] = df["CE.totalTradedVolume"].fillna(0)
    df["PE.totalTradedVolume"] = df["PE.totalTradedVolume"].fillna(0)

    #Filter out strikes with zero volume on both sides
    df = df[(df["CE.totalTradedVolume"] > 0) | (df["PE.totalTradedVolume"] > 0)]

    # Get underlying price
    underlying_price = df["CE.underlyingValue"].dropna().iloc[0]

    #Filter strikes within ±1000 of underlying
    df = df[(df["strikePrice"] >= underlying_price - range_width) & (df["strikePrice"] <= underlying_price + range_width)]
    df["expiryDate"] = pd.to_datetime(df["expiryDate"], format="%d-%b-%Y")
    latest_expiry = df["expiryDate"].min()
    #print(latest_expiry)
    df_latest = df[df["expiryDate"] == latest_expiry].copy()

    #Calculate support/resistance
    support_strike = df_latest.loc[df_latest["PE.openInterest"].idxmax()]["strikePrice"]
    resistance_strike = df_latest.loc[df_latest["CE.openInterest"].idxmax()]["strikePrice"]

    #Add % OI change and IV skew
    #overall
    df["PE_OI_Change_%"] = df["PE.pchangeinOpenInterest"]
    df["CE_OI_Change_%"] = df["CE.pchangeinOpenInterest"]
    df["IV_Skew"] = df["CE.impliedVolatility"] - df["PE.impliedVolatility"]
    # Latest
    df_latest["PE_OI_Change_%"] = df_latest["PE.pchangeinOpenInterest"]
    df_latest["CE_OI_Change_%"] = df_latest["CE.pchangeinOpenInterest"]
    df_latest["IV_Skew"] = df_latest["CE.impliedVolatility"] - df_latest["PE.impliedVolatility"]

    #Compute Bid-Ask Spreads
    #overall
    df["CE_Spread"] = df["CE.askPrice"] - df["CE.bidprice"]
    df["PE_Spread"] = df["PE.askPrice"] - df["PE.bidprice"]

    top_pe_oi_change_o = df.sort_values(by="PE_OI_Change_%", ascending=False).head(3)
    top_ce_oi_change_o = df.sort_values(by="CE_OI_Change_%", ascending=False).head(3)
    top_iv_skew_o = df.sort_values(by="IV_Skew", ascending=False).head(3)
    #latest
    df_latest["CE_Spread"] = df_latest["CE.askPrice"] - df_latest["CE.bidprice"]
    df_latest["PE_Spread"] = df_latest["PE.askPrice"] - df_latest["PE.bidprice"]

    top_pe_oi_change_l = df_latest.sort_values(by="PE_OI_Change_%", ascending=False).head(3)
    top_ce_oi_change_l = df_latest.sort_values(by="CE_OI_Change_%", ascending=False).head(3)
    top_iv_skew_l = df_latest.sort_values(by="IV_Skew", ascending=False).head(3)

    
    # Here, the options that meet this criteria are:
    liquid_ce = df[df["CE_Spread"] < 2].sort_values(by="CE.totalTradedVolume", ascending=False).head(3)
    liquid_pe = df[df["PE_Spread"] < 2].sort_values(by="PE.totalTradedVolume", ascending=False).head(3)

    #PCR ratio:
    total_pe_oi = df["PE.openInterest"].sum()
    total_ce_oi = df["CE.openInterest"].sum()
    pcr = total_pe_oi / total_ce_oi if total_ce_oi != 0 else float('inf')

    #print(f"Underlying Price: {underlying_price}")
    #print(f"🟢 Strongest Support (PE OI): {support_strike}")
    
    #print(f"🔴 Strongest Resistance (CE OI): {resistance_strike}")
    #print(f"Put-Call Ratio (PCR): {pcr:.2f}")
    return {
        "Filtered Data": df,
        "Top PE OI Change Overall": top_pe_oi_change_o,
        "Top CE OI Change Overall": top_ce_oi_change_o,
        "Top IV Skew Overall": top_iv_skew_o,
        "Top PE OI Change Latest": top_pe_oi_change_l,
        "Top CE OI Change Latest": top_ce_oi_change_l,
        "Top IV Skew Latest": top_iv_skew_l,        
        "Liquid Calls": liquid_ce,
        "Liquid Puts": liquid_pe
    }

if __name__ == "__main__":
    df = get_data("NIFTY")
    #df = pd.read_csv(r"C:\Users\SRI SAI\Desktop\trade-analyst\test_in_progress\oi_data.csv")
    if not df.empty:
        results = analyze_option_chain(df)

        print(f"\nPut side Overall: \n{results['Top PE OI Change Overall'][['expiryDate', 'strikePrice', 'PE.openInterest', 'PE_OI_Change_%']]}")
        print(f"\nCall side Overall: \n{results['Top CE OI Change Overall'][['expiryDate', 'strikePrice', 'CE.openInterest', 'CE_OI_Change_%']]}")
        print(f"\nPut side Latest: \n{results['Top PE OI Change Latest'][['expiryDate', 'strikePrice', 'PE.openInterest', 'PE_OI_Change_%']]}")
        print(f"\nCall side Latest: \n{results['Top CE OI Change Latest'][['expiryDate', 'strikePrice', 'CE.openInterest', 'CE_OI_Change_%']]}")
        print(f"\nLiquid Calls Latest: \n{results['Liquid Calls'][['expiryDate', 'strikePrice', 'CE.openInterest', 'CE_OI_Change_%', 'IV_Skew', 'CE_Spread' ,'PE_Spread']]}")
        print(f"\nLiquid Puts: \n{results['Liquid Puts'][['expiryDate', 'strikePrice', 'PE.openInterest', 'PE_OI_Change_%', 'IV_Skew', 'CE_Spread' ,'PE_Spread']]}")        
        print(f"\nIV Skew Overall : \n{results['Top IV Skew Overall'][['expiryDate', 'strikePrice', 'IV_Skew']]}")
        print(f"\nIV Skew Latest: \n{results['Top IV Skew Latest'][['expiryDate', 'strikePrice', 'IV_Skew']]}")
    else:
        print("No data fetched.")
