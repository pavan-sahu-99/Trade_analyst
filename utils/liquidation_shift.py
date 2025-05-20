## Liquidation Map
# Identifies potential liquidation zones from option chain data.
'''
Scenario	            OI + Price Action	                    Signal	            Confirmation
Bullish Breakout	    CE unwinding + Price ↑ + Low CE OI	✅ Buy Call	           High PE sell volume
Bearish Breakdown	    PE unwinding + Price ↓ + Low PE OI	✅ Buy Put	           High CE sell volume
Bull Trap (Fakeout)	    Price ↑ but PE OI ↑ (Short buildup)	❌ Avoid Calls	       Low CE volume + High PE OI
Bear Trap (Fakeout)	    Price ↓ but CE OI ↑ (Call buildup)	❌ Avoid Puts	       Low PE volume + High CE OI
Battle Zone (Volatile)	CE & PE OI both rising	            ⚠️ Wait for breakout    High volume both sides

'''

import pandas as pd
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import random

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
        time.sleep(random.uniform(2, 4))

        text = driver.find_element("tag name", "pre").text
        response = json.loads(text)
        data = response.get("records", {}).get("data", [])

        driver.quit()
        if data:
            return pd.json_normalize(data)
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def get_liquidation_zones(df, oi_threshold=20000, unwinding_threshold=-2000, buildup_threshold=2000):
    """
    Identifies potential liquidation, buildup, and conflict zones in option chain data.
    """
    signals = []

    for _, row in df.iterrows():
        strike = row['strikePrice']

        ce_oi = row.get('CE.openInterest', 0)
        ce_oi_chg = row.get('CE.changeinOpenInterest', 0)
        ce_buy = row.get('CE.totalBuyQuantity', 0)
        ce_sell = row.get('CE.totalSellQuantity', 0)

        pe_oi = row.get('PE.openInterest', 0)
        pe_oi_chg = row.get('PE.changeinOpenInterest', 0)
        pe_buy = row.get('PE.totalBuyQuantity', 0)
        pe_sell = row.get('PE.totalSellQuantity', 0)

        # --- CE Unwinding (Bullish Breakout) ---
        if ce_oi >= oi_threshold and ce_oi_chg <= unwinding_threshold and ce_buy > ce_sell:
            signals.append({
                'strike': strike,
                'type': 'CE',
                'signal': 'CE Unwinding - Bullish Resistance Break',
                'action': 'Buy Call',
                'OI': ce_oi, 'Change_in_OI': ce_oi_chg, 'Buy': ce_buy, 'Sell': ce_sell
            })

        # --- PE Unwinding (Bullish Hold) ---
        if pe_oi >= oi_threshold and pe_oi_chg <= unwinding_threshold and pe_sell > pe_buy:
            signals.append({
                'strike': strike,
                'type': 'PE',
                'signal': 'PE Unwinding - Bullish Support Hold',
                'action': 'Buy Call',
                'OI': pe_oi, 'Change_in_OI': pe_oi_chg, 'Buy': pe_buy, 'Sell': pe_sell
            })

        # --- PE Unwinding (Bearish Breakdown) ---
        if pe_oi >= oi_threshold and pe_oi_chg <= unwinding_threshold and pe_buy > pe_sell:
            signals.append({
                'strike': strike,
                'type': 'PE',
                'signal': 'PE Unwinding - Bearish Breakdown',
                'action': 'Buy Put',
                'OI': pe_oi, 'Change_in_OI': pe_oi_chg, 'Buy': pe_buy, 'Sell': pe_sell
            })

        # --- CE Buildup (Bearish) ---
        if ce_oi >= oi_threshold and ce_oi_chg >= buildup_threshold and ce_sell > ce_buy:
            signals.append({
                'strike': strike,
                'type': 'CE',
                'signal': 'CE Buildup - Bearish Resistance',
                'action': 'Buy Put',
                'OI': ce_oi, 'Change_in_OI': ce_oi_chg, 'Buy': ce_buy, 'Sell': ce_sell
            })

        # --- PE Buildup (Bullish) ---
        if pe_oi >= oi_threshold and pe_oi_chg >= buildup_threshold and pe_sell > pe_buy:
            signals.append({
                'strike': strike,
                'type': 'PE',
                'signal': 'PE Buildup - Bullish Support',
                'action': 'Buy Call',
                'OI': pe_oi, 'Change_in_OI': pe_oi_chg, 'Buy': pe_buy, 'Sell': pe_sell
            })

        # --- Conflict Zones ---
        if ce_oi >= oi_threshold and ce_oi_chg >= buildup_threshold and \
           pe_oi >= oi_threshold and pe_oi_chg >= buildup_threshold:
            signals.append({
                'strike': strike,
                'type': 'CONFLICT',
                'signal': 'Battle Zone - Both Sides Building Positions',
                'action': 'Wait and Watch (Volatility Expected)',
                'CE_OI': ce_oi, 'CE_OI_Change': ce_oi_chg,
                'PE_OI': pe_oi, 'PE_OI_Change': pe_oi_chg
            })

        if ce_oi >= oi_threshold and ce_oi_chg <= unwinding_threshold and \
           pe_oi >= oi_threshold and pe_oi_chg <= unwinding_threshold:
            signals.append({
                'strike': strike,
                'type': 'CONFLICT',
                'signal': 'Trap Zone - Both Sides Unwinding',
                'action': 'Watch for Sharp Breakout',
                'CE_OI': ce_oi, 'CE_OI_Change': ce_oi_chg,
                'PE_OI': pe_oi, 'PE_OI_Change': pe_oi_chg
            })

    if not signals:
        signals.append({'signal': 'No strong liquidation signals detected.', 'action': 'No Action'})

    return pd.DataFrame(signals)

# Execute only when run as script
if __name__ == "__main__":
    df = get_data("NIFTY")

    if not df.empty:
        signal_df = get_liquidation_zones(df).dropna(subset=['action'])

        # Top signals for PE and CE
        pe_df = signal_df[signal_df['type'] == 'PE'].sort_values(by='Change_in_OI', ascending=False).head(4)
        ce_df = signal_df[signal_df['type'] == 'CE'].sort_values(by='Change_in_OI', ascending=False).head(4)

        # Combine and sort
        major_levels = pd.concat([pe_df, ce_df]).sort_values(by='strike')
        print(major_levels)
    else:
        print("Dataframe is empty.")
