from kiteconnect import KiteConnect
import pandas as pd
import time

def gen_ses():
    key = open(r"kite\data\api.txt","r").read().split()
    kite = KiteConnect(api_key=key[0])
    kite.set_access_token(key[2])
    return kite

def sectorials():
    kite = gen_ses()
    print("Kite Session Generated")
    
    sect_data = pd.read_csv(r'kite\data\data_sect.csv')  # Read sector data
    all_quotes = []

    for _, row in sect_data.iterrows():
        instrument_token = row['instrument_token']
        tradingsymbol = row['name']
        try:
            quote = kite.quote(instrument_token)
            instrument_quote = quote[str(instrument_token)]

            last_price = instrument_quote.get('last_price', 0)
            prev_close = instrument_quote.get('ohlc', {}).get('close', 0)
            change_pct = ((last_price - prev_close) / prev_close) * 100 if prev_close else 0
            
            all_quotes.append({
                'Index': tradingsymbol,
                'LTP': last_price,
                '% Change': round(change_pct, 2),
                'net_change': instrument_quote.get('net_change', 0)
                #'volume': instrument_quote.get('volume', 0)
            })
            time.sleep(1)  # Respect rate limit
        except Exception as e:
            print(f"Error fetching {instrument_token} - {tradingsymbol}: {str(e)}")
            time.sleep(1)  # Sleep even on failure

    df = pd.DataFrame(all_quotes)
    return df

if __name__ == "__main__":
    data = sectorials()
    print(data.head())