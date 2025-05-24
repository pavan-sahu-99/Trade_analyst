from kiteconnect import KiteConnect
import pandas as pd
import json
import time
from datetime import datetime, timedelta

def gen_ses():
    """Generate KiteConnect session"""
    with open(r"kite\data\api.txt", "r") as f:
        key = f.read().split()
    kite = KiteConnect(api_key=key[0])
    kite.set_access_token(key[2])
    return kite

def calculate_r_score(df, min_days=18):
    """
    Enhanced R-Score calculation combining both approaches
    """
    # Ensure proper datetime handling
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.tz_localize(None)
    df['day'] = df['date'].dt.date
    
    # Calculate daily metrics
    df['turnover'] = df['close'] * df['volume']
    #df['return'] = (df['close'] - df['open']) / df['open']
    
    results = []
    
    for token in df['instrument_token'].unique():
        token_df = df[df['instrument_token'] == token].sort_values('day')
        
        if len(token_df) < min_days:
            continue  # Skip if insufficient data
            
        latest_day = token_df['day'].max()
        latest_candle = token_df[token_df['day'] == latest_day].iloc[0]
        
        past_candles = token_df[token_df['day'] < latest_day].sort_values('day').tail(min_days)
        
        # Calculate averages and standard deviations
        metrics = {
            'volume': (0.2, past_candles['volume']),
            'turnover': (0.3, past_candles['turnover']),
            'return': (0.5, (past_candles['close'] - past_candles['open']) / past_candles['open'])
        }
        
        r_factors = []
        for metric, (weight, values) in metrics.items():
            avg = values.mean()
            std = values.std() + 1e-6  # Add small value to avoid division by zero
            
            if metric == 'return':
                latest_value = (latest_candle['close'] - latest_candle['open']) / latest_candle['open']
            else:
                latest_value = latest_candle[metric]
                
            z_score = (latest_value - avg) / std
            r_factors.append(z_score * weight)
            
        r_factor = sum(r_factors)
        r_score = max(0, min(100, 50 + r_factor * 10))
        
        results.append({
            'instrument_token': token,
            'r_score': round(r_score, 2),
            'z_volume': round(r_factors[0]/0.4, 2),  # Actual z-score without weight
            'z_turnover': round(r_factors[1]/0.3, 2),
            'z_return': round(r_factors[2]/0.3, 2),
            'latest_close': latest_candle['close'],
            'latest_volume': latest_candle['volume']
        })
    
    return pd.DataFrame(results)

def add_prev_data(data):
    """
    Aggregate 1-minute data for each day up to the current intraday time (e.g., 12:28).
    Returns DataFrame with daily OHLCV and Symbol data per instrument.
    """
    # Convert and localize datetime
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.tz_localize(None)
    data['time'] = data['date'].dt.time
    data['day'] = data['date'].dt.date

    # Set the intraday cutoff time (now or simulated)
    now = datetime.now()
    current_cutoff_time = now.time()
    
    # for backtesting
    # current_cutoff_time = datetime.strptime("12:28", "%H:%M").time()

    # Filter each day’s data up to the cutoff time
    filtered_data = data[data['time'] <= current_cutoff_time]

    # Aggregate up to the cutoff time for each day & instrument
    agg_data = (
        filtered_data
        .groupby(['instrument_token', 'day'])
        .agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'Symbol': 'first'
        })
        .reset_index()
    )

    # Recreate a pseudo timestamp column (you may use this for sorting/debugging)
    agg_data['date'] = pd.to_datetime(agg_data['day'])
    agg_data = agg_data[['Symbol', 'instrument_token', 'date', 'open', 'high', 'low', 'close', 'volume']]

    return agg_data

def get_data(kite,l):
    stocks = pd.read_csv(r"data\data_stock_fo.csv")
    stocks = pd.DataFrame(l)
    all_rows = []
    
    for _, stock in stocks.iterrows():
        token = int(stock["instrument_token"])
        symbol = stock["symbol"]

        try:
            quote = kite.quote([token])
            q = quote[str(token)]

            row = {
                'Symbol': symbol,
                'instrument_token': token,
                'date': q['last_trade_time'].date(),
                'open': q['ohlc']['open'],
                'high': q['ohlc']['high'],
                'low': q['ohlc']['low'],
                'close': q['ohlc']['close'],
                'last_price': q['last_price'],
                'buy_quantity': q['buy_quantity'],
                'sell_quantity': q['sell_quantity'],
                'oi': q['oi'],                
                'volume': q['volume'],
                'last_trade_time': q['last_trade_time']
            }

            all_rows.append(row)

        except Exception as e:
            print(f"Error fetching data for {symbol} - {e}")

        time.sleep(0.35)
    df = pd.DataFrame(all_rows)
    return df

def get_sector_data(kite, sector_name, json_path, min_days=18):
    
    with open(json_path, "r") as f:
        sector_map = json.load(f)

    if sector_name not in sector_map:
        raise ValueError(f"Sector '{sector_name}' not found in JSON")

    stocks = sector_map[sector_name]
    all_data = []
    
    # First collect all historical data
    historical_data = pd.read_csv(r'data\stock_1.csv')
    historical_data = add_prev_data(historical_data)
    today_data = get_data(kite,stocks)
   #print(today_data)
    today_data['instrument_token'] = today_data['instrument_token'].astype(int)
    # Prepare today's data for R-score calculation (without extra columns)
    today_agg = today_data[['Symbol', 'instrument_token', 'date', 'open', 'high', 'low', 'close', 'volume']].copy()
    combined = pd.concat([historical_data, today_agg], ignore_index=True)
    
    # Calculate R-Scores in one go
    if not combined.empty:
        r_scores = calculate_r_score(combined, min_days)
    
    # Now process each stock
    for stock in stocks:
        token = int(stock["instrument_token"])
        symbol = stock["symbol"]
        #print(type(today_data["instrument_token"].astype(int)),today_data["instrument_token"],type (token),token)
        today_row = today_data[today_data["instrument_token"] == token]
        #print(today_row,"type: ",type(today_row))
        if today_row.empty:
            raise ValueError(f"No data for token: {token} ({symbol})")
        
        try:
            
            # Get R-Score if available
            r_score_data = None
            if not combined.empty:
                r_score_match = r_scores[r_scores['instrument_token'] == token]
                if not r_score_match.empty:
                    r_score_data = r_score_match.iloc[0]
            
            #print(f"{r_score_data}")
            
            # Calculate percentage change
            # try:
            #     pct_change = round(((today_data['last_price'] - today_data['close']) / today_data['close']) * 100, 2)
            # except:
            #     pct_change = 0
            
            # Prepare the output row
            row = {
                "Symbol": symbol,
                "Last Price": today_row['last_price'].iloc[0],
                "Prev Close": today_row['close'].iloc[0],
                "% Change": round(((today_row['last_price'].iloc[0] - today_row['close'].iloc[0]) / today_row['close'].iloc[0]) * 100, 2),
                "Volume": today_row['volume'].iloc[0],
                "OI": today_row['oi'].iloc[0],
                "Buy": today_row['buy_quantity'].iloc[0],
                "Sell": today_row['sell_quantity'].iloc[0],
                "R-Score": r_score_data['r_score'] if r_score_data is not None else None,
                "Z-Volume": r_score_data['z_volume'] if r_score_data is not None else None,
                "Z-Turnover": r_score_data['z_turnover'] if r_score_data is not None else None,
                "Z-Return": r_score_data['z_return'] if r_score_data is not None else None,
                "Last Trade Time": today_row['last_trade_time'].iloc[0]
            }
            all_data.append(row)
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
    
    return pd.DataFrame(all_data)
if __name__ == "__main__":
    kite = gen_ses()
    print("Kite session active.")

    sector = input("Enter the Sector Ex: NIFTY 50, NIFTY FMCG  : ")
    json_path = r"data/sector_data.json"

    df = get_sector_data(kite, sector, json_path)
    
    # Filter and sort results
    if 'R-Score' in df.columns:
        df = df[~df['R-Score'].isna()]  # Remove stocks with no R-Score
        df = df.sort_values('R-Score', ascending=False)
        filtered_df = df
        # Apply additional filters like in the first code
        '''
        filtered_df = df[
            (df['Z-Volume'] > 1) &
            (df['Z-Turnover'] > 1) &
            (df['Z-Return'] > 0.6)
        ]'''
    
        print("\nTop Ranked Stocks:")
        print(filtered_df.head(20))
