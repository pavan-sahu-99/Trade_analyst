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
            'volume': (0.4, past_candles['volume']),
            'turnover': (0.3, past_candles['turnover']),
            'return': (0.3, (past_candles['close'] - past_candles['open']) / past_candles['open'])
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

def get_sector_data(kite, sector_name, json_path, min_days=18):
    """Enhanced sector data with better R-Score calculation"""
    with open(json_path, "r") as f:
        sector_map = json.load(f)

    if sector_name not in sector_map:
        raise ValueError(f"Sector '{sector_name}' not found in JSON")

    stocks = sector_map[sector_name]
    all_data = []
    
    # First collect all historical data
    historical_data = pd.read_csv(r'data\stock_1d.csv')
    # Calculate R-Scores in one go
    if not historical_data.empty:
        r_scores = calculate_r_score(historical_data, min_days)
    
    # Now get live data and combine with R-Scores
    for stock in stocks:
        token = stock["instrument_token"]
        symbol = stock["symbol"]
        try:
            # Get live quote
            quote = kite.quote(token)
            q = quote[str(token)]
            
            # Get R-Score if available
            r_score_data = None
            if not historical_data.empty:
                r_score_data = r_scores[r_scores['instrument_token'] == token].iloc[0] if not r_scores[r_scores['instrument_token'] == token].empty else None
            
            all_data.append({
                "Symbol": symbol,
                "Last Price": q["last_price"],
                "Prev Close": q["ohlc"]["close"],
                "% Change": round(((q["last_price"] - q["ohlc"]["close"]) / q["ohlc"]["close"]) * 100, 2),
                "Volume": q["volume"],
                "OI": q['oi'],
                "R-Score": r_score_data['r_score'] if r_score_data is not None else None,
                "Z-Volume": r_score_data['z_volume'] if r_score_data is not None else None,
                "Z-Turnover": r_score_data['z_turnover'] if r_score_data is not None else None,
                "Z-Return": r_score_data['z_return'] if r_score_data is not None else None,
                "R-Signal": ("Strong Buy" if r_score_data['r_score'] >= 80 else
                            "Buy" if r_score_data['r_score'] >= 65 else
                            "Neutral" if r_score_data['r_score'] >= 35 else
                            "Sell" if r_score_data['r_score'] >= 20 else
                            "Strong Sell") if r_score_data is not None else "Insufficient Data",
                "Last Trade Time": q["last_trade_time"]
            })
            time.sleep(0.2)  # Respect rate limit
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    
    return pd.DataFrame(all_data)

if __name__ == "__main__":
    kite = gen_ses()
    print("Kite session active.")

    sector = input("Enter the Sector Ex: NIFTY 50, NIFTY FMCG: ")
    json_path = r"kite\data\sector_data.json"

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
            (df['Z-Return'] > 0.8)
        ]'''
    
    print("\nTop Ranked Stocks:")
    print(filtered_df.head(20) if 'filtered_df' in locals() else df.head())