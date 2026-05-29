import pandas as pd
import json
import glob
import os

def process_silver_layer():
    # Find all raw_trades files in the directory
    raw_files = glob.glob("raw_trades_*.jsonl")
    
    if not raw_files:
        print("System: No raw data files found to process.")
        return

    # Grab the first file for transformation
    target_file = raw_files[0]
    print(f"Silver Worker: Found raw data. Processing {target_file}...")

    # 1. EXTRACT & FLATTEN: Traverse the nested JSON tree
    records = []
    with open(target_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            
            # Dig into the nested arrays to find the actual market data
            if "events" in data:
                for event in data["events"]:
                    if "tickers" in event:
                        for ticker in event["tickers"]:
                            # Bring the top-level timestamp down into the flat record
                            ticker['time'] = data.get('timestamp')
                            records.append(ticker)
    
    # Load the FLATTENED records into Pandas
    df = pd.DataFrame(records)

    if df.empty:
        print("Silver Worker Error: No valid ticker data found in the file.")
        return

    # Drop all the unnecessary network routing columns we don't need
    keep_cols = ['product_id', 'time', 'price', 'volume_24_h', 'high_24_h', 'low_24_h']
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    # 2. TRANSFORM: Enforce strict data types (Strings -> Numerics)
    df['price'] = pd.to_numeric(df['price'])
    df['volume_24_h'] = pd.to_numeric(df['volume_24_h'])
    df['high_24_h'] = pd.to_numeric(df['high_24_h'])
    df['low_24_h'] = pd.to_numeric(df['low_24_h'])
    
    # Convert the ISO-8601 string into a proper Database Timestamp object
    df['time'] = pd.to_datetime(df['time'])

    # Feature Engineering: Derive the Total 24-Hour USD Volume
    df['total_24h_usd_volume'] = df['price'] * df['volume_24_h']

    # Standardize column names to match our future Database Schema
    df.rename(columns={'time': 'trade_timestamp'}, inplace=True)

    # 3. LOAD: Save locally as a cleaned CSV (The Silver Layer)
    clean_filename = target_file.replace("raw_", "clean_").replace(".jsonl", ".csv")
    df.to_csv(clean_filename, index=False)

    print(f"Silver Worker: Successfully transformed {len(df)} records.")
    print("-" * 75)
    # Print a preview of the clean, tabular data
    print(df[['trade_timestamp', 'price', 'volume_24_h', 'total_24h_usd_volume']].head())
    print("-" * 75)
    print(f"Saved analytics-ready file to: {clean_filename}")

if __name__ == "__main__":
    process_silver_layer()
