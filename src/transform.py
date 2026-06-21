import pandas as pd
import json
import glob
import os
import sys
import shutil

DATA_DIR = "data"
ARCHIVE_DIR = "data/_archive"

def get_oldest_raw_file():
    """Finds and sorts raw trade files chronologically to process data sequentially."""
    raw_files = glob.glob(os.path.join(DATA_DIR, "raw_trades_*.jsonl"))
    if not raw_files:
        return None
    # Sorting ensures we don't process files arbitrarily out of time order
    raw_files.sort()
    return raw_files[0]

def parse_json_lines(filepath):
    """Safely extracts trade objects line by line, preventing whole-file crash on corruption."""
    records = []
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                records.extend(extract_tickers(data))
            except json.JSONDecodeError as e:
                # Blast radius isolation: log the corruption to stderr and advance
                print(f"Schema Violation: Malformed JSON at line {line_num} in {filepath}: {e}", file=sys.stderr)
                continue
    return records

def extract_tickers(data):
    """Navigates nested Coinbase JSON trees down to relational ticker payloads."""
    tickers = []
    if "events" in data:
        for event in data["events"]:
            if "tickers" in event:
                for ticker in event["tickers"]:
                    ticker['time'] = data.get('timestamp')
                    tickers.append(ticker)
    return tickers

def clean_and_coerce(records):
    """Enforces rigorous structural schemas and types on extracted data lists."""
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame()

    keep_cols = ['product_id', 'time', 'price', 'volume_24_h', 'high_24_h', 'low_24_h']
    df = df[[c for c in keep_cols if c in df.columns]].copy()
    
    # Coerce to numerics safely; invalid entries become NaN instead of crashing processing
    numeric_cols = ['price', 'volume_24_h', 'high_24_h', 'low_24_h']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    # Drop rows missing crucial analytical dimensions
    df.dropna(subset=['product_id', 'time', 'price'], inplace=True)
    
    df['total_24h_usd_volume'] = df['price'] * df['volume_24_h']
    df.rename(columns={'time': 'trade_timestamp'}, inplace=True)
    return df

def archive_processed_file(filepath):
    """Moves processed raw files out of active runtime path to avoid processing loops."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    destination = os.path.join(ARCHIVE_DIR, os.path.basename(filepath))
    shutil.move(filepath, destination)
    print(f"Archive Manager: Moved {filepath} safely to storage repository.")

def process_silver_layer():
    """Main operational coordinator for silver layer schema processing."""
    try:
        target_file = get_oldest_raw_file()
        if not target_file:
            print("System: No fresh raw market data found to process.")
            return

        print(f"Silver Worker: Transforming active batch {target_file}...")
        raw_records = parse_json_lines(target_file)
        transformed_df = clean_and_coerce(raw_records)

        if not transformed_df.empty:
            clean_filename = target_file.replace("raw_", "clean_").replace(".jsonl", ".csv")
            transformed_df.to_csv(clean_filename, index=False)
            print(f"Silver Worker: Successfully generated tabular state: {clean_filename}")
        
        archive_processed_file(target_file)
    except Exception as e:
        print(f"Fatal Silver Engine Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    process_silver_layer()
