import pandas as pd
import oracledb
import glob
import os
import sys
import shutil

# --- SECURE CONFIGURATION VIA ENVIRONMENT LAYERS ---
DB_USER = os.environ.get("DB_USER", "ADMIN")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
WALLET_PASSWORD = os.environ.get("WALLET_PASSWORD")
DB_DSN = "coinbasegold_high"
WALLET_DIR = "./wallet"
SILVER_ARCHIVE_DIR = "data/_silver_archive"

def get_oldest_clean_file():
    """Finds clean tracking states sequentially to guarantee pipeline alignment."""
    clean_files = glob.glob("data/clean_trades_*.csv")
    if not clean_files:
        return None
    clean_files.sort()
    return clean_files[0]

def execute_idempotent_merge(cursor, dataframe):
    """Executes atomic UPSERT logic while filtering out poisoned rows without rolling back stable states."""
    dataframe['trade_timestamp'] = pd.to_datetime(dataframe['trade_timestamp'])
    records = dataframe.to_dict(orient='records')
    
    for record in records:
        if hasattr(record['trade_timestamp'], 'to_pydatetime'):
            record['trade_timestamp'] = record['trade_timestamp'].to_pydatetime()

    merge_sql = """
    MERGE INTO fact_crypto_trades tgt
    USING (
        SELECT 
            :product_id AS product_id, :trade_timestamp AS trade_timestamp, :price AS price,
            :volume_24_h AS volume_24_h, :high_24_h AS high_24_h, :low_24_h AS low_24_h,
            :total_24h_usd_volume AS total_24h_usd_volume
        FROM dual
    ) src
    ON (tgt.product_id = src.product_id AND tgt.trade_timestamp = src.trade_timestamp AND tgt.price = src.price)
    WHEN NOT MATCHED THEN
    INSERT (product_id, trade_timestamp, price, volume_24_h, high_24_h, low_24_h, total_24h_usd_volume)
    VALUES (src.product_id, src.trade_timestamp, src.price, src.volume_24_h, src.high_24_h, src.low_24_h, src.total_24h_usd_volume)
    """

    print(f"Gold Loader: Merging {len(records)} transaction records into Oracle ADW natively...")
    cursor.executemany(merge_sql, records, batcherrors=True)
    
    # Isolate row failures without executing an unneeded full rollback path
    errors = cursor.getbatcherrors()
    if errors:
        for error in errors:
            print(f"Row Level DB Error at array offsets [{error.offset}]: {error.message}", file=sys.stderr)

def archive_silver_file(filepath):
    """Archives transformed tracking files out of active processing pipeline."""
    os.makedirs(SILVER_ARCHIVE_DIR, exist_ok=True)
    destination = os.path.join(SILVER_ARCHIVE_DIR, os.path.basename(filepath))
    shutil.move(filepath, destination)
    print(f"Archive Manager: Moved transformed state {filepath} to safe storage.")

def process_gold_layer():
    """Main operational coordinator for idempotent gold loading."""
    target_file = get_oldest_clean_file()
    if not target_file:
        print("System: No clean analytical layers found to upload.")
        return

    print(f"Gold Worker: Opening target batch {target_file}...")
    df = pd.read_csv(target_file)
    
    try:
        connection = oracledb.connect(
            user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN,
            config_dir=WALLET_DIR, wallet_location=WALLET_DIR, wallet_password=WALLET_PASSWORD
        )
        cursor = connection.cursor()
        cursor.execute("ALTER SESSION DISABLE PARALLEL DML")
        
        execute_idempotent_merge(cursor, df)
        connection.commit()
        print("Gold Worker: Database state synchronization completed successfully.")
        archive_silver_file(target_file)
    except Exception as e:
        print(f"Fatal Database Loading Exception: {e}", file=sys.stderr)
        if 'connection' in locals():
            connection.rollback()
        sys.exit(1)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'connection' in locals(): connection.close()

if __name__ == "__main__":
    process_gold_layer()
