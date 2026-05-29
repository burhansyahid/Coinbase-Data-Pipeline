import pandas as pd
import oracledb
import glob
import os

# --- CONFIGURATION ---
DB_USER = "ADMIN"
DB_PASSWORD = "Preman901202!"    # <-- Update this
WALLET_PASSWORD = "Preman901202!"  # <-- Update this (the password you made when downloading the zip)
DB_DSN = "coinbasegold_high" 
WALLET_DIR = "./wallet"

def process_gold_layer():
    # 1. Find the newest cleaned CSV file
    clean_files = glob.glob("clean_trades_*.csv")
    if not clean_files:
        print("System: No cleaned Silver Layer files found.")
        return
    
    clean_files.sort(reverse=True)
    target_file = clean_files[0]
    print(f"Gold Worker: Found clean data. Preparing to load {target_file}...")

    df = pd.read_csv(target_file)
    df['trade_timestamp'] = pd.to_datetime(df['trade_timestamp'])
    
    records_to_insert = [tuple(x) for x in df.to_numpy()]

    print("Gold Worker: Connecting to Oracle Autonomous Data Warehouse via mTLS...")
    
    # 2. Establish the Secure Connection
    try:
        # THE FIX: Added config_dir and wallet_password parameters
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            config_dir=WALLET_DIR,
            wallet_location=WALLET_DIR,
            wallet_password=WALLET_PASSWORD
        )
        cursor = connection.cursor()
        print("Gold Worker: Connection Successful!")

        # 3. Create the Database Table (if it doesn't exist)
        create_table_sql = """
        CREATE TABLE fact_crypto_trades (
            product_id VARCHAR2(20),
            trade_timestamp TIMESTAMP WITH TIME ZONE,
            price NUMBER(18, 4),
            volume_24_h NUMBER(18, 4),
            high_24_h NUMBER(18, 4),
            low_24_h NUMBER(18, 4),
            total_24h_usd_volume NUMBER(18, 4)
        )
        """
        try:
            cursor.execute(create_table_sql)
            print("Gold Worker: Created new table 'fact_crypto_trades'.")
        except oracledb.DatabaseError as e:
            error, = e.args
            if error.code != 955:
                raise

        # 4. Perform a High-Speed Batch Insert
        insert_sql = """
        INSERT INTO fact_crypto_trades 
        (product_id, trade_timestamp, price, volume_24_h, high_24_h, low_24_h, total_24h_usd_volume) 
        VALUES (:1, :2, :3, :4, :5, :6, :7)
        """
        
        print(f"Gold Worker: Inserting {len(records_to_insert)} rows...")
        cursor.executemany(insert_sql, records_to_insert)
        
        connection.commit()
        print("Gold Worker: Data committed successfully!")

        # 5. Run an Analytics Query to prove it works
        cursor.execute("SELECT COUNT(*) FROM fact_crypto_trades")
        count = cursor.fetchone()[0]
        print("-" * 50)
        print(f"SUCCESS: The database now contains {count} total rows.")
        print("-" * 50)

    except Exception as e:
        print(f"Gold Worker Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    process_gold_layer()
