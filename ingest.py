import asyncio
import websockets
import json
import oci
import os
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge

# --- 1. PROMETHEUS METRICS SETUP ---
TRADES_PROCESSED = Counter('coinbase_trades_total', 'Total number of trades processed')
CURRENT_BTC_PRICE = Gauge('coinbase_btc_price', 'Live price of BTC in USD')

# --- 2. OCI CLOUD CONFIGURATION ---
# Authenticate securely using the server's IAM Instance Principals
signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

# Your specific OCI Namespace (from your earlier success logs)
NAMESPACE = "axhrcbgwo7cd"
# VERIFY THIS: Ensure this matches the name of your OCI Object Storage bucket
BUCKET_NAME = "coinbase-bronze" 

print(f"System: Successfully authenticated using Instance Principals. Namespace: {NAMESPACE}")

def upload_to_oci(filepath):
    """Uploads a completed JSONL file to the Oracle Cloud bucket."""
    filename = os.path.basename(filepath)
    print(f"System: Uploading {filename} to OCI Object Storage...")
    try:
        with open(filepath, "rb") as f:
            object_storage.put_object(
                namespace_name=NAMESPACE,
                bucket_name=BUCKET_NAME,
                object_name=filename,
                put_object_body=f
            )
        print(f"System: Successfully uploaded {filename}")
    except Exception as e:
        print(f"System Error: Failed to upload to OCI: {e}")

def get_current_filename():
    """Generates a filename based on the current hour (UTC)."""
    now = datetime.now(timezone.utc)
    return f"raw_trades_{now.strftime('%Y%m%d_%H')}.jsonl"

async def subscribe_to_coinbase():
    url = "wss://advanced-trade-ws.coinbase.com"
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channel": "ticker"
    }

    # --- 3. START PROMETHEUS SERVER ---
    start_http_server(8000)
    print("Producer: Prometheus Metrics server started on port 8000")

    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        print("Producer: Subscribed to BTC-USD.")

        current_filename = get_current_filename()
        file_handle = open(current_filename, "a")

        while True:
            try:
                response = await websocket.recv()
                data = json.loads(response)
                
                # --- 4. FILE ROTATION LOGIC ---
                new_filename = get_current_filename()
                if new_filename != current_filename:
                    file_handle.close()
                    
                    # Upload the old file to OCI now that the hour is over
                    upload_to_oci(current_filename)
                    
                    # Open the new file for the new hour
                    current_filename = new_filename
                    file_handle = open(current_filename, "a")

                # --- 5. METRICS & DATA EXTRACTION ---
                if "events" in data:
                    for event in data["events"]:
                        if "tickers" in event:
                            for ticker in event["tickers"]:
                                # Update Prometheus live metrics
                                TRADES_PROCESSED.inc() 
                                CURRENT_BTC_PRICE.set(float(ticker['price'])) 
                                
                # Write the raw JSON string to the file
                file_handle.write(json.dumps(data) + "\n")
                file_handle.flush()
                
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(subscribe_to_coinbase())
    except KeyboardInterrupt:
        print("System: Shutting down ingestion engine.")
