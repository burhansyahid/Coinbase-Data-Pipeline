import asyncio
import websockets
import json
import oci
import os
import sys
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge

# --- 1. PROMETHEUS METRICS SETUP ---
TRADES_PROCESSED = Counter('coinbase_trades_total', 'Total number of trades processed')
CURRENT_BTC_PRICE = Gauge('coinbase_btc_price', 'Live price of BTC in USD')

# --- 2. OCI CLOUD CONFIGURATION ---
signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

NAMESPACE = "axhrcbgwo7cd"
BUCKET_NAME = "coinbase-bronze" [cite: 37]

def upload_to_oci(filepath):
    """Uploads a completed JSONL file to the Oracle Cloud bucket."""
    filename = os.path.basename(filepath)
    try:
        with open(filepath, "rb") as f:
            object_storage.put_object(
                namespace_name=NAMESPACE, bucket_name=BUCKET_NAME,
                object_name=filename, put_object_body=f [cite: 39]
            )
        print(f"System: Successfully uploaded {filename} to OCI.")
    except Exception as e:
        print(f"System Error: Failed to upload to OCI: {e}", file=sys.stderr)

def get_current_filename():
    """Generates a filename inside the shared volume path based on the current hour (UTC)."""
    now = datetime.now(timezone.utc)
    os.makedirs("data", exist_ok=True)
    return f"data/raw_trades_{now.strftime('%Y%m%d_%H')}.jsonl"

def process_metrics_and_write(data, file_handle):
    """Parses live payloads, updates metrics, and commits to disk safely."""
    if "events" in data: [cite: 44]
        for event in data["events"]:
            if "tickers" in event: [cite: 45]
                for ticker in event["tickers"]:
                    TRADES_PROCESSED.inc() [cite: 46]
                    CURRENT_BTC_PRICE.set(float(ticker['price'])) [cite: 46]
                    
    # Atomic write to shared block volume storage
    file_handle.write(json.dumps(data) + "\n") [cite: 47]
    file_handle.flush()

async def stream_market_data(websocket, subscribe_message):
    """Manages active streaming payloads and file rotations over an open socket session."""
    await websocket.send(json.dumps(subscribe_message)) [cite: 41]
    print("Producer: Subscribed to BTC-USD.")

    current_filename = get_current_filename() [cite: 41]
    file_handle = open(current_filename, "a")

    try:
        while True:
            response = await websocket.recv() [cite: 41]
            data = json.loads(response)
            
            # Smart File Rotation
            new_filename = get_current_filename() [cite: 42]
            if new_filename != current_filename: [cite: 42]
                file_handle.close() [cite: 42]
                upload_to_oci(current_filename) [cite: 43]
                current_filename = new_filename [cite: 44]
                file_handle = open(current_filename, "a") [cite: 44]

            process_metrics_and_write(data, file_handle)
    finally:
        file_handle.close()

async def subscribe_to_coinbase():
    """Manages connection context states and triggers connection backoffs on failure."""
    url = "wss://advanced-trade-ws.coinbase.com" [cite: 40]
    subscribe_message = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"} [cite: 40]

    start_http_server(8000) [cite: 41]
    print("Producer: Prometheus Metrics server active on port 8000")

    # Outer loop handles continuous service re-entry upon failure
    while True:
        try:
            print("Producer: Launching connection attempt to Coinbase API...")
            async with websockets.connect(url) as websocket:
                await stream_market_data(websocket, subscribe_message)
        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            print(f"Network Connection Disrupted: {e}. Re-establishing link in 5s...", file=sys.stderr)
            await asyncio.sleep(5) [cite: 48]

if __name__ == "__main__":
    try:
        asyncio.run(subscribe_to_coinbase())
    except KeyboardInterrupt:
        print("System: Shutting down ingestion engine.")
