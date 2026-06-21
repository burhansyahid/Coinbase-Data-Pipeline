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
BUCKET_NAME = "coinbase-bronze"

def upload_to_oci(filepath):
    """Uploads a completed JSONL file to the Oracle Cloud bucket."""
    filename = os.path.basename(filepath)
    try:
        with open(filepath, "rb") as f:
            object_storage.put_object(
                namespace_name=NAMESPACE, bucket_name=BUCKET_NAME,
                object_name=filename, put_object_body=f
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
    """Parses live payloads, updates metrics, and commits to disk safely without risking stream crashes."""
    try:
        if "events" in data:
            for event in data["events"]:
                for ticker in event.get("tickers", []):
                    try:
                        TRADES_PROCESSED.inc()
                        if 'price' in ticker:
                            CURRENT_BTC_PRICE.set(float(ticker['price']))
                    except (ValueError, TypeError, KeyError) as row_err:
                        print(f"Metrics Error: Skipping malformed ticker row: {row_err}", file=sys.stderr)
    except Exception as tree_err:
        print(f"Payload Error: Mismatched structural schema tree: {tree_err}", file=sys.stderr)
   
    # Guarantee local raw persistence regardless of metrics extraction health
    file_handle.write(json.dumps(data) + "\n")
    file_handle.flush()

async def stream_market_data(websocket, subscribe_message):
    """Manages active streaming payloads and file rotations over an open socket session."""
    await websocket.send(json.dumps(subscribe_message))
    print("Producer: Subscribed to BTC-USD.")

    current_filename = get_current_filename()
    file_handle = open(current_filename, "a")

    try:
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            
            new_filename = get_current_filename()
            if new_filename != current_filename:
                file_handle.close()
                upload_to_oci(current_filename)
                current_filename = new_filename
                file_handle = open(current_filename, "a")

            process_metrics_and_write(data, file_handle)
    finally:
        file_handle.close()

async def subscribe_to_coinbase():
    """Manages connection context states and triggers connection backoffs on failure."""
    url = "wss://advanced-trade-ws.coinbase.com"
    subscribe_message = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "ticker"}

    start_http_server(8000)
    print("Producer: Prometheus Metrics server active on port 8000")

    while True:
        try:
            print("Producer: Launching connection attempt to Coinbase API...")
            async with websockets.connect(url) as websocket:
                await stream_market_data(websocket, subscribe_message)
        except (websockets.exceptions.ConnectionClosed, Exception) as e:
            print(f"Network Connection Disrupted: {e}. Re-establishing link in 5s...", file=sys.stderr)
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(subscribe_to_coinbase())
    except KeyboardInterrupt:
        print("System: Shutting down ingestion engine.")
