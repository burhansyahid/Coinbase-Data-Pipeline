import asyncio
import websockets
import json
import time
import os
import oci

COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
SUBSCRIBE_MESSAGE = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "ticker"
}

# OCI Storage Configurations
BUCKET_NAME = "coinbase-stream-bronze"
# This retrieves your default OCI Namespace dynamically
try:
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    object_storage_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    NAMESPACE = object_storage_client.get_namespace().data
    print(f"System: Successfully authenticated using Instance Principals. Namespace: {NAMESPACE}")
except Exception as e:
    print(f"System Warning: Cloud auth failed. Running in local-only mode. Error: {e}")
    object_storage_client = None

async def upload_to_oci(local_filepath):
    """Helper function to upload a completed file to OCI Object Storage."""
    if not object_storage_client:
        print(f"Upload Skipped: Cloud client not initialized for {local_filepath}")
        return

    try:
        filename = os.path.basename(local_filepath)
        print(f"Cloud Worker: Starting upload for {filename}...")
        
        with open(local_filepath, "rb") as f:
            object_storage_client.put_object(
                namespace_name=NAMESPACE,
                bucket_name=BUCKET_NAME,
                object_name=f"landing/{filename}",
                put_object_body=f
            )
        print(f"Cloud Worker: Successfully uploaded {filename} to OCI Bucket!")
        
        # Optional: Delete the local file to save server space after successful upload
        # os.remove(local_filepath)
    except Exception as e:
        print(f"Cloud Worker Error: Failed to upload {local_filepath}. Reason: {e}")

async def producer(queue):
    """Task 1: Listen to WebSocket and push items to the queue immediately."""
    async with websockets.connect(COINBASE_WS_URL) as ws:
        await ws.send(json.dumps(SUBSCRIBE_MESSAGE))
        print("Producer: Subscribed to BTC-USD.")

        trade_count = 0
        while True:
            message = await ws.recv()
            data = json.loads(message)
            if data.get("channel") == "ticker":
                await queue.put(data)
                trade_count += 1
                if trade_count % 10 == 0:
                    print(".", end="", flush=True)

async def consumer(queue):
    """Task 2: Periodically flush queue. If the hour changes, trigger a cloud upload."""
    # Track the current active tracking hour string (e.g., "20260529_00")
    active_hour = time.strftime("%Y%m%d_%H")
    
    while True:
        await asyncio.sleep(5)
        
        batch = []
        while not queue.empty():
            trade = await queue.get()
            batch.append(trade)
            queue.task_done()
        
        if batch:
            system_hour = time.strftime("%Y%m%d_%H")
            hourly_output_file = f"raw_trades_{system_hour}.jsonl"
            
            # If the hour changed since our last loop, the previous hour's file is complete!
            if system_hour != active_hour:
                previous_file = f"raw_trades_{active_hour}.jsonl"
                print(f"\nHour changed from {active_hour} to {system_hour}. Rotating file.")
                
                # Fire off the upload task in the background so it doesn't freeze ingestion
                if os.path.exists(previous_file):
                    asyncio.create_task(upload_to_oci(previous_file))
                
                # Reset tracking to the new current hour
                active_hour = system_hour
            
            with open(hourly_output_file, "a", encoding="utf-8") as f:
                for trade in batch:
                    f.write(json.dumps(trade) + "\n")

async def main():
    queue = asyncio.Queue()
    await asyncio.gather(producer(queue), consumer(queue))

if __name__ == "__main__":
    asyncio.run(main())
