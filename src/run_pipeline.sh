#!/bin/bash

# Navigate to the project root directory
cd /home/opc/coinbase-pipeline

# Securely inject local environment state into execution context
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "CRITICAL ERROR: .env resource file missing. Aborting execution pipeline." >&2
    exit 1
fi

# Activate the virtual environment
source venv/bin/activate

echo "=== Pipeline Execution Started: $(date) ==="

echo "Running Silver Layer Transformation..."
python src/transform.py

if [ $? -eq 0 ]; then
    echo "Silver Layer Complete. Running Gold Layer Loader..."
    python src/load.py
else
    echo "ERROR: Silver Layer transformation failed. Aborting database load." >&2
    exit 1
fi

echo "=== Pipeline Execution Finished: $(date) ==="
