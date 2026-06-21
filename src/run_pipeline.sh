#!/bin/bash

# 1. Force the script to drop into your absolute project root folder
cd /home/opc/coinbase-pipeline

# 2. Explicitly load the environment configuration file
if [ -f .env ]; then
    export $(cat .env | xargs)
fi

# 3. Activate the local Python virtual environment
source venv/bin/activate

echo "=== Pipeline Execution Started: $(date) ==="
echo "Running Silver Layer Transformation..."
python3 src/transform.py

echo "Running Gold Layer Loader..."
python3 src/load.py
echo "=== Pipeline Execution Finished: $(date) ==="
