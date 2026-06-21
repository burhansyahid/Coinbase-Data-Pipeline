#!/bin/bash

# Navigate to the project root directory
cd /home/opc/coinbase-pipeline

# Activate the virtual environment so Python can find Pandas and Oracledb
source venv/bin/activate

echo "=== Pipeline Execution Started: $(date) ==="

# 1. Run the Silver Layer Transformation from the src directory
echo "Running Silver Layer Transformation..."
python src/transform.py

# 2. Check if the previous script succeeded
if [ $? -eq 0 ]; then
    echo "Silver Layer Complete. Running Gold Layer Loader..."
    # 3. Run the Gold Layer Database Loader from the src directory
    python src/load.py
else
    echo "ERROR: Silver Layer transformation failed. Aborting database load."
    exit 1
fi

echo "=== Pipeline Execution Finished: $(date) ==="
