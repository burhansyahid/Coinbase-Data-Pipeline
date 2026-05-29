#!/bin/bash

# Navigate to the project directory
cd /home/opc/coinbase-pipeline

# Activate the virtual environment so Python can find Pandas and Oracledb
source venv/bin/activate

echo "=== Pipeline Execution Started: $(date) ==="

# 1. Run the Silver Layer Transformation
echo "Running Silver Layer Transformation..."
python transform.py

# 2. Check if the previous script succeeded ($? checks the exit code of the last command)
if [ $? -eq 0 ]; then
    echo "Silver Layer Complete. Running Gold Layer Loader..."
    # 3. Run the Gold Layer Database Loader
    python load.py
else
    echo "ERROR: Silver Layer transformation failed. Aborting database load."
    exit 1
fi

echo "=== Pipeline Execution Finished: $(date) ==="
