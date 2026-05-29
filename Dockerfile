# 1. Base Image: Use an official, lightweight Python Linux environment
# The "slim" variant removes unnecessary OS tools to keep the container small and secure.
FROM python:3.9-slim

# 2. Working Directory: Create a folder inside the container to hold our code
WORKDIR /app

# 3. Dependencies First: Copy ONLY the requirements file first.
# This is a Docker caching trick! If we change our Python code later, 
# Docker won't have to re-download the pip packages every single time.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy Code: Now copy our actual Python script into the container
COPY ingest.py .

# 5. Environment Variables: Force Python to print logs immediately to the console 
# without buffering them. This is critical for cloud monitoring (Grafana).
ENV PYTHONUNBUFFERED=1

# 6. Execution: The exact command the container runs when it turns on
CMD ["python", "ingest.py"]
