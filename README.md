# 📈 Real-Time Cryptocurrency Data Pipeline & SRE Observability Stack

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg?logo=docker)
![Oracle Cloud](https://img.shields.io/badge/OCI-Autonomous_Data_Warehouse-F80000.svg?logo=oracle)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C.svg?logo=prometheus)
![Grafana](https://img.shields.io/badge/Grafana-Observability-F46800.svg?logo=grafana)

## 📌 Project Overview
An enterprise-grade, end-to-end Data Engineering and DevOps pipeline that ingests high-velocity live market data from the Coinbase WebSocket API. The system enforces strict Medallion Architecture principles (Bronze, Silver, Gold layers), operates fully autonomously via Docker and Cron, and provides real-time system monitoring using an industry-standard Prometheus and Grafana stack.

**Business Value:** Demonstrates the ability to handle continuous streaming data without loss, securely transform unstructured nested payloads, and maintain zero-downtime infrastructure.

---

## 📊 Live System Proof

Here is the pipeline running successfully in production, maintaining a zero-downtime streaming state and broadcasting live infrastructure telemetry:

### 1. Real-Time Grafana Telemetry Dashboard
*This live line-graph visualizes real-time market volatility and transaction ingestion throughput directly from the Coinbase WebSocket engine:*

<img width="961" height="505" alt="Screenshot 2026-05-29 095111" src="https://github.com/user-attachments/assets/34c9f7ab-c949-46b0-9f37-991e4ccbe93e" />


### 2. Containerized Microservice Topology (`docker ps`)
*The entire pipeline running in isolated, self-healing Docker containers on an Oracle Cloud compute instance:*

<img width="1103" height="121" alt="image" src="https://github.com/user-attachments/assets/98be3171-d263-4d71-9f36-5e26519d4143" />


---
## 🏗️ Architecture & Data Flow

```mermaid
graph TD
    A[Coinbase WebSocket API] -->|Real-Time JSON| B(Python Ingestion Engine)
    B -->|Hourly Rotation| C[(OCI Object Storage)]
    
    subgraph DevOps Containerization
    B
    end
    
    C -->|Bronze: Raw JSONL| D(Pandas Transformation Layer)
    D -->|Silver: Flattened CSV| E(Gold Layer Loader)
    E -->|Batch Insert via mTLS| F[(Oracle Autonomous Data Warehouse)]
    
    subgraph SRE Observability
    B -.->|Port 8000| G(Prometheus)
    G -.-> H(Grafana Dashboard)
    end
