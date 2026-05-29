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
