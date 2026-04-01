# Fire Data Pipeline Project

## Project Overview

This project is a data engineering pipeline designed to:

1. Fetch fire data from the NASA API
2. Stream the data
3. Store it
4. Transform it
5. Build dashboards and alerts

---

## Project Structure

```
project/
│── ingestion/                # Scripts for fetching data
│── .env                      # Secret keys (not shared)
│── requirements.txt          # Project dependencies
│── docker-compose.yml        # Infrastructure services
```

### ingestion/

Contains scripts responsible for fetching and preparing data.

### .env

Stores sensitive information such as API keys.
This file must not be pushed to Git.

### requirements.txt

Contains required Python libraries:

* `requests`: makes HTTP calls to the NASA API
* `pandas`: processes data as DataFrames
* `python-dotenv`: loads environment variables from `.env`

---

## Pipeline Steps

## 1. Data Ingestion (NASA API)

Script: `nasa_firms_client.py`

Purpose:

* Validate API key
* Fetch fire data
* Convert data into a structured table (DataFrame)

---

## 2. Data Streaming with Kafka

### Why Kafka

The pipeline objective is to:

1. Fetch data
2. Store data
3. Transform data
4. Visualize data

Initially, data was only fetched without further processing. Kafka introduces a streaming layer to handle data flow efficiently.

### Data Flow

```
NASA API → Producer → Kafka Topic → Consumer
```

### Concept

Kafka acts as a message broker:

* The producer sends fire data
* The topic stores the data
* The consumer reads data independently

### Advantages

* Decouples producer and consumer
* Prevents data loss
* Handles failures reliably

### Requirement

Kafka requires Zookeeper to manage its operations.

---

### Producer vs Consumer

| Producer                             | Consumer                           |
| ------------------------------------ | ---------------------------------- |
| Fetches NASA data                    | Listens to Kafka topic             |
| Converts rows to dictionaries        | Receives messages                  |
| Serializes data to bytes             | Deserializes bytes to dictionaries |
| Sends to topic (`fires_raw`)         | Reads from topic                   |
| Runs periodically (e.g., every hour) | Runs continuously                  |

They operate independently.

---

## 3. Data Storage with MinIO

### Problem

Without storage:

```
Kafka → Consumer → print() → Data lost
```

### Solution: MinIO

MinIO is an object storage system similar to Amazon S3.

### With MinIO

```
Kafka → Consumer → MinIO → Data stored
```

### Why MinIO

* Scalable
* Cloud-compatible (S3 API)
* Accessible by multiple services

### Implementation

* Create `minio_client.py`
* Reuse it across the project (DRY principle)
* Update the consumer to store data instead of printing

---

## 4. Workflow Orchestration with Airflow

### Problem

Manual execution is unreliable:

```
Run scripts manually → forgotten execution → pipeline failure
```

### Solution: Airflow

Airflow automates:

* Scheduling
* Execution order
* Failure handling

### Key Concepts

| Concept    | Description         | Analogy           |
| ---------- | ------------------- | ----------------- |
| DAG        | Pipeline definition | Recipe            |
| Task       | Single step         | Step in a process |
| Operator   | Type of task        | Execution method  |
| Schedule   | Execution timing    | Fixed schedule    |
| Dependency | Task order          | Step sequencing   |

---

### Airflow Structure

```
dags/
│── firms_ingestion_dag.py   # Hourly ingestion
│── enrichment_dag.py        # Daily enrichment
│── reporting_dag.py         # Aggregation and alerts
```

---

## Final Architecture

```
NASA API
   ↓
Producer (Python)
   ↓
Kafka (Topic)
   ↓
Consumer
   ↓
MinIO (Storage)
   ↓
Airflow (Orchestration)
   ↓
Analytics / Dashboard
```

---

## Important Notes

* `.env` must be in `.gitignore`
* `requirements.txt` must be tracked
* `docker-compose.yml` must be tracked

---

## Future Improvements

* Add transformation layer
* Build dashboards (e.g., Streamlit)
* Add alerting system
* Deploy to cloud
