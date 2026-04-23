# Real-Time IoT Pipeline

End-to-end streaming pipeline using real sensor data from Sensor.Community.

## Architecture

```
Sensor.Community API (Real IoT Data)
        ↓
Kafka Producer (Python)
        ↓
Kafka Topics: sensor_raw, sensor_alerts
        ↓
Python Stream Processor
  - Windowed Aggregations
  - Anomaly Detection
  - Alert Generation
        ↓
AWS S3 Bronze / Silver / Gold
  └── Apache Iceberg (ACID table format on Gold layer)
        +
AWS DynamoDB (Real-Time sensor readings & alerts)
        ↓
Amazon Athena (SQL Queries on S3 Gold layer)
        ↓
Plotly Dash (Live Dashboard)
```

## How Each Layer Works

| Layer | Technology | What It Does |
|-------|-----------|--------------|
| IoT Data Source | Sensor.Community API | Free real sensor data, no API key needed |
| Message Broker | Apache Kafka (Docker) | Ingests and streams raw sensor messages |
| Stream Processing | Python 3.13 | Anomaly detection, windowed aggregations |
| Bronze Layer | AWS S3 | Raw data stored exactly as received |
| Silver Layer | AWS S3 + Parquet | Cleaned, normalised, validated data |
| Gold Layer | AWS S3 + Apache Iceberg | Aggregated, query-ready tables with ACID |
| Real-Time DB | AWS DynamoDB | Fast storage for live readings and alerts |
| Query Engine | Amazon Athena | Serverless SQL on top of S3 Gold layer |
| Dashboard | Plotly Dash | Live charts and alerts in the browser |

## Why Apache Iceberg?

S3 stores files. Iceberg gives those files full table features:
- ACID transactions (no corrupt writes)
- Schema evolution (add columns safely)
- Time travel (query data as of any point in time)
- Partition management (faster Athena queries)

## Why DynamoDB?

DynamoDB handles everything real-time:
- Stores live sensor readings as they arrive
- Stores anomaly alerts instantly
- Fast key-value lookups for the dashboard
- Free tier: 25GB storage, 25 read/write units

## Project Structure

```
iot-pipeline/
├── config/
│   └── settings.py
├── producer/
│   └── sensor_producer.py
├── processor/
│   └── stream_processor.py
├── storage/
│   ├── s3_layer.py
│   └── dynamodb_writer.py
├── query/
│   └── athena_client.py
├── dashboard/
│   └── app.py
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

### Prerequisites
- Python 3.13
- Docker Desktop
- AWS CLI configured

### Run Locally

```
1. Start Kafka
   docker start zookeeper kafka

2. Install dependencies
   pip install -r requirements.txt

3. Configure environment
   cp .env.example .env

4. Run producer
   python producer/sensor_producer.py

5. Run stream processor
   python processor/stream_processor.py

6. Run dashboard
   python dashboard/app.py
```

## Build Steps

| Step | What We Build |
|------|--------------|
| Step 0 | Git + GitHub setup |
| Step 1 | Kafka topics setup |
| Step 2 | Project structure + config |
| Step 3 | Kafka Producer (Sensor.Community to Kafka) |
| Step 4 | Stream Processor (anomaly detection + aggregations) |
| Step 5 | S3 Bronze/Silver/Gold + Apache Iceberg |
| Step 6 | DynamoDB (real-time readings and alerts) |
| Step 7 | Plotly Dash live dashboard |

## Author
Saikrishna — Senior Data Engineer