# Kafka JSON Pipeline with PostgreSQL Integration

## Overview
This project sets up a Kafka-based pipeline to process JSON data, send it to a Kafka topic, and store it in a PostgreSQL database. The pipeline includes a producer that reads nested JSON data, flattens it, and sends it to a Kafka topic, and a consumer that reads data from the topic and stores it in a PostgreSQL database.

---
#### Key Features of Kafka Producer:
- Fetches data from a Dropbox URL.
- Uses multithreading for efficient processing.
- Sends data to a Kafka topic (`mec-xdr`).

#### Requirements:
- `requests`
- `kafka-python`
- `concurrent.futures`

### 2. Key Feature of Kafka Consumer
- Reads messages from the Kafka topic `mec-xdr`.
- Inserts data into the `server_data` table in PostgreSQL.

#### Docker Compose Configuration:
- **Services**:
  - Zookeeper
  - Kafka Broker
  - Schema Registry
  - Kafka UI for monitoring
  - 
---

## Instructions

### 1. Clone Repository
```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. Start Kafka and Zookeeper
```bash
docker-compose up -d
```

### 3. Run the Producer
```bash
python producer.py
```

### 4. Run the Consumer
```bash
python consumer.py
```

### 5. Access Kafka UI
- URL: [http://localhost:8000](http://localhost:8000)

---

## Dependencies

### Python Libraries:
- kafka-python
- Zookeeper
- psycopg2
- requests

### Tools:
- Docker
- PostgreSQL
