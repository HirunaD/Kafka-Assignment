# Kafka Order Processing System with Avro Serialization

A complete real-time order processing system built with Apache Kafka, implementing **Avro serialization**, real-time aggregation, retry logic, and Dead Letter Queue (DLQ) functionality.

## 📋 Project Overview

This system fulfills all assignment requirements by building a Kafka-based system that:
- **Produces** order messages with **Avro serialization** 
- **Consumes** messages and calculates **running average prices** per product
- **Handles failures** with configurable **retry logic** (3 attempts)
- **Manages permanent failures** through a **Dead Letter Queue (DLQ)**
- **Runs entirely on Docker** with Schema Registry
- **Demonstrates live** with real-time aggregation and Avro schema management

## 🏗️ Project Structure

kafka-avro-order-system/
├── docker-compose.yml          # Kafka + Schema Registry
├── order.avsc                  # Avro schema definition
├── producer.py                 # Avro Order Producer
├── consumer.py                 # Avro Consumer with retry logic
├── dlq_consumer.py             # DLQ processor
├── requirements.txt            # Python dependencies
└── README.md                  # This file

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+

### 1. Clone and Setup

```bash
# Create project directory
mkdir kafka-order-processing-system
cd kafka-order-processing-system
```
# Copy all provided files to this directory

### 2. Initial Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

### 3. Start Services

```bash
# Start Kafka and Zookeeper
docker-compose up -d
```

### 4. Run the System

# Terminal 1 - Start Producer:

```bash
python producer.py
```

# Terminal 2 - Start Consumer:

```bash
python consumer.py
```

# Terminal 3 - Start DLQ Consumer

```bash
python dlq_consumer.py
```

### 📊 Expected Output

# Producer Output:

```bash
📦 Avro Order: {'orderId': 'AVRO-000029', 'product': 'Tablet', 'price': 211.86}
✅ Delivered to orders [0]
```

# Consumer Output:

```bash
📨 Avro Order: AVRO-000146 - Keyboard - $515.53
🔄 Attempt 1/3 for AVRO-000146
✅ Success: AVRO-000146
```

# DLQ Consumer Output:

```bash
❌ PERMANENTLY FAILED ORDER:
   Order ID: AVRO-000116
   Product:  Keyboard
   Price:    $641.82
   Error:    Simulated temporary failure: price too high
   Retries:  3 attempts
```

