# Task 2: Real-Time Data Streaming Challenge (35%)

## Overview
Create a Producer-Consumer application using Apache Kafka for real-time data streaming with analytics and machine learning.

## Objectives
- Set up Kafka topics
- Implement data producer for streaming
- Implement data consumer with processing
- Calculate rolling averages and aggregates
- Apply ML models for real-time analytics

## Files
- `kafka_setup.py` - Kafka topic setup and management
- `kafka_producer.py` - Real-time data producer
- `kafka_consumer.py` - Data consumer with analytics
- `requirements.txt` - Python dependencies

## Setup Instructions

### 1. Install Kafka
Download and install Apache Kafka from: https://kafka.apache.org/downloads

### 2. Start Kafka Services

#### Start Zookeeper
```powershell
# Navigate to Kafka directory
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

#### Start Kafka Server
```powershell
# In a new terminal
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 3. Install Python Dependencies
```powershell
pip install -r requirements.txt
```

### 4. Create Kafka Topics
```powershell
python kafka_setup.py
```

## Usage

### Step 1: Start the Consumer
```powershell
python kafka_consumer.py
```

### Step 2: Start the Producer (in another terminal)
```powershell
python kafka_producer.py
```

## Data Streams Implemented

### 1. Sensor Data Stream
- Temperature, humidity, pressure readings
- Location and status information
- Anomaly detection
- Rolling averages

### 2. Transaction Data Stream
- Customer transactions
- Amount aggregation
- Fraud detection patterns
- Rolling totals

### 3. Log Data Stream
- Application logs
- Response time monitoring
- Error rate tracking
- Performance analytics

## Real-Time Analytics Features

### 1. Rolling Statistics
- Rolling average
- Rolling standard deviation
- Min/Max tracking
- Sum aggregation

### 2. Anomaly Detection
- Z-score based detection
- Threshold alerting
- Pattern recognition

### 3. Machine Learning Integration
- Classification (Random Forest)
- Regression models
- Real-time predictions
- Confidence scoring

## Customization

### Change Data Type
In `kafka_producer.py`, modify:
```python
DATA_TYPE = 'sensor'  # Options: 'sensor', 'transaction', 'log'
```

### Adjust Streaming Duration
```python
DURATION = 300  # seconds (0 for infinite)
```

### Configure Buffer Sizes
In `kafka_consumer.py`:
```python
self.data_buffer = deque(maxlen=100)  # Adjust maxlen
```

## Monitoring

### Check Topics
```powershell
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Check Consumer Groups
```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

## Troubleshooting

### Kafka Connection Issues
1. Ensure Zookeeper is running
2. Ensure Kafka server is running
3. Check `localhost:9092` is accessible

### Consumer Lag
- Increase number of partitions
- Scale consumer group
- Optimize processing logic

## Performance Tips
- Use appropriate batch sizes
- Configure proper buffer sizes
- Monitor consumer lag
- Use compression for large messages
