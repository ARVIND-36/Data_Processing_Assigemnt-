# Task 3: Incremental Data Processing Challenge (25%)

## Overview
Implement Incremental Data Processing using Change Data Capture (CDC) techniques to update machine learning models in response to data changes.

## Objectives
- Capture data changes from streaming sources
- Process changes incrementally
- Update ML models online (incremental learning)
- Track data versioning and changes

## Files
- `cdc_processor.py` - CDC event processor with incremental ML
- `cdc_simulator.py` - CDC event simulator for testing
- `requirements.txt` - Python dependencies

## Setup Instructions

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Start Kafka Services
Ensure Kafka and Zookeeper are running (see Task 2 instructions)

### 3. Create CDC Topic
```powershell
# Using Kafka CLI
.\bin\windows\kafka-topics.bat --create --topic cdc-changes --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Usage

### Step 1: Start CDC Processor
```powershell
python cdc_processor.py
```

### Step 2: Start CDC Event Simulator (in another terminal)
```powershell
python cdc_simulator.py
```

## CDC Event Format

CDC events follow the Debezium-like format:

```json
{
    "op": "c/u/d",
    "before": {
        "id": 123,
        "field1": "old_value"
    },
    "after": {
        "id": 123,
        "field1": "new_value"
    },
    "source": {
        "table": "table_name",
        "db": "database_name"
    },
    "ts_ms": 1234567890
}
```

### Operations
- `c` - CREATE (INSERT)
- `u` - UPDATE
- `d` - DELETE

## Features Implemented

### 1. Change Data Capture
- Real-time change detection
- Operation type identification
- Before/After state tracking
- Source metadata capture

### 2. Incremental Processing
- Stream-based processing
- Incremental data updates
- State management
- Data versioning

### 3. Incremental Machine Learning
- Online learning with SGD
- Incremental model updates
- Feature scaling
- Model persistence

### 4. Supported ML Models
- **SGDClassifier**: For classification tasks
- **SGDRegressor**: For regression tasks
- **StandardScaler**: For feature normalization

## Model Update Strategy

### Partial Fit
Models are updated incrementally using `partial_fit()`:
- No need to retrain on entire dataset
- Memory efficient
- Real-time learning
- Adapts to data drift

### Model Persistence
- Models saved every 100 updates
- Automatic loading on restart
- Version control ready

## Customization

### Modify Feature Extraction
Edit `extract_features()` in `cdc_processor.py`:
```python
def extract_features(self, data):
    features = []
    # Add your feature extraction logic
    features.append(data.get('your_field'))
    return np.array(features).reshape(1, -1)
```

### Adjust Label Extraction
Edit `extract_label()` for your use case:
```python
def extract_label(self, data):
    return data.get('your_label_field')
```

### Change Update Frequency
Modify save frequency:
```python
if self.update_count % 100 == 0:  # Change 100 to your value
    self.save_models()
```

## Integration with Real Databases

### Using Debezium with MySQL
```properties
# Debezium connector configuration
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=localhost
database.port=3306
database.user=debezium
database.password=dbz
database.server.id=184054
database.server.name=myserver
table.include.list=inventory.customers
```

### Using Kafka Connect
```bash
# Start Kafka Connect
.\bin\connect-standalone.bat .\config\connect-standalone.properties .\config\debezium-mysql-connector.properties
```

## Monitoring

### Check Model Performance
Models are saved in `models/` directory:
- `classifier.pkl`
- `regressor.pkl`
- `scaler.pkl`

### Track Statistics
The processor logs statistics every 50 updates:
- Total updates processed
- Records in store
- Model fit status

## Advanced Features

### Data Store
In-memory data store maintains current state:
```python
self.data_store[record_id] = data
```

### Prediction API
Make predictions with current model:
```python
features = extract_features(new_data)
result = processor.predict(features)
```

## Performance Considerations
- Use appropriate batch sizes
- Monitor memory usage
- Periodic model checkpointing
- Consider model drift detection

## Troubleshooting

### Model Not Fitting
- Ensure sufficient training data
- Check feature extraction logic
- Verify label format

### Memory Issues
- Reduce buffer sizes
- Increase checkpoint frequency
- Clear old data from store
