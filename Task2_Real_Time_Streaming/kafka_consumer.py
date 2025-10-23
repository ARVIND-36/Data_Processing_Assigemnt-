"""
Task 2: Real-Time Data Streaming - Kafka Consumer
Consumes and processes real-time data from Kafka topics with ML analytics
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import time
from datetime import datetime
from collections import deque
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import logging
import pickle
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealTimeDataConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], group_id='data-processor'):
        """Initialize Kafka Consumer"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=100
            )
            logger.info(f"Kafka Consumer initialized with group_id: {group_id}")
            
            # Initialize data buffers for rolling analytics
            self.data_buffer = deque(maxlen=100)
            self.temperature_buffer = deque(maxlen=50)
            self.amount_buffer = deque(maxlen=50)
            
            # Initialize ML models
            self.classification_model = None
            self.regression_model = None
            self.label_encoders = {}
            
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise
    
    def subscribe_to_topics(self, topics):
        """Subscribe to Kafka topics"""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
    
    def calculate_rolling_average(self, buffer):
        """Calculate rolling average from buffer"""
        if len(buffer) > 0:
            return np.mean(list(buffer))
        return 0.0
    
    def calculate_rolling_std(self, buffer):
        """Calculate rolling standard deviation"""
        if len(buffer) > 1:
            return np.std(list(buffer))
        return 0.0
    
    def aggregate_values(self, buffer, agg_type='sum'):
        """Aggregate values from buffer"""
        if len(buffer) == 0:
            return 0.0
        
        data = list(buffer)
        if agg_type == 'sum':
            return np.sum(data)
        elif agg_type == 'mean':
            return np.mean(data)
        elif agg_type == 'max':
            return np.max(data)
        elif agg_type == 'min':
            return np.min(data)
        else:
            return np.median(data)
    
    def detect_anomaly(self, value, buffer, threshold=2):
        """Simple anomaly detection using z-score"""
        if len(buffer) < 10:
            return False
        
        mean = np.mean(list(buffer))
        std = np.std(list(buffer))
        
        if std == 0:
            return False
        
        z_score = abs((value - mean) / std)
        return z_score > threshold
    
    def prepare_features_for_classification(self, data):
        """
        Prepare features for classification model
        Example: Classify sensor status based on readings
        """
        features = []
        
        if 'temperature' in data and 'humidity' in data and 'pressure' in data:
            features = [
                data.get('temperature', 0),
                data.get('humidity', 0),
                data.get('pressure', 0)
            ]
        elif 'amount' in data and 'response_time_ms' in data:
            features = [
                data.get('amount', 0),
                data.get('response_time_ms', 0)
            ]
        
        return np.array(features).reshape(1, -1) if features else None
    
    def train_simple_classifier(self, X_train, y_train):
        """Train a simple Random Forest classifier"""
        logger.info("Training classification model...")
        self.classification_model = RandomForestClassifier(
            n_estimators=100,
            max_depth=5,
            random_state=42
        )
        self.classification_model.fit(X_train, y_train)
        logger.info("Classification model trained")
    
    def train_simple_regressor(self, X_train, y_train):
        """Train a simple Random Forest regressor"""
        logger.info("Training regression model...")
        self.regression_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=5,
            random_state=42
        )
        self.regression_model.fit(X_train, y_train)
        logger.info("Regression model trained")
    
    def predict_classification(self, features):
        """Make classification prediction"""
        if self.classification_model is None:
            return "model_not_trained"
        
        try:
            prediction = self.classification_model.predict(features)
            probability = self.classification_model.predict_proba(features)
            return prediction[0], np.max(probability)
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return "error", 0.0
    
    def predict_regression(self, features):
        """Make regression prediction"""
        if self.regression_model is None:
            return 0.0
        
        try:
            prediction = self.regression_model.predict(features)
            return prediction[0]
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return 0.0
    
    def process_sensor_data(self, data):
        """Process sensor data with analytics"""
        # Extract temperature
        if 'temperature' in data:
            temp = data['temperature']
            self.temperature_buffer.append(temp)
            
            # Calculate rolling average
            rolling_avg = self.calculate_rolling_average(self.temperature_buffer)
            rolling_std = self.calculate_rolling_std(self.temperature_buffer)
            
            # Detect anomaly
            is_anomaly = self.detect_anomaly(temp, self.temperature_buffer)
            
            # ML Prediction (if model is trained)
            features = self.prepare_features_for_classification(data)
            if features is not None and self.classification_model is not None:
                predicted_status, confidence = self.predict_classification(features)
                
                logger.info(
                    f"Sensor: {data.get('sensor_id')} | "
                    f"Temp: {temp:.2f}°C | "
                    f"Avg: {rolling_avg:.2f}°C | "
                    f"Std: {rolling_std:.2f} | "
                    f"Anomaly: {is_anomaly} | "
                    f"Predicted: {predicted_status} (conf: {confidence:.2f})"
                )
            else:
                logger.info(
                    f"Sensor: {data.get('sensor_id')} | "
                    f"Temp: {temp:.2f}°C | "
                    f"Avg: {rolling_avg:.2f}°C | "
                    f"Anomaly: {is_anomaly}"
                )
    
    def process_transaction_data(self, data):
        """Process transaction data with analytics"""
        if 'amount' in data:
            amount = data['amount']
            self.amount_buffer.append(amount)
            
            # Calculate aggregates
            total_amount = self.aggregate_values(self.amount_buffer, 'sum')
            avg_amount = self.aggregate_values(self.amount_buffer, 'mean')
            max_amount = self.aggregate_values(self.amount_buffer, 'max')
            
            logger.info(
                f"Transaction: {data.get('transaction_id')} | "
                f"Amount: ${amount:.2f} | "
                f"Rolling Total: ${total_amount:.2f} | "
                f"Avg: ${avg_amount:.2f} | "
                f"Max: ${max_amount:.2f}"
            )
    
    def process_log_data(self, data):
        """Process log data with analytics"""
        if 'response_time_ms' in data:
            response_time = data['response_time_ms']
            
            # Simple threshold-based alerting
            if response_time > 1000:
                logger.warning(
                    f"SLOW RESPONSE: {data.get('service')} | "
                    f"Time: {response_time}ms | "
                    f"Status: {data.get('status_code')}"
                )
            else:
                logger.info(
                    f"Service: {data.get('service')} | "
                    f"Level: {data.get('level')} | "
                    f"Time: {response_time}ms"
                )
    
    def consume_and_process(self, timeout_ms=1000):
        """
        Main consumer loop with processing and analytics
        """
        logger.info("Starting message consumption...")
        message_count = 0
        
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=timeout_ms)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        message_count += 1
                        data = record.value
                        
                        # Store in buffer
                        self.data_buffer.append(data)
                        
                        # Process based on topic
                        if 'sensor' in record.topic:
                            self.process_sensor_data(data)
                        elif 'transaction' in record.topic:
                            self.process_transaction_data(data)
                        elif 'log' in record.topic:
                            self.process_log_data(data)
                        
                        # Log every 50 messages
                        if message_count % 50 == 0:
                            logger.info(f"Processed {message_count} messages")
        
        except KeyboardInterrupt:
            logger.info("Consumption interrupted by user")
        finally:
            logger.info(f"Total messages processed: {message_count}")
            self.close()
    
    def close(self):
        """Close Kafka consumer"""
        self.consumer.close()
        logger.info("Kafka Consumer closed")


def main():
    """Main function to run the consumer"""
    # Configuration
    BOOTSTRAP_SERVERS = ['localhost:9092']
    GROUP_ID = 'realtime-processor'
    TOPICS = ['sensor-data', 'transaction-data', 'log-data']
    
    # Create consumer
    consumer = RealTimeDataConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID
    )
    
    # Subscribe to topics
    consumer.subscribe_to_topics(TOPICS)
    
    # Optional: Train a simple model (you would do this with historical data)
    # For demo purposes, creating synthetic training data
    # X_train = np.random.rand(100, 3) * 30
    # y_train = np.random.choice(['normal', 'warning', 'critical'], 100)
    # consumer.train_simple_classifier(X_train, y_train)
    
    # Start consuming
    consumer.consume_and_process()


if __name__ == "__main__":
    main()
