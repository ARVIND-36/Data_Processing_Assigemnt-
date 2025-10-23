"""
Task 2: Real-Time Data Streaming - Kafka Producer
Generates and streams real-time data to Kafka topics
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealTimeDataProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialize Kafka Producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("Kafka Producer initialized successfully")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise
    
    def generate_sensor_data(self):
        """Generate simulated IoT sensor data"""
        return {
            'sensor_id': f'sensor_{random.randint(1, 10)}',
            'timestamp': datetime.now().isoformat(),
            'temperature': round(random.uniform(15.0, 35.0), 2),
            'humidity': round(random.uniform(30.0, 90.0), 2),
            'pressure': round(random.uniform(980.0, 1020.0), 2),
            'location': random.choice(['Zone_A', 'Zone_B', 'Zone_C', 'Zone_D']),
            'status': random.choice(['normal', 'warning', 'critical'])
        }
    
    def generate_transaction_data(self):
        """Generate simulated transaction data"""
        return {
            'transaction_id': f'txn_{random.randint(10000, 99999)}',
            'timestamp': datetime.now().isoformat(),
            'user_id': f'user_{random.randint(1, 1000)}',
            'amount': round(random.uniform(10.0, 5000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            'category': random.choice(['food', 'electronics', 'clothing', 'entertainment']),
            'merchant': f'merchant_{random.randint(1, 50)}',
            'status': random.choice(['completed', 'pending', 'failed'])
        }
    
    def generate_log_data(self):
        """Generate simulated application log data"""
        return {
            'log_id': f'log_{random.randint(10000, 99999)}',
            'timestamp': datetime.now().isoformat(),
            'level': random.choice(['INFO', 'WARNING', 'ERROR', 'CRITICAL']),
            'service': random.choice(['api-service', 'auth-service', 'db-service', 'cache-service']),
            'message': random.choice([
                'Request processed successfully',
                'Connection timeout',
                'Authentication failed',
                'Database query slow',
                'Cache miss'
            ]),
            'response_time_ms': random.randint(10, 5000),
            'status_code': random.choice([200, 201, 400, 401, 403, 404, 500, 503])
        }
    
    def send_message(self, topic, data, key=None):
        """Send message to Kafka topic"""
        try:
            future = self.producer.send(topic, value=data, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message sent to topic: {record_metadata.topic}, "
                       f"partition: {record_metadata.partition}, "
                       f"offset: {record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def stream_sensor_data(self, topic='sensor-data', duration=60, interval=1):
        """
        Stream sensor data continuously
        
        Args:
            topic: Kafka topic name
            duration: Duration in seconds (0 for infinite)
            interval: Interval between messages in seconds
        """
        logger.info(f"Starting sensor data stream to topic: {topic}")
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check duration
                if duration > 0 and (time.time() - start_time) > duration:
                    break
                
                # Generate and send data
                data = self.generate_sensor_data()
                if self.send_message(topic, data, key=data['sensor_id']):
                    message_count += 1
                    if message_count % 10 == 0:
                        logger.info(f"Sent {message_count} messages")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        finally:
            logger.info(f"Total messages sent: {message_count}")
    
    def stream_transaction_data(self, topic='transaction-data', duration=60, interval=0.5):
        """Stream transaction data continuously"""
        logger.info(f"Starting transaction data stream to topic: {topic}")
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                if duration > 0 and (time.time() - start_time) > duration:
                    break
                
                data = self.generate_transaction_data()
                if self.send_message(topic, data, key=data['user_id']):
                    message_count += 1
                    if message_count % 20 == 0:
                        logger.info(f"Sent {message_count} messages")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        finally:
            logger.info(f"Total messages sent: {message_count}")
    
    def stream_log_data(self, topic='log-data', duration=60, interval=0.2):
        """Stream log data continuously"""
        logger.info(f"Starting log data stream to topic: {topic}")
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                if duration > 0 and (time.time() - start_time) > duration:
                    break
                
                data = self.generate_log_data()
                if self.send_message(topic, data, key=data['service']):
                    message_count += 1
                    if message_count % 50 == 0:
                        logger.info(f"Sent {message_count} messages")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        finally:
            logger.info(f"Total messages sent: {message_count}")
    
    def close(self):
        """Close Kafka producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka Producer closed")


def main():
    """Main function to run the producer"""
    # Configuration
    BOOTSTRAP_SERVERS = ['localhost:9092']
    DATA_TYPE = 'sensor'  # Options: 'sensor', 'transaction', 'log'
    DURATION = 300  # seconds (0 for infinite)
    
    # Create producer
    producer = RealTimeDataProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    try:
        if DATA_TYPE == 'sensor':
            producer.stream_sensor_data(
                topic='sensor-data',
                duration=DURATION,
                interval=1
            )
        elif DATA_TYPE == 'transaction':
            producer.stream_transaction_data(
                topic='transaction-data',
                duration=DURATION,
                interval=0.5
            )
        elif DATA_TYPE == 'log':
            producer.stream_log_data(
                topic='log-data',
                duration=DURATION,
                interval=0.2
            )
        else:
            logger.error(f"Unknown data type: {DATA_TYPE}")
    
    finally:
        producer.close()


if __name__ == "__main__":
    main()
