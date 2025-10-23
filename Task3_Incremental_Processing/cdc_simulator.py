"""
Task 3: CDC Event Simulator
Simulates database change events for testing CDC system
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCEventSimulator:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialize CDC Event Producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.record_counter = 0
        logger.info("CDC Event Simulator initialized")
    
    def generate_create_event(self):
        """Generate CREATE (INSERT) event"""
        self.record_counter += 1
        record_id = self.record_counter
        
        event = {
            'op': 'c',  # create
            'before': None,
            'after': {
                'id': record_id,
                'name': f'Record_{record_id}',
                'value': round(random.uniform(10, 100), 2),
                'age': random.randint(18, 80),
                'income': round(random.uniform(20000, 150000), 2),
                'status': random.choice(['normal', 'warning', 'critical']),
                'label': random.randint(0, 2),
                'created_at': datetime.now().isoformat()
            },
            'source': {
                'table': 'users',
                'db': 'testdb'
            },
            'ts_ms': int(time.time() * 1000)
        }
        
        return event
    
    def generate_update_event(self):
        """Generate UPDATE event"""
        record_id = random.randint(1, max(1, self.record_counter))
        
        before_value = round(random.uniform(10, 100), 2)
        after_value = before_value + round(random.uniform(-10, 10), 2)
        
        before_status = random.choice(['normal', 'warning', 'critical'])
        after_status = random.choice(['normal', 'warning', 'critical'])
        
        event = {
            'op': 'u',  # update
            'before': {
                'id': record_id,
                'name': f'Record_{record_id}',
                'value': before_value,
                'status': before_status,
                'label': random.randint(0, 2)
            },
            'after': {
                'id': record_id,
                'name': f'Record_{record_id}',
                'value': after_value,
                'status': after_status,
                'label': random.randint(0, 2),
                'updated_at': datetime.now().isoformat()
            },
            'source': {
                'table': 'users',
                'db': 'testdb'
            },
            'ts_ms': int(time.time() * 1000)
        }
        
        return event
    
    def generate_delete_event(self):
        """Generate DELETE event"""
        record_id = random.randint(1, max(1, self.record_counter))
        
        event = {
            'op': 'd',  # delete
            'before': {
                'id': record_id,
                'name': f'Record_{record_id}',
                'value': round(random.uniform(10, 100), 2),
                'status': random.choice(['normal', 'warning', 'critical'])
            },
            'after': None,
            'source': {
                'table': 'users',
                'db': 'testdb'
            },
            'ts_ms': int(time.time() * 1000)
        }
        
        return event
    
    def send_event(self, topic, event):
        """Send CDC event to Kafka"""
        try:
            future = self.producer.send(topic, event)
            future.get(timeout=10)
            logger.info(f"Sent {event['op']} event for record {event.get('after', event.get('before', {})).get('id')}")
            return True
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            return False
    
    def simulate_cdc_stream(self, topic='cdc-changes', duration=300, interval=2):
        """
        Simulate CDC event stream
        
        Args:
            topic: Kafka topic for CDC events
            duration: Duration in seconds (0 for infinite)
            interval: Interval between events
        """
        logger.info(f"Starting CDC event simulation on topic: {topic}")
        start_time = time.time()
        event_count = 0
        
        try:
            while True:
                if duration > 0 and (time.time() - start_time) > duration:
                    break
                
                # Generate random operation
                operation = random.choices(
                    ['create', 'update', 'delete'],
                    weights=[0.5, 0.4, 0.1]  # More creates/updates than deletes
                )[0]
                
                if operation == 'create':
                    event = self.generate_create_event()
                elif operation == 'update' and self.record_counter > 0:
                    event = self.generate_update_event()
                elif operation == 'delete' and self.record_counter > 0:
                    event = self.generate_delete_event()
                else:
                    event = self.generate_create_event()
                
                # Send event
                if self.send_event(topic, event):
                    event_count += 1
                
                if event_count % 10 == 0:
                    logger.info(f"Sent {event_count} CDC events")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        
        finally:
            logger.info(f"Total CDC events sent: {event_count}")
            self.producer.close()


def main():
    """Main function"""
    BOOTSTRAP_SERVERS = ['localhost:9092']
    CDC_TOPIC = 'cdc-changes'
    DURATION = 300  # 5 minutes
    INTERVAL = 1  # 1 second between events
    
    simulator = CDCEventSimulator(bootstrap_servers=BOOTSTRAP_SERVERS)
    simulator.simulate_cdc_stream(
        topic=CDC_TOPIC,
        duration=DURATION,
        interval=INTERVAL
    )


if __name__ == "__main__":
    main()
