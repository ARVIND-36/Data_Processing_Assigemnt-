"""
Task 2: Kafka Setup and Topic Management
Utilities for creating and managing Kafka topics
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaTopicManager:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialize Kafka Admin Client"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='topic_manager'
            )
            logger.info("Kafka Admin Client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Admin Client: {e}")
            raise
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """Create a new Kafka topic"""
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            self.admin_client.create_topics([topic], validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully")
            return True
        except TopicAlreadyExistsError:
            logger.warning(f"Topic '{topic_name}' already exists")
            return False
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            return False
    
    def delete_topic(self, topic_name):
        """Delete a Kafka topic"""
        try:
            self.admin_client.delete_topics([topic_name])
            logger.info(f"Topic '{topic_name}' deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic_name}': {e}")
            return False
    
    def list_topics(self):
        """List all Kafka topics"""
        try:
            topics = self.admin_client.list_topics()
            logger.info(f"Available topics: {topics}")
            return topics
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    def close(self):
        """Close Admin Client"""
        self.admin_client.close()
        logger.info("Kafka Admin Client closed")


def setup_topics():
    """Setup all required topics for the assignment"""
    manager = KafkaTopicManager()
    
    topics_to_create = [
        ('sensor-data', 3, 1),
        ('transaction-data', 3, 1),
        ('log-data', 3, 1),
        ('processed-data', 3, 1)
    ]
    
    logger.info("Creating Kafka topics...")
    for topic_name, partitions, replication in topics_to_create:
        manager.create_topic(topic_name, partitions, replication)
    
    # List all topics
    logger.info("\nCurrent topics:")
    manager.list_topics()
    
    manager.close()


if __name__ == "__main__":
    setup_topics()
