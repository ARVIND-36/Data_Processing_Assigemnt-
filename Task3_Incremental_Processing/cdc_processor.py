"""
Task 3: Incremental Data Processing with Change Data Capture (CDC)
Implements CDC system using Kafka Connect and processes changes incrementally
"""

import json
import logging
from kafka import KafkaConsumer
from collections import defaultdict
import numpy as np
from sklearn.linear_model import SGDClassifier, SGDRegressor
from sklearn.preprocessing import StandardScaler
import pickle
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCProcessor:
    def __init__(self, bootstrap_servers=['localhost:9092'], model_path='models/'):
        """Initialize CDC Processor"""
        self.bootstrap_servers = bootstrap_servers
        self.model_path = model_path
        
        # Create model directory if it doesn't exist
        os.makedirs(model_path, exist_ok=True)
        
        # Initialize incremental learning models
        self.classifier = SGDClassifier(loss='log_loss', random_state=42)
        self.regressor = SGDRegressor(random_state=42)
        self.scaler = StandardScaler()
        
        # Data storage for incremental updates
        self.data_store = defaultdict(dict)
        self.update_count = 0
        self.is_fitted = False
        
        # Load existing models if available
        self.load_models()
        
        logger.info("CDC Processor initialized")
    
    def create_cdc_consumer(self, topics, group_id='cdc-processor'):
        """Create Kafka consumer for CDC topics"""
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info(f"CDC Consumer created for topics: {topics}")
        return consumer
    
    def parse_cdc_event(self, event):
        """
        Parse CDC event from Kafka Connect format
        Expected format: {
            'op': 'c/u/d',  # create/update/delete
            'before': {...},
            'after': {...},
            'source': {...}
        }
        """
        operation = event.get('op', 'u')
        before_data = event.get('before', {})
        after_data = event.get('after', {})
        source_info = event.get('source', {})
        
        return {
            'operation': operation,
            'before': before_data,
            'after': after_data,
            'source': source_info,
            'timestamp': event.get('ts_ms', datetime.now().timestamp())
        }
    
    def extract_features(self, data):
        """Extract features from data record"""
        # Customize based on your data schema
        features = []
        
        # Example: Extract numeric features
        numeric_fields = ['age', 'income', 'amount', 'value', 'score']
        for field in numeric_fields:
            if field in data:
                features.append(float(data[field]))
        
        # If no features found, create dummy features
        if not features:
            features = [float(data.get('value', 0))]
        
        return np.array(features).reshape(1, -1)
    
    def extract_label(self, data):
        """Extract label for supervised learning"""
        # Customize based on your use case
        if 'label' in data:
            return data['label']
        elif 'status' in data:
            # Map status to numeric label
            status_map = {'normal': 0, 'warning': 1, 'critical': 2}
            return status_map.get(data['status'], 0)
        elif 'target' in data:
            return float(data['target'])
        else:
            return 0
    
    def process_create_operation(self, record_id, data):
        """Process CREATE operation"""
        logger.info(f"CREATE: Adding new record {record_id}")
        
        # Store data
        self.data_store[record_id] = data
        
        # Extract features and label
        features = self.extract_features(data)
        label = self.extract_label(data)
        
        # Incremental model update
        self.update_model_incrementally(features, label)
        
        self.update_count += 1
    
    def process_update_operation(self, record_id, before_data, after_data):
        """Process UPDATE operation"""
        logger.info(f"UPDATE: Modifying record {record_id}")
        
        # Update stored data
        if record_id in self.data_store:
            old_data = self.data_store[record_id]
            logger.debug(f"Before: {old_data}")
        
        self.data_store[record_id] = after_data
        logger.debug(f"After: {after_data}")
        
        # Extract features and label from updated data
        features = self.extract_features(after_data)
        label = self.extract_label(after_data)
        
        # Incremental model update
        self.update_model_incrementally(features, label)
        
        self.update_count += 1
    
    def process_delete_operation(self, record_id):
        """Process DELETE operation"""
        logger.info(f"DELETE: Removing record {record_id}")
        
        if record_id in self.data_store:
            del self.data_store[record_id]
            logger.debug(f"Record {record_id} deleted from store")
    
    def update_model_incrementally(self, features, label):
        """
        Update ML model incrementally with new data
        Uses Stochastic Gradient Descent for online learning
        """
        try:
            # Scale features
            if self.is_fitted:
                features_scaled = self.scaler.transform(features)
            else:
                features_scaled = self.scaler.fit_transform(features)
            
            # Update classifier (for classification tasks)
            if isinstance(label, (int, np.integer)) or label in [0, 1, 2]:
                if self.is_fitted:
                    self.classifier.partial_fit(features_scaled, [label])
                else:
                    self.classifier.partial_fit(features_scaled, [label], 
                                               classes=np.array([0, 1, 2]))
                    self.is_fitted = True
                
                logger.debug(f"Classifier updated with label: {label}")
            
            # Update regressor (for regression tasks)
            else:
                if self.is_fitted:
                    self.regressor.partial_fit(features_scaled, [label])
                else:
                    self.regressor.partial_fit(features_scaled, [label])
                    self.is_fitted = True
                
                logger.debug(f"Regressor updated with target: {label}")
            
            # Save models periodically
            if self.update_count % 100 == 0:
                self.save_models()
                logger.info(f"Models saved after {self.update_count} updates")
        
        except Exception as e:
            logger.error(f"Error updating model: {e}")
    
    def predict(self, features):
        """Make prediction using current model"""
        if not self.is_fitted:
            logger.warning("Model not yet fitted")
            return None
        
        try:
            features_scaled = self.scaler.transform(features)
            
            # Try classification first
            try:
                prediction = self.classifier.predict(features_scaled)
                probability = self.classifier.predict_proba(features_scaled)
                return {
                    'type': 'classification',
                    'prediction': int(prediction[0]),
                    'probability': float(np.max(probability))
                }
            except:
                # Fall back to regression
                prediction = self.regressor.predict(features_scaled)
                return {
                    'type': 'regression',
                    'prediction': float(prediction[0])
                }
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return None
    
    def save_models(self):
        """Save models to disk"""
        try:
            with open(os.path.join(self.model_path, 'classifier.pkl'), 'wb') as f:
                pickle.dump(self.classifier, f)
            with open(os.path.join(self.model_path, 'regressor.pkl'), 'wb') as f:
                pickle.dump(self.regressor, f)
            with open(os.path.join(self.model_path, 'scaler.pkl'), 'wb') as f:
                pickle.dump(self.scaler, f)
            
            logger.info("Models saved successfully")
        except Exception as e:
            logger.error(f"Error saving models: {e}")
    
    def load_models(self):
        """Load models from disk"""
        try:
            classifier_path = os.path.join(self.model_path, 'classifier.pkl')
            regressor_path = os.path.join(self.model_path, 'regressor.pkl')
            scaler_path = os.path.join(self.model_path, 'scaler.pkl')
            
            if os.path.exists(classifier_path):
                with open(classifier_path, 'rb') as f:
                    self.classifier = pickle.load(f)
                with open(regressor_path, 'rb') as f:
                    self.regressor = pickle.load(f)
                with open(scaler_path, 'rb') as f:
                    self.scaler = pickle.load(f)
                
                self.is_fitted = True
                logger.info("Models loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load existing models: {e}")
    
    def get_statistics(self):
        """Get processing statistics"""
        return {
            'total_updates': self.update_count,
            'records_in_store': len(self.data_store),
            'model_fitted': self.is_fitted
        }
    
    def run_cdc_processor(self, topics):
        """
        Main CDC processing loop
        Consumes CDC events and updates models incrementally
        """
        consumer = self.create_cdc_consumer(topics)
        
        logger.info("Starting CDC processing...")
        
        try:
            for message in consumer:
                event = message.value
                
                # Parse CDC event
                parsed = self.parse_cdc_event(event)
                operation = parsed['operation']
                
                # Extract record ID (customize based on your schema)
                record_id = parsed.get('after', {}).get('id') or \
                           parsed.get('before', {}).get('id') or \
                           f"record_{self.update_count}"
                
                # Process based on operation type
                if operation == 'c':  # CREATE
                    self.process_create_operation(record_id, parsed['after'])
                
                elif operation == 'u':  # UPDATE
                    self.process_update_operation(
                        record_id,
                        parsed['before'],
                        parsed['after']
                    )
                
                elif operation == 'd':  # DELETE
                    self.process_delete_operation(record_id)
                
                # Log statistics every 50 updates
                if self.update_count % 50 == 0:
                    stats = self.get_statistics()
                    logger.info(f"Statistics: {stats}")
        
        except KeyboardInterrupt:
            logger.info("CDC processing interrupted by user")
        
        finally:
            # Save final models
            self.save_models()
            consumer.close()
            logger.info("CDC Processor closed")


def main():
    """Main function"""
    BOOTSTRAP_SERVERS = ['localhost:9092']
    CDC_TOPICS = ['cdc-changes', 'database-changes']
    
    # Create CDC processor
    processor = CDCProcessor(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    # Run CDC processing
    processor.run_cdc_processor(CDC_TOPICS)


if __name__ == "__main__":
    main()
