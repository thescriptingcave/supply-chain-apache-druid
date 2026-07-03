"""
Kafka Output Adapter - Publishes events to Kafka topics
"""

import json
import logging
from typing import Any, Dict, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .base import BaseOutputAdapter

logger = logging.getLogger(__name__)


class KafkaOutputAdapter(BaseOutputAdapter):
    """Publishes events to Apache Kafka"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.bootstrap_servers = config.get('bootstrap_servers', 'localhost:9092')
        self.topic_mapping = config.get('topics', {})
        self.acks = config.get('acks', 'all')
        self.compression = config.get('compression', 'snappy')
        self.batch_size = config.get('batch_size', 16384)
        self.linger_ms = config.get('linger_ms', 10)
        
        self.producer: Optional[KafkaProducer] = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=self.acks,
                compression_type=self.compression,
                batch_size=self.batch_size,
                linger_ms=self.linger_ms,
                max_retries=3,
                retry_backoff_ms=100
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send(self, event: Dict[str, Any], event_type: str, key: Optional[str] = None):
        """Send an event to Kafka"""
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        topic = self.topic_mapping.get(event_type)
        if not topic:
            logger.warning(f"No topic mapping for event type: {event_type}")
            return False
        
        try:
            # Use event_id or generated key for partitioning
            if not key:
                key = event.get('event_id', event.get('shipment_id', event.get('order_id', '')))
            
            future = self.producer.send(topic, key=key, value=event)
            
            # Async - could add callback for error handling
            # future.add_callback(self._on_success, topic)
            # future.add_errback(self._on_error, topic)
            
            self._stats['sent_count'] += 1
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send event to {topic}: {e}")
            self._stats['error_count'] += 1
            return False
    
    def flush(self):
        """Flush pending messages"""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.producer = None
            logger.info("Kafka producer closed")
    
    def _on_success(self, record_metadata, topic):
        """Callback for successful send"""
        logger.debug(f"Sent to {topic} at offset {record_metadata.offset}")
    
    def _on_error(self, excp, topic):
        """Callback for failed send"""
        logger.error(f"Failed to send to {topic}: {excp}")
        self._stats['error_count'] += 1