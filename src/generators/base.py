"""
Base Event Generator - Abstract base class for all event generators
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
import random
import uuid
import math


class BaseEventGenerator(ABC):
    """Abstract base class for supply chain event generators"""
    
    def __init__(self, reference_data, state_manager, config: Dict[str, Any]):
        self.ref = reference_data
        self.state = state_manager
        self.config = config
        self.anomaly_config = config.get('anomalies', {})
        self.event_count = 0
        self.anomaly_count = 0
    
    @abstractmethod
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate a single event at the given timestamp"""
        pass
    
    @abstractmethod
    def get_topic_name(self) -> str:
        """Return the Kafka topic name for this event type"""
        pass
    
    def generate_batch(self, timestamp: datetime, count: int) -> List[Dict[str, Any]]:
        """Generate a batch of events"""
        events = []
        for _ in range(count):
            event = self.generate_event(timestamp)
            if event:
                events.append(event)
                self.event_count += 1
        return events
    
    def should_inject_anomaly(self) -> bool:
        """Determine if an anomaly should be injected"""
        if not self.anomaly_config.get('enabled', False):
            return False
        return random.random() < self.anomaly_config.get('probability', 0.02)
    
    def inject_anomaly(self, event: Dict[str, Any], anomaly_type: str) -> Dict[str, Any]:
        """Inject an anomaly into an event. Override in subclasses for specific anomaly types."""
        event['_is_anomaly'] = True
        event['_anomaly_type'] = anomaly_type
        self.anomaly_count += 1
        return event
    
    def generate_id(self, prefix: str = "") -> str:
        """Generate a unique ID with optional prefix"""
        if prefix:
            return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"
        return uuid.uuid4().hex[:12].upper()
    
    def normal_distribution(self, mean: float, std_dev: float, min_val: float = None, max_val: float = None) -> float:
        """Generate a value from a normal distribution with optional bounds"""
        value = random.gauss(mean, std_dev)
        if min_val is not None:
            value = max(min_val, value)
        if max_val is not None:
            value = min(max_val, value)
        return value
    
    def weighted_choice(self, choices: List[Any], weights: List[float]) -> Any:
        """Make a weighted random choice"""
        return random.choices(choices, weights=weights, k=1)[0]
    
    def get_seasonal_multiplier(self, timestamp: datetime) -> float:
        """Get seasonality multiplier for demand/cost adjustments"""
        return self.ref.get_seasonality_multiplier(timestamp)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get generator statistics"""
        return {
            "event_count": self.event_count,
            "anomaly_count": self.anomaly_count,
            "anomaly_rate": self.anomaly_count / max(1, self.event_count)
        }