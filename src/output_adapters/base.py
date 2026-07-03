"""
Base Output Adapter - Abstract base class for output adapters
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseOutputAdapter(ABC):
    """
    Abstract base class for event output adapters.
    
    All output adapters (Kafka, HTTP, File) must inherit from this class
    and implement the `send`, `flush`, and `close` methods.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._stats = {
            'sent_count': 0,
            'error_count': 0,
            'queued_count': 0
        }
    
    @abstractmethod
    def send(self, event: Dict[str, Any], event_type: str, key: Optional[str] = None) -> bool:
        """
        Send an event to the output destination.
        
        Args:
            event: The event data as a dictionary
            event_type: Type of event (e.g., 'inventory_events', 'shipment_events')
            key: Optional key for partitioning/ordering (used heavily by Kafka)
            
        Returns:
            True if send was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def flush(self):
        """Flush any buffered events to the destination."""
        pass
    
    @abstractmethod
    def close(self):
        """Close the adapter and release any resources (connections, file handles, etc.)."""
        pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get adapter statistics (sent count, error count, etc.)."""
        return dict(self._stats)
    
    def reset_statistics(self):
        """Reset statistics counters to zero."""
        self._stats = {
            'sent_count': 0,
            'error_count': 0,
            'queued_count': 0
        }