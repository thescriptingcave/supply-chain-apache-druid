"""
File Output Adapter - Writes events to files for debugging/backup
"""

import json
import gzip
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional
from .base import BaseOutputAdapter

logger = logging.getLogger(__name__)


class FileOutputAdapter(BaseOutputAdapter):
    """Writes events to rotating JSON files"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.output_dir = Path(config.get('output_dir', './data/output'))
        self.format = config.get('format', 'json')
        self.rotate_size_mb = config.get('rotate_size_mb', 100)
        self.compress = config.get('compress', True)
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Track open files and their sizes
        self._files: Dict[str, Any] = {}  # event_type -> file handle
        self._file_sizes: Dict[str, int] = {}
        self._file_counts: Dict[str, int] = {}
        
    def send(self, event: Dict[str, Any], event_type: str, key: Optional[str] = None):
        """Write an event to file"""
        try:
            # Check if we need to rotate
            if self._needs_rotation(event_type):
                self._rotate_file(event_type)
            
            # Open file if not already open
            if event_type not in self._files:
                self._open_file(event_type)
            
            # Write event
            line = json.dumps(event) + '\n'
            self._files[event_type].write(line.encode('utf-8'))
            
            self._file_sizes[event_type] = self._file_sizes.get(event_type, 0) + len(line)
            self._file_counts[event_type] = self._file_counts.get(event_type, 0) + 1
            
            self._stats['sent_count'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to write event to file: {e}")
            self._stats['error_count'] += 1
            return False
    
    def _open_file(self, event_type: str):
        """Open a new file for the event type"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if self.compress:
            filename = f"{event_type}_{timestamp}.json.gz"
            filepath = self.output_dir / filename
            self._files[event_type] = gzip.open(filepath, 'ab')
        else:
            filename = f"{event_type}_{timestamp}.json"
            filepath = self.output_dir / filename
            self._files[event_type] = open(filepath, 'ab')
        
        self._file_sizes[event_type] = 0
        self._file_counts[event_type] = 0
        logger.debug(f"Opened file: {filepath}")
    
    def _needs_rotation(self, event_type: str) -> bool:
        """Check if file needs rotation"""
        if event_type not in self._file_sizes:
            return False
        size_bytes = self._file_sizes[event_type]
        max_bytes = self.rotate_size_mb * 1024 * 1024
        return size_bytes >= max_bytes
    
    def _rotate_file(self, event_type: str):
        """Rotate the current file"""
        if event_type in self._files:
            self._files[event_type].close()
            del self._files[event_type]
        self._open_file(event_type)
        self._stats['rotations'] = self._stats.get('rotations', 0) + 1
    
    def flush(self):
        """Flush all open files"""
        for f in self._files.values():
            f.flush()
    
    def close(self):
        """Close all open files"""
        for event_type, f in self._files.items():
            try:
                f.close()
                logger.info(f"Closed {event_type} file ({self._file_counts.get(event_type, 0)} events)")
            except Exception as e:
                logger.error(f"Error closing {event_type} file: {e}")
        self._files.clear()