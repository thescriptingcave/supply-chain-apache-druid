"""
HTTP Output Adapter - Sends events to Apache Druid via HTTP/REST
Supports both direct ingestion and supervisor-based ingestion
"""

import json
import logging
import asyncio
import aiohttp
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import time

from .base import BaseOutputAdapter

logger = logging.getLogger(__name__)


class DruidIngestionMode(str, Enum):
    """Druid ingestion modes via HTTP"""
    NATIVE_BATCH = "native_batch"          # /druid/indexer/v1/task
    SUPERVISOR = "supervisor"              # /druid/indexer/v1/supervisor
    REALTIME_PUSH = "realtime_push"        # /druid/v2/events (deprecated but still used)
    SQL_INSERT = "sql_insert"              # /druid/v2/sql


@dataclass
class HTTPBatchConfig:
    """Configuration for HTTP batching"""
    enabled: bool = True
    max_batch_size: int = 100              # Max events per batch
    max_batch_bytes: int = 5 * 1024 * 1024 # Max batch size in bytes (5MB)
    flush_interval_seconds: float = 5.0    # Max time between flushes
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    retry_backoff_multiplier: float = 2.0


@dataclass
class BatchStats:
    """Statistics for a single batch"""
    events_sent: int = 0
    bytes_sent: int = 0
    http_status: int = 0
    response_time_ms: float = 0.0
    success: bool = False
    error_message: str = ""


class EventBatch:
    """Accumulates events for batched HTTP submission"""
    
    def __init__(self, event_type: str, config: HTTPBatchConfig):
        self.event_type = event_type
        self.config = config
        self.events: List[Dict[str, Any]] = []
        self.current_size_bytes: int = 0
        self.created_at: float = time.time()
    
    def add_event(self, event: Dict[str, Any]) -> bool:
        """Add an event to the batch. Returns False if batch is full."""
        if not self.config.enabled:
            return True  # No batching, event should be sent immediately
        
        event_bytes = len(json.dumps(event).encode('utf-8'))
        
        # Check if adding this event would exceed limits
        if self.events and (
            len(self.events) >= self.config.max_batch_size or
            self.current_size_bytes + event_bytes > self.config.max_batch_bytes
        ):
            return False  # Batch is full
        
        self.events.append(event)
        self.current_size_bytes += event_bytes
        return True
    
    def is_empty(self) -> bool:
        return len(self.events) == 0
    
    def is_full(self) -> bool:
        return (
            len(self.events) >= self.config.max_batch_size or
            self.current_size_bytes >= self.config.max_batch_bytes
        )
    
    def should_flush(self) -> bool:
        """Check if batch should be flushed based on time"""
        if not self.config.enabled:
            return True
        if self.is_empty():
            return False
        elapsed = time.time() - self.created_at
        return elapsed >= self.config.flush_interval_seconds or self.is_full()
    
    def clear(self):
        """Clear the batch"""
        self.events.clear()
        self.current_size_bytes = 0
        self.created_at = time.time()
    
    def to_json(self) -> str:
        """Convert batch to JSON string"""
        return json.dumps(self.events)


class HTTPOutputAdapter(BaseOutputAdapter):
    """
    HTTP Output Adapter for Apache Druid
    
    Supports multiple ingestion patterns:
    1. Real-time push to /druid/v2/events (simple but deprecated)
    2. Native batch indexing via /druid/indexer/v1/task
    3. Kafka supervisor management via /druid/indexer/v1/supervisor
    
    For production use with Druid, Kafka ingestion is recommended.
    This adapter is useful for:
    - Backfilling historical data
    - Testing and development
    - Low-volume real-time ingestion
    """
    
    # Default Druid dataspec templates
    DEFAULT_DATASPEC_TEMPLATE = {
        "type": "index",
        "spec": {
            "dataSchema": {
                "dataSource": "{datasource}",
                "timestampSpec": {
                    "column": "timestamp",
                    "format": "iso"
                },
                "dimensionsSpec": {
                    "dimensions": [],
                    "dimensionExclusions": [],
                    "includeAllDimensions": True
                },
                "granularitySpec": {
                    "segmentGranularity": "DAY",
                    "queryGranularity": "{query_granularity}",
                    "rollup": True
                },
                "metricsSpec": []
            },
            "ioConfig": {
                "type": "index",
                "firehose": {
                    "type": "inline",
                    "data": "{data}"
                },
                "appendToExisting": False
            },
            "tuningConfig": {
                "type": "index",
                "maxRowsInMemory": 100000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": False
            }
        }
    }
    
    DEFAULT_SUPERVISOR_TEMPLATE = {
        "type": "kafka",
        "dataSchema": {
            "dataSource": "{datasource}",
            "timestampSpec": {
                "column": "timestamp",
                "format": "iso"
            },
            "dimensionsSpec": {
                "dimensions": [],
                "includeAllDimensions": True
            },
            "granularitySpec": {
                "segmentGranularity": "HOUR",
                "queryGranularity": "MINUTE",
                "rollup": True
            },
            "metricsSpec": []
        },
        "ioConfig": {
            "type": "kafka",
            "consumerProperties": {
                "bootstrap.servers": "{kafka_brokers}"
            },
            "topic": "{topic}",
            "inputFormat": {
                "type": "json"
            },
            "taskCount": 2,
            "replicas": 1,
            "taskDuration": "PT1H",
            "startDelay": "PT5S",
            "useEarliestOffset": True
        },
        "tuningConfig": {
            "type": "kafka"
        }
    }
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Connection settings
        self.endpoint = config.get('endpoint', 'http://localhost:8888')
        self.method = config.get('method', 'POST').upper()
        self.headers = config.get('headers', {
            'Content-Type': 'application/json'
        })
        self.timeout = config.get('timeout', 30)
        self.connect_timeout = config.get('connect_timeout', 10)
        
        # Ingestion mode
        mode_str = config.get('mode', 'realtime_push')
        self.mode = DruidIngestionMode(mode_str)
        
        # Authentication
        self.auth_username = config.get('auth', {}).get('username')
        self.auth_password = config.get('auth', {}).get('password')
        self.auth_token = config.get('auth', {}).get('token')
        
        # Batching configuration
        batch_config = config.get('batch', {})
        self.batch_config = HTTPBatchConfig(
            enabled=batch_config.get('enabled', True),
            max_batch_size=batch_config.get('max_batch_size', 100),
            max_batch_bytes=batch_config.get('max_batch_bytes', 5 * 1024 * 1024),
            flush_interval_seconds=batch_config.get('flush_interval_seconds', 5.0),
            max_retries=batch_config.get('max_retries', 3),
            retry_delay_seconds=batch_config.get('retry_delay_seconds', 1.0),
            retry_backoff_multiplier=batch_config.get('retry_backoff_multiplier', 2.0)
        )
        
        # Druid-specific settings
        self.druid_config = config.get('druid', {})
        self.datasource_mapping = self.druid_config.get('datasource_mapping', {
            'inventory_events': 'inventory_events',
            'shipment_events': 'shipment_events',
            'production_events': 'production_events',
            'demand_events': 'demand_events',
            'supplier_events': 'supplier_events',
            'iot_telemetry': 'iot_telemetry'
        })
        self.query_granularity = self.druid_config.get('query_granularity', 'MINUTE')
        self.segment_granularity = self.druid_config.get('segment_granularity', 'DAY')
        self.kafka_brokers = self.druid_config.get('kafka_brokers', 'kafka:9092')
        self.topic_mapping = self.druid_config.get('topic_mapping', {
            'inventory_events': 'supply_chain.inventory_events',
            'shipment_events': 'supply_chain.shipment_events',
            'production_events': 'supply_chain.production_events',
            'demand_events': 'supply_chain.demand_events',
            'supplier_events': 'supply_chain.supplier_events',
            'iot_telemetry': 'supply_chain.iot_telemetry'
        })
        
        # Metrics specs from config
        self.metrics_specs = self.druid_config.get('metrics_specs', {})
        
        # Event batches (one per event type)
        self.batches: Dict[str, EventBatch] = {}
        
        # Session and connector
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        
        # Background flush task
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Batch statistics
        self._batch_stats: List[BatchStats] = []
        
        logger.info(f"HTTP adapter initialized: mode={self.mode}, endpoint={self.endpoint}")
    
    async def _ensure_session(self):
        """Ensure HTTP session is created"""
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=10,
                limit_per_host=5,
                ttl_dns_cache=300,
                enable_cleanup_closed=True
            )
            
            auth = None
            if self.auth_username and self.auth_password:
                auth = aiohttp.BasicAuth(self.auth_username, self.auth_password)
            
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                auth=auth,
                timeout=aiohttp.ClientTimeout(
                    total=self.timeout,
                    connect=self.connect_timeout
                ),
                headers=self.headers
            )
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        headers = dict(self.headers)
        if self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        return headers
    
    def _get_or_create_batch(self, event_type: str) -> EventBatch:
        """Get or create a batch for the event type"""
        if event_type not in self.batches:
            self.batches[event_type] = EventBatch(event_type, self.batch_config)
        return self.batches[event_type]
    
    def send(self, event: Dict[str, Any], event_type: str, key: Optional[str] = None) -> bool:
        """Add event to batch (synchronous wrapper for async send)"""
        # For non-batching mode or immediate send, we'd need an event loop
        # This is a simplified version - in production, use async_send directly
        batch = self._get_or_create_batch(event_type)
        
        if not batch.add_event(event):
            # Batch is full, need to flush
            # In sync context, we can't await, so we'll let the background task handle it
            logger.debug(f"Batch full for {event_type}, will be flushed by background task")
        
        self._stats['queued_count'] = self._stats.get('queued_count', 0) + 1
        return True
    
    async def async_send(self, event: Dict[str, Any], event_type: str, key: Optional[str] = None) -> bool:
        """Async version of send"""
        batch = self._get_or_create_batch(event_type)
        
        if not batch.add_event(event):
            # Batch is full, flush it first
            await self._flush_batch(event_type)
            # Now add the event
            batch.add_event(event)
        
        self._stats['queued_count'] = self._stats.get('queued_count', 0) + 1
        return True
    
    async def start_background_flush(self):
        """Start background flush task"""
        if self._flush_task is None or self._flush_task.done():
            self._running = True
            self._flush_task = asyncio.create_task(self._background_flush_loop())
            logger.info("Started background flush task")
    
    async def stop_background_flush(self):
        """Stop background flush task"""
        self._running = False
        if self._flush_task and not self._flush_task.done():
            # Final flush
            await self.flush_all()
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped background flush task")
    
    async def _background_flush_loop(self):
        """Background loop that flushes batches periodically"""
        while self._running:
            try:
                await asyncio.sleep(self.batch_config.flush_interval_seconds)
                await self.flush_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background flush: {e}")
                await asyncio.sleep(1)
    
    async def flush_all(self):
        """Flush all pending batches"""
        for event_type in list(self.batches.keys()):
            await self._flush_batch(event_type)
    
    async def _flush_batch(self, event_type: str) -> BatchStats:
        """Flush a single batch"""
        batch = self.batches.get(event_type)
        if not batch or batch.is_empty():
            return BatchStats()
        
        stats = BatchStats(events_sent=len(batch.events), bytes_sent=batch.current_size_bytes)
        
        try:
            await self._ensure_session()
            
            if self.mode == DruidIngestionMode.REALTIME_PUSH:
                success = await self._send_realtime_push(batch, stats)
            elif self.mode == DruidIngestionMode.NATIVE_BATCH:
                success = await self._send_native_batch(batch, stats)
            elif self.mode == DruidIngestionMode.SUPERVISOR:
                # Supervisor mode doesn't send events directly, just creates supervisor
                success = True
                logger.warning("Supervisor mode doesn't support direct event ingestion")
            elif self.mode == DruidIngestionMode.SQL_INSERT:
                success = await self._send_sql_insert(batch, stats)
            else:
                success = await self._send_realtime_push(batch, stats)
            
            stats.success = success
            if success:
                self._stats['sent_count'] += stats.events_sent
            else:
                self._stats['error_count'] += stats.events_sent
            
        except Exception as e:
            logger.error(f"Error flushing {event_type} batch: {e}")
            stats.success = False
            stats.error_message = str(e)
            self._stats['error_count'] += stats.events_sent
        
        self._batch_stats.append(stats)
        batch.clear()
        
        return stats
    
    async def _send_realtime_push(self, batch: EventBatch, stats: BatchStats) -> bool:
        """
        Send events via Druid's real-time push endpoint
        POST /druid/v2/events/{datasource}
        """
        datasource = self.datasource_mapping.get(batch.event_type, batch.event_type)
        url = f"{self.endpoint}/druid/v2/events/{datasource}"
        
        start_time = time.time()
        
        for attempt in range(self.batch_config.max_retries):
            try:
                async with self._session.post(
                    url,
                    json=batch.events,
                    headers=self._get_auth_headers()
                ) as response:
                    stats.http_status = response.status
                    stats.response_time_ms = (time.time() - start_time) * 1000
                    
                    if response.status in (200, 202, 204):
                        return True
                    elif response.status in (429, 503):
                        # Rate limited or service unavailable - retry
                        wait_time = self.batch_config.retry_delay_seconds * (
                            self.batch_config.retry_backoff_multiplier ** attempt
                        )
                        logger.warning(f"Rate limited ({response.status}), retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(f"HTTP {response.status} from Druid: {error_text}")
                        stats.error_message = f"HTTP {response.status}: {error_text[:200]}"
                        return False
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt < self.batch_config.max_retries - 1:
                    wait_time = self.batch_config.retry_delay_seconds * (
                        self.batch_config.retry_backoff_multiplier ** attempt
                    )
                    await asyncio.sleep(wait_time)
                continue
            except aiohttp.ClientError as e:
                logger.warning(f"Client error on attempt {attempt + 1}: {e}")
                if attempt < self.batch_config.max_retries - 1:
                    wait_time = self.batch_config.retry_delay_seconds * (
                        self.batch_config.retry_backoff_multiplier ** attempt
                    )
                    await asyncio.sleep(wait_time)
                continue
        
        stats.error_message = f"Max retries ({self.batch_config.max_retries}) exceeded"
        return False
    
    async def _send_native_batch(self, batch: EventBatch, stats: BatchStats) -> bool:
        """
        Send events via Druid's native batch indexing
        POST /druid/indexer/v1/task
        """
        datasource = self.datasource_mapping.get(batch.event_type, batch.event_type)
        url = f"{self.endpoint}/druid/indexer/v1/task"
        
        # Build task spec
        task_id = f"index_{datasource}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{hashlib.md5(json.dumps(batch.events).encode()).hexdigest()[:8]}"
        
        # Build dataspec
        dataspec = json.loads(json.dumps(self.DEFAULT_DATASPEC_TEMPLATE)
            .replace('{datasource}', datasource)
            .replace('{query_granularity}', self.query_granularity)
            .replace('{data}', json.dumps(batch.events))
        )
        
        # Add metrics if configured
        if batch.event_type in self.metrics_specs:
            dataspec['spec']['dataSchema']['metricsSpec'] = self.metrics_specs[batch.event_type]
        
        # Update segment granularity
        dataspec['spec']['dataSchema']['granularitySpec']['segmentGranularity'] = self.segment_granularity
        
        # Build task
        task = {
            "type": "index",
            "id": task_id,
            "spec": dataspec['spec']
        }
        
        start_time = time.time()
        
        for attempt in range(self.batch_config.max_retries):
            try:
                async with self._session.post(
                    url,
                    json=task,
                    headers=self._get_auth_headers()
                ) as response:
                    stats.http_status = response.status
                    stats.response_time_ms = (time.time() - start_time) * 1000
                    
                    if response.status in (200, 201):
                        result = await response.json()
                        task_id = result.get('task', task_id)
                        logger.debug(f"Submitted indexing task: {task_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to submit task: HTTP {response.status}: {error_text}")
                        stats.error_message = f"HTTP {response.status}: {error_text[:200]}"
                        return False
                        
            except Exception as e:
                logger.warning(f"Error submitting task (attempt {attempt + 1}): {e}")
                if attempt < self.batch_config.max_retries - 1:
                    await asyncio.sleep(self.batch_config.retry_delay_seconds)
                continue
        
        return False
    
    async def _send_sql_insert(self, batch: EventBatch, stats: BatchStats) -> bool:
        """
        Send events via Druid SQL INSERT
        POST /druid/v2/sql
        """
        datasource = self.datasource_mapping.get(batch.event_type, batch.event_type)
        url = f"{self.endpoint}/druid/v2/sql"
        
        # Build INSERT statements
        if not batch.events:
            return False
        
        # Get column names from first event
        columns = list(batch.events[0].keys())
        columns_str = ', '.join(f'"{c}"' for c in columns)
        placeholders = ', '.join(['?'] * len(columns))
        
        sql = f'INSERT INTO "{datasource}" ({columns_str}) VALUES '
        values_list = []
        
        for event in batch.events:
            values = []
            for col in columns:
                val = event.get(col)
                if val is None:
                    values.append('NULL')
                elif isinstance(val, bool):
                    values.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    values.append(f"'{escaped}'")
                else:
                    values.append(f"'{json.dumps(val)}'")
            values_list.append(f"({', '.join(values)})")
        
        sql += ', '.join(values_list)
        
        payload = {
            "query": sql,
            "context": {
                "sqlInsertSegmentGranularity": self.segment_granularity
            }
        }
        
        start_time = time.time()
        
        for attempt in range(self.batch_config.max_retries):
            try:
                async with self._session.post(
                    url,
                    json=payload,
                    headers=self._get_auth_headers()
                ) as response:
                    stats.http_status = response.status
                    stats.response_time_ms = (time.time() - start_time) * 1000
                    
                    if response.status in (200, 202):
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"SQL INSERT failed: HTTP {response.status}: {error_text}")
                        stats.error_message = f"HTTP {response.status}: {error_text[:200]}"
                        return False
                        
            except Exception as e:
                logger.warning(f"SQL INSERT error (attempt {attempt + 1}): {e}")
                if attempt < self.batch_config.max_retries - 1:
                    await asyncio.sleep(self.batch_config.retry_delay_seconds)
                continue
        
        return False
    
    async def create_kafka_supervisor(self, event_type: str) -> bool:
        """
        Create a Kafka supervisor for an event type
        POST /druid/indexer/v1/supervisor
        """
        datasource = self.datasource_mapping.get(event_type, event_type)
        topic = self.topic_mapping.get(event_type, f"supply_chain.{event_type}")
        url = f"{self.endpoint}/druid/indexer/v1/supervisor"
        
        # Build supervisor spec
        supervisor_spec = json.loads(json.dumps(self.DEFAULT_SUPERVISOR_TEMPLATE)
            .replace('{datasource}', datasource)
            .replace('{kafka_brokers}', self.kafka_brokers)
            .replace('{topic}', topic)
        )
        
        # Add metrics if configured
        if event_type in self.metrics_specs:
            supervisor_spec['dataSchema']['metricsSpec'] = self.metrics_specs[event_type]
        
        try:
            await self._ensure_session()
            
            async with self._session.post(
                url,
                json=supervisor_spec,
                headers=self._get_auth_headers()
            ) as response:
                if response.status in (200, 201):
                    result = await response.json()
                    logger.info(f"Created supervisor for {datasource}: {result}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create supervisor: HTTP {response.status}: {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error creating supervisor: {e}")
            return False
    
    async def create_all_supervisors(self) -> Dict[str, bool]:
        """Create Kafka supervisors for all event types"""
        results = {}
        for event_type in self.datasource_mapping.keys():
            results[event_type] = await self.create_kafka_supervisor(event_type)
        return results
    
    async def get_supervisor_status(self, event_type: str) -> Optional[Dict]:
        """Get status of a supervisor"""
        datasource = self.datasource_mapping.get(event_type, event_type)
        url = f"{self.endpoint}/druid/indexer/v1/supervisor/{datasource}/status"
        
        try:
            await self._ensure_session()
            
            async with self._session.get(
                url,
                headers=self._get_auth_headers()
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Failed to get supervisor status: HTTP {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting supervisor status: {e}")
            return None
    
    async def suspend_supervisor(self, event_type: str) -> bool:
        """Suspend a supervisor"""
        datasource = self.datasource_mapping.get(event_type, event_type)
        url = f"{self.endpoint}/druid/indexer/v1/supervisor/{datasource}/suspend"
        
        try:
            await self._ensure_session()
            
            async with self._session.post(
                url,
                headers=self._get_auth_headers()
            ) as response:
                return response.status in (200, 202)
                
        except Exception as e:
            logger.error(f"Error suspending supervisor: {e}")
            return False
    
    async def resume_supervisor(self, event_type: str) -> bool:
        """Resume a suspended supervisor"""
        datasource = self.datasource_mapping.get(event_type, event_type)
        url = f"{self.endpoint}/druid/indexer/v1/supervisor/{datasource}/resume"
        
        try:
            await self._ensure_session()
            
            async with self._session.post(
                url,
                headers=self._get_auth_headers()
            ) as response:
                return response.status in (200, 202)
                
        except Exception as e:
            logger.error(f"Error resuming supervisor: {e}")
            return False
    
    async def shutdown_supervisor(self, event_type: str) -> bool:
        """Shutdown and remove a supervisor"""
        datasource = self.datasource_mapping.get(event_type, event_type)
        url = f"{self.endpoint}/druid/indexer/v1/supervisor/{datasource}/shutdown"
        
        try:
            await self._ensure_session()
            
            async with self._session.post(
                url,
                headers=self._get_auth_headers()
            ) as response:
                return response.status in (200, 202)
                
        except Exception as e:
            logger.error(f"Error shutting down supervisor: {e}")
            return False
    
    async def check_druid_health(self) -> bool:
        """Check if Druid is healthy"""
        url = f"{self.endpoint}/status/health"
        
        try:
            await self._ensure_session()
            
            async with self._session.get(
                url,
                headers=self._get_auth_headers()
            ) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    async def get_datasource_metadata(self, datasource: str) -> Optional[Dict]:
        """Get metadata for a datasource"""
        url = f"{self.endpoint}/druid/coordinator/v1/metadata/datasources/{datasource}"
        
        try:
            await self._ensure_session()
            
            async with self._session.get(
                url,
                headers=self._get_auth_headers()
            ) as response:
                if response.status == 200:
                    return await response.json()
                return None
                
        except Exception as e:
            logger.error(f"Error getting datasource metadata: {e}")
            return None
    
    def flush(self):
        """Synchronous flush - queues flush for async handling"""
        # This is a no-op for the sync interface
        # The background task handles flushing
        pass
    
    async def async_flush(self):
        """Async flush"""
        await self.flush_all()
    
    def close(self):
        """Close the adapter"""
        # Schedule cleanup
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self._close_async())
            else:
                loop.run_until_complete(self._close_async())
        except RuntimeError:
            # No event loop, just close session directly
            if self._session and not self._session.closed:
                self._session.close()
    
    async def _close_async(self):
        """Async cleanup"""
        await self.stop_background_flush()
        await self.flush_all()
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        logger.info("HTTP adapter closed")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get adapter statistics including batch stats"""
        stats = super().get_statistics()
        
        # Aggregate batch statistics
        total_batches = len(self._batch_stats)
        successful_batches = sum(1 for s in self._batch_stats if s.success)
        failed_batches = total_batches - successful_batches
        total_events_sent = sum(s.events_sent for s in self._batch_stats)
        total_bytes_sent = sum(s.bytes_sent for s in self._batch_stats)
        avg_response_time = (
            sum(s.response_time_ms for s in self._batch_stats) / total_batches
            if total_batches > 0 else 0
        )
        
        # Current batch sizes
        pending_events = sum(len(b.events) for b in self.batches.values())
        pending_bytes = sum(b.current_size_bytes for b in self.batches.values())
        
        stats.update({
            'mode': self.mode.value,
            'batches': {
                'total': total_batches,
                'successful': successful_batches,
                'failed': failed_batches,
                'success_rate': successful_batches / total_batches if total_batches > 0 else 0
            },
            'events': {
                'total_sent': total_events_sent,
                'pending': pending_events
            },
            'bytes': {
                'total_sent': total_bytes_sent,
                'pending': pending_bytes
            },
            'performance': {
                'avg_response_time_ms': round(avg_response_time, 2)
            },
            'pending_batches': {
                event_type: len(batch.events)
                for event_type, batch in self.batches.items()
                if not batch.is_empty()
            }
        })
        
        return stats


# Convenience function for creating supervisors
async def setup_druid_kafka_ingestion(
    druid_router_url: str = "http://localhost:8888",
    kafka_brokers: str = "kafka:9092",
    topic_prefix: str = "supply_chain."
) -> Dict[str, bool]:
    """
    Convenience function to set up Kafka supervisors in Druid for all event types
    
    Usage:
        results = await setup_druid_kafka_ingestion()
        for event_type, success in results.items():
            print(f"{event_type}: {'✓' if success else '✗'}")
    """
    config = {
        'endpoint': druid_router_url,
        'mode': 'supervisor',
        'druid': {
            'kafka_brokers': kafka_brokers,
            'datasource_mapping': {
                'inventory_events': 'inventory_events',
                'shipment_events': 'shipment_events',
                'production_events': 'production_events',
                'demand_events': 'demand_events',
                'supplier_events': 'supplier_events',
                'iot_telemetry': 'iot_telemetry'
            },
            'topic_mapping': {
                event_type: f"{topic_prefix}{event_type}"
                for event_type in [
                    'inventory_events',
                    'shipment_events',
                    'production_events',
                    'demand_events',
                    'supplier_events',
                    'iot_telemetry'
                ]
            }
        }
    }
    
    adapter = HTTPOutputAdapter(config)
    results = await adapter.create_all_supervisors()
    await adapter._close_async()
    
    return results