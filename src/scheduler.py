"""
Event Scheduler - Manages timing and rate control for event generation
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import random

logger = logging.getLogger(__name__)


@dataclass
class GeneratorSchedule:
    """Schedule configuration for a single generator"""
    generator_name: str
    generator_instance: Any
    base_rate: float  # events per second
    topic_name: str
    enabled: bool = True


class EventScheduler:
    """
    Schedules and coordinates event generation across multiple generators.
    Handles rate limiting, time acceleration, and graceful shutdown.
    """
    
    def __init__(
        self,
        schedules: List[GeneratorSchedule],
        output_adapters: List[Any],
        config: Dict[str, Any]
    ):
        self.schedules = {s.generator_name: s for s in schedules}
        self.output_adapters = output_adapters
        self.config = config
        
        self.time_config = config.get('time', {})
        self.speed_multiplier = self.time_config.get('speed_multiplier', 60)
        self.realtime_mode = self.time_config.get('realtime_mode', True)
        
        self._running = False
        self._start_time: Optional[datetime] = None
        self._simulated_time: Optional[datetime] = None
        self._last_real_time: float = 0
        self._total_events_generated = 0
        self._cleanup_interval = 60  # seconds
        self._last_cleanup = 0
    
    async def start(self):
        """Start the scheduler"""
        logger.info("Starting event scheduler...")
        logger.info(f"  Speed multiplier: {self.speed_multiplier}x")
        logger.info(f"  Real-time mode: {self.realtime_mode}")
        
        for name, schedule in self.schedules.items():
            if schedule.enabled:
                logger.info(f"  {name}: {schedule.base_rate} events/sec -> {schedule.topic_name}")
        
        self._running = True
        self._start_time = datetime.now()
        self._simulated_time = self._start_time
        self._last_real_time = time.time()
        
        try:
            # Create tasks for each generator
            tasks = []
            for name, schedule in self.schedules.items():
                if schedule.enabled:
                    task = asyncio.create_task(
                        self._run_generator(name, schedule),
                        name=name
                    )
                    tasks.append(task)
            
            # Run until stopped
            await asyncio.gather(*tasks)
            
        except asyncio.CancelledError:
            logger.info("Scheduler cancelled")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the scheduler gracefully"""
        logger.info("Stopping scheduler...")
        self._running = False
        
        # Flush all adapters
        for adapter in self.output_adapters:
            adapter.flush()
            adapter.close()
        
        # Print statistics
        self._print_statistics()
    
    async def _run_generator(self, name: str, schedule: GeneratorSchedule):
        """Run a single generator at its configured rate"""
        generator = schedule.generator_instance
        base_rate = schedule.base_rate
        interval = 1.0 / base_rate if base_rate > 0 else 1.0
        
        logger.info(f"Starting {name} generator (interval: {interval:.4f}s)")
        
        while self._running:
            try:
                # Get current simulated time
                timestamp = self._get_current_timestamp()
                
                # Apply seasonality to rate
                seasonal_mult = generator.get_seasonal_multiplier(timestamp)
                adjusted_rate = base_rate * seasonal_mult
                
                # Random variation in rate (±20%)
                rate_variation = random.uniform(0.8, 1.2)
                adjusted_interval = 1.0 / (adjusted_rate * rate_variation)
                
                # Generate event
                event = generator.generate_event(timestamp)
                
                if event:
                    # Send to all output adapters
                    for adapter in self.output_adapters:
                        adapter.send(event, name)

                    #update both scheduler and generator statistics
                    self._total_events_generated += 1
                    generator.event_count += 1
                
                # Wait for next interval
                await asyncio.sleep(adjusted_interval)
                
                # Periodic cleanup
                current_real_time = time.time()
                if current_real_time - self._last_cleanup > self._cleanup_interval:
                    await self._periodic_cleanup()
                    self._last_cleanup = current_real_time
                
            except Exception as e:
                logger.error(f"Error in {name} generator: {e}")
                await asyncio.sleep(1)  # Brief pause before retry
    
    def _get_current_timestamp(self) -> datetime:
        """Get the current simulated timestamp"""
        if self.realtime_mode:
            return datetime.now()
        else:
            # Time acceleration mode
            current_real_time = time.time()
            elapsed_real = current_real_time - self._last_real_time
            elapsed_simulated = timedelta(seconds=elapsed_real * self.speed_multiplier)
            self._simulated_time += elapsed_simulated
            self._last_real_time = current_real_time
            return self._simulated_time
    
    async def _periodic_cleanup(self):
        """Perform periodic cleanup tasks"""
        # Cleanup old completed entities in state
        for name, schedule in self.schedules.items():
            if hasattr(schedule.generator_instance, 'state'):
                schedule.generator_instance.state.cleanup_completed_entities()
        
        # Log statistics
        elapsed = (datetime.now() - self._start_time).total_seconds() if self._start_time else 1
        rate = self._total_events_generated / elapsed
        logger.info(f"Statistics: {self._total_events_generated} events generated, {rate:.1f} events/sec")
        
        # Print per-generator stats
        for name, schedule in self.schedules.items():
            stats = schedule.generator_instance.get_statistics()
            logger.debug(f"  {name}: {stats}")
    
    def _print_statistics(self):
        """Print final statistics"""
        if not self._start_time:
            return
        
        elapsed = (datetime.now() - self._start_time).total_seconds()
        
        print("\n" + "="*60)
        print("GENERATOR STATISTICS")
        print("="*60)
        print(f"Total runtime: {elapsed:.1f} seconds")
        print(f"Total events generated: {self._total_events_generated}")
        print(f"Average rate: {self._total_events_generated / elapsed:.1f} events/sec")
        print()
        
        for name, schedule in self.schedules.items():
            stats = schedule.generator_instance.get_statistics()
            print(f"{name}:")
            print(f"  Events: {stats['event_count']}")
            print(f"  Anomalies: {stats['anomaly_count']}")
            print(f"  Anomaly rate: {stats['anomaly_rate']:.2%}")
            print()
        
        print("\nOutput Adapter Statistics:")
        for adapter in self.output_adapters:
            print(f"  {adapter.__class__.__name__}: {adapter.get_statistics()}")
        
        print("="*60)