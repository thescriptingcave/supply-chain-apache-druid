"""
Supply Chain Data Generator - Main Entry Point
Generates synthetic supply chain data for Apache Druid ingestion
"""

import asyncio
import argparse
import logging
import yaml
from pathlib import Path
from datetime import datetime

from .reference_data import ReferenceDataManager
from .state_manager import StateManager
from .generators.inventory_generator import InventoryEventGenerator
from .generators.shipment_generator import ShipmentEventGenerator
from .generators.production_generator import ProductionEventGenerator
from .generators.demand_generator import DemandEventGenerator
from .generators.supplier_generator import SupplierEventGenerator
from .generators.iot_generator import IoTTelemetryGenerator
from .output_adapters.kafka_adapter import KafkaOutputAdapter
from .output_adapters.file_adapter import FileOutputAdapter
from .scheduler import EventScheduler, GeneratorSchedule


def setup_logging(level: str = "INFO"):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Reduce noise from some libraries
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def load_config(config_path: str) -> dict:
    """Load generator configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_output_adapters(config: dict) -> list:
    """Create output adapters based on configuration"""
    adapters = []
    output_config = config.get('output', {})
    
    # Kafka adapter
    kafka_config = output_config.get('kafka', {})
    if kafka_config.get('enabled', False):
        try:
            adapter = KafkaOutputAdapter(kafka_config)
            adapters.append(adapter)
            logging.info("Kafka output adapter initialized")
        except Exception as e:
            logging.error(f"Failed to initialize Kafka adapter: {e}")
    
    # File adapter
    file_config = output_config.get('file', {})
    if file_config.get('enabled', False):
        adapter = FileOutputAdapter(file_config)
        adapters.append(adapter)
        logging.info("File output adapter initialized")
    
    # Console adapter (for debugging)
    # Could add console adapter here
    
    if not adapters:
        logging.warning("No output adapters enabled! Events will not be persisted.")
    
    return adapters


def create_generators(ref_data: ReferenceDataManager, state_manager: StateManager, config: dict) -> dict:
    """Create all event generators"""
    generators = {}
    rates = config.get('generator', {}).get('rates', {})
    anomaly_config = config.get('generator', {}).get('anomalies', {})
    
    generator_config = {
        'anomalies': anomaly_config
    }
    
    # Inventory events
    generators['inventory_events'] = InventoryEventGenerator(
        ref_data, state_manager, generator_config
    )
    
    # Shipment events
    generators['shipment_events'] = ShipmentEventGenerator(
        ref_data, state_manager, generator_config
    )
    
    # Production events
    generators['production_events'] = ProductionEventGenerator(
        ref_data, state_manager, generator_config
    )
    
    # Demand events
    generators['demand_events'] = DemandEventGenerator(
        ref_data, state_manager, generator_config
    )
    
    # Supplier events
    generators['supplier_events'] = SupplierEventGenerator(
        ref_data, state_manager, generator_config
    )
    
    # IoT telemetry
    generators['iot_telemetry'] = IoTTelemetryGenerator(
        ref_data, state_manager, generator_config
    )
    
    return generators


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Supply Chain Data Generator for Apache Druid'
    )
    parser.add_argument(
        '-c', '--config',
        default='config/generator_config.yaml',
        help='Path to generator configuration file'
    )
    parser.add_argument(
        '-r', '--reference',
        default='config/reference_data.yaml',
        help='Path to reference data configuration file'
    )
    parser.add_argument(
        '-l', '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    parser.add_argument(
        '--inventory-rate',
        type=float,
        help='Override inventory event rate (events/sec)'
    )
    parser.add_argument(
        '--shipment-rate',
        type=float,
        help='Override shipment event rate (events/sec)'
    )
    parser.add_argument(
        '--production-rate',
        type=float,
        help='Override production event rate (events/sec)'
    )
    parser.add_argument(
        '--demand-rate',
        type=float,
        help='Override demand event rate (events/sec)'
    )
    parser.add_argument(
        '--supplier-rate',
        type=float,
        help='Override supplier event rate (events/sec)'
    )
    parser.add_argument(
        '--iot-rate',
        type=float,
        help='Override IoT telemetry rate (events/sec)'
    )
    parser.add_argument(
        '--speed-multiplier',
        type=float,
        help='Override time speed multiplier'
    )
    parser.add_argument(
        '--kafka-only',
        action='store_true',
        help='Only use Kafka output adapter'
    )
    parser.add_argument(
        '--file-only',
        action='store_true',
        help='Only use file output adapter'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("Supply Chain Data Generator for Apache Druid")
    logger.info("="*60)
    
    # Load configuration
    try:
        config = load_config(args.config)
        logger.info(f"Loaded configuration from {args.config}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    # Load reference data
    try:
        ref_data = ReferenceDataManager(args.reference)
        logger.info(f"Loaded reference data: {len(ref_data.warehouses)} warehouses, "
                   f"{len(ref_data.products)} products, {len(ref_data.suppliers)} suppliers")
    except Exception as e:
        logger.error(f"Failed to load reference data: {e}")
        return 1
    
    # Initialize state manager
    state_manager = StateManager(ref_data)
    inv_summary = state_manager.get_inventory_summary()
    logger.info(f"Initialized state: {inv_summary['total_sku_locations']} SKU locations, "
               f"${inv_summary['total_inventory_value']:,.2f} inventory value")
    
    # Create generators
    generators = create_generators(ref_data, state_manager, config)
    logger.info(f"Created {len(generators)} event generators")
    
    # Apply rate overrides
    rates = config.get('generator', {}).get('rates', {})
    if args.inventory_rate:
        rates['inventory_events'] = args.inventory_rate
    if args.shipment_rate:
        rates['shipment_events'] = args.shipment_rate
    if args.production_rate:
        rates['production_events'] = args.production_rate
    if args.demand_rate:
        rates['demand_events'] = args.demand_rate
    if args.supplier_rate:
        rates['supplier_events'] = args.supplier_rate
    if args.iot_rate:
        rates['iot_telemetry'] = args.iot_rate
    
    # Apply speed multiplier override
    if args.speed_multiplier:
        config['generator']['time']['speed_multiplier'] = args.speed_multiplier
    
    # Handle output adapter selection
    if args.kafka_only:
        config['output']['file']['enabled'] = False
    elif args.file_only:
        config['output']['kafka']['enabled'] = False
    
    # Create output adapters
    output_adapters = create_output_adapters(config)
    
    if not output_adapters:
        logger.error("No output adapters could be initialized. Exiting.")
        return 1
    
    # Create schedules
    schedules = []
    for name, generator in generators.items():
        rate = rates.get(name, 1.0)
        schedule = GeneratorSchedule(
            generator_name=name,
            generator_instance=generator,
            base_rate=rate,
            topic_name=generator.get_topic_name()
        )
        schedules.append(schedule)
    
    # Create and start scheduler
    scheduler = EventScheduler(schedules, output_adapters, config.get('generator', {}))
    
    try:
        asyncio.run(scheduler.start())
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())