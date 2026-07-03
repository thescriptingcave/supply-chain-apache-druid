"""
IoT Telemetry Generator - Generates high-frequency sensor data from supply chain assets
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import random
import math
from .base import BaseEventGenerator
from ..models import IoTTelemetryEvent, DeviceType, LocationType


class IoTTelemetryGenerator(BaseEventGenerator):
    """Generates high-frequency IoT sensor data"""
    
    # Device configurations
    DEVICE_CONFIGS = {
        DeviceType.TEMPERATURE_SENSOR: {
            "metrics": [
                {"name": "temperature", "unit": "celsius", "min": -25, "max": 40, "normal_range": (2, 8)}
            ],
            "reading_interval_seconds": 30,
            "locations": [LocationType.WAREHOUSE, LocationType.COLD_STORAGE, LocationType.TRUCK, LocationType.CONTAINER]
        },
        DeviceType.HUMIDITY_SENSOR: {
            "metrics": [
                {"name": "humidity", "unit": "percent", "min": 0, "max": 100, "normal_range": (40, 70)}
            ],
            "reading_interval_seconds": 60,
            "locations": [LocationType.WAREHOUSE, LocationType.COLD_STORAGE, LocationType.CONTAINER]
        },
        DeviceType.GPS_TRACKER: {
            "metrics": [
                {"name": "latitude", "unit": "degrees", "min": -90, "max": 90, "normal_range": None},
                {"name": "longitude", "unit": "degrees", "min": -180, "max": 180, "normal_range": None},
                {"name": "speed", "unit": "kmh", "min": 0, "max": 120, "normal_range": None},
                {"name": "altitude", "unit": "meters", "min": -100, "max": 5000, "normal_range": None}
            ],
            "reading_interval_seconds": 15,
            "locations": [LocationType.TRUCK, LocationType.CONTAINER]
        },
        DeviceType.VIBRATION_SENSOR: {
            "metrics": [
                {"name": "vibration", "unit": "mm_s", "min": 0, "max": 50, "normal_range": (0, 10)}
            ],
            "reading_interval_seconds": 5,
            "locations": [LocationType.PRODUCTION_LINE]
        },
        DeviceType.WEIGHT_SENSOR: {
            "metrics": [
                {"name": "weight", "unit": "kg", "min": 0, "max": 50000, "normal_range": None}
            ],
            "reading_interval_seconds": 10,
            "locations": [LocationType.WAREHOUSE, LocationType.PRODUCTION_LINE]
        },
        DeviceType.PRESSURE_SENSOR: {
            "metrics": [
                {"name": "pressure", "unit": "bar", "min": 0, "max": 10, "normal_range": (2, 6)}
            ],
            "reading_interval_seconds": 5,
            "locations": [LocationType.PRODUCTION_LINE]
        }
    }
    
    DEVICE_MANUFACTURERS = ["SensorTech", "IoT Solutions", "DataSense", "SmartDevice Co."]
    DEVICE_MODELS = ["ST-100", "IS-200", "DS-300", "SD-400"]
    FIRMWARE_VERSIONS = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Maintain device state for realistic readings
        self.device_states: Dict[str, Dict] = {}
        self._initialize_devices()
    
    def _initialize_devices(self):
        """Initialize device states for consistent readings"""
        device_id = 0
        
        # Warehouse sensors
        for warehouse in self.ref.warehouses.values():
            # Temperature sensors (multiple per warehouse)
            for i in range(random.randint(3, 8)):
                device_id += 1
                dev_id = f"DEV-TEMP-{device_id:04d}"
                base_temp = 4.0 if warehouse.has_cold_storage else random.uniform(18, 25)
                self.device_states[dev_id] = {
                    "device_type": DeviceType.TEMPERATURE_SENSOR,
                    "location_id": warehouse.warehouse_id,
                    "location_type": LocationType.COLD_STORAGE if warehouse.has_cold_storage and i < 2 else LocationType.WAREHOUSE,
                    "location_name": warehouse.name,
                    "base_value": base_temp,
                    "current_value": base_temp,
                    "drift_rate": random.uniform(-0.01, 0.01)
                }
            
            # Humidity sensors
            for i in range(random.randint(2, 4)):
                device_id += 1
                dev_id = f"DEV-HUM-{device_id:04d}"
                base_hum = 85.0 if warehouse.has_cold_storage else random.uniform(40, 60)
                self.device_states[dev_id] = {
                    "device_type": DeviceType.HUMIDITY_SENSOR,
                    "location_id": warehouse.warehouse_id,
                    "location_type": LocationType.WAREHOUSE,
                    "location_name": warehouse.name,
                    "base_value": base_hum,
                    "current_value": base_hum,
                    "drift_rate": random.uniform(-0.05, 0.05)
                }
            
            # Weight sensors
            for i in range(random.randint(2, 5)):
                device_id += 1
                dev_id = f"DEV-WT-{device_id:04d}"
                self.device_states[dev_id] = {
                    "device_type": DeviceType.WEIGHT_SENSOR,
                    "location_id": warehouse.warehouse_id,
                    "location_type": LocationType.WAREHOUSE,
                    "location_name": warehouse.name,
                    "base_value": random.uniform(100, 1000),
                    "current_value": random.uniform(100, 1000),
                    "drift_rate": 0
                }
        
        # Production line sensors
        for line in self.ref.production_lines.values():
            for machine in line.machines:
                # Vibration sensor
                device_id += 1
                dev_id = f"DEV-VIB-{device_id:04d}"
                self.device_states[dev_id] = {
                    "device_type": DeviceType.VIBRATION_SENSOR,
                    "location_id": machine['machine_id'],
                    "location_type": LocationType.PRODUCTION_LINE,
                    "location_name": f"{line.name} - {machine['name']}",
                    "associated_asset_id": machine['machine_id'],
                    "base_value": random.uniform(2, 5),
                    "current_value": random.uniform(2, 5),
                    "drift_rate": random.uniform(-0.1, 0.1)
                }
                
                # Pressure sensor (for applicable machines)
                if machine['type'] in ['filling', 'sealing', 'smt']:
                    device_id += 1
                    dev_id = f"DEV-PRES-{device_id:04d}"
                    self.device_states[dev_id] = {
                        "device_type": DeviceType.PRESSURE_SENSOR,
                        "location_id": machine['machine_id'],
                        "location_type": LocationType.PRODUCTION_LINE,
                        "location_name": f"{line.name} - {machine['name']}",
                        "associated_asset_id": machine['machine_id'],
                        "base_value": random.uniform(3, 5),
                        "current_value": random.uniform(3, 5),
                        "drift_rate": random.uniform(-0.02, 0.02)
                    }
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate an IoT telemetry event"""
        
        # Select random device
        if not self.device_states:
            return None
        
        device_id = random.choice(list(self.device_states.keys()))
        device_state = self.device_states[device_id]
        
        device_type = device_state['device_type']
        config = self.DEVICE_CONFIGS[device_type]
        
        # Select metric (most devices have one primary metric)
        metric_config = random.choice(config['metrics'])
        
        # Generate realistic reading
        value, is_anomaly = self._generate_reading(
            device_state,
            metric_config,
            timestamp
        )
        
        # Determine thresholds
        min_threshold = metric_config['min']
        max_threshold = metric_config['max']
        normal_range = metric_config.get('normal_range')
        
        # Determine if alert
        is_alert = False
        alert_severity = ""
        alert_message = ""
        
        if normal_range:
            if value < normal_range[0]:
                is_alert = True
                alert_severity = "warning" if value > normal_range[0] * 0.9 else "critical"
                alert_message = f"{metric_config['name']} below threshold: {value} {metric_config['unit']}"
            elif value > normal_range[1]:
                is_alert = True
                alert_severity = "warning" if value < normal_range[1] * 1.1 else "critical"
                alert_message = f"{metric_config['name']} above threshold: {value} {metric_config['unit']}"
        
        # For GPS trackers, get shipment data
        associated_asset_id = device_state.get('associated_asset_id', '')
        if device_type == DeviceType.GPS_TRACKER:
            # Find a shipment to associate with
            active_shipments = self.state.get_active_shipments_for_update()
            if active_shipments:
                shipment = random.choice(active_shipments)
                associated_asset_id = shipment.shipment_id
                device_state['location_id'] = shipment.vehicle_id
                device_state['location_type'] = LocationType.TRUCK
                device_state['location_name'] = f"Vehicle {shipment.vehicle_id}"
                
                if metric_config['name'] == 'latitude':
                    value = shipment.current_latitude + random.uniform(-0.001, 0.001)
                elif metric_config['name'] == 'longitude':
                    value = shipment.current_longitude + random.uniform(-0.001, 0.001)
                elif metric_config['name'] == 'speed':
                    value = random.uniform(0, 100)
                elif metric_config['name'] == 'altitude':
                    value = random.uniform(0, 500)
        
        # Battery level (slowly decreases)
        battery = max(10, 100 - random.uniform(0, 0.1))
        
        # Signal strength (varies)
        signal = self.normal_distribution(-60, 10, -100, -30)
        
        # Build event
        event = IoTTelemetryEvent(
            timestamp=timestamp,
            device_id=device_id,
            device_type=device_type,
            device_manufacturer=random.choice(self.DEVICE_MANUFACTURERS),
            device_model=random.choice(self.DEVICE_MODELS),
            location_id=device_state['location_id'],
            location_type=device_state['location_type'],
            location_name=device_state['location_name'],
            associated_asset_id=associated_asset_id,
            metric_name=metric_config['name'],
            metric_value=round(value, 3),
            metric_unit=metric_config['unit'],
            min_threshold=min_threshold,
            max_threshold=max_threshold,
            is_anomaly=is_anomaly,
            is_alert=is_alert,
            alert_severity=alert_severity,
            alert_message=alert_message,
            battery_level_percent=round(battery, 1),
            signal_strength_dbm=round(signal, 1),
            firmware_version=random.choice(self.FIRMWARE_VERSIONS)
        )
        
        # Update device state with new value
        device_state['current_value'] = value
        
        result = event.to_druid_dict()
        
        # Inject anomaly if configured
        if self.should_inject_anomaly() and not is_anomaly:
            result = self.inject_anomaly(result, "temperature_spike")
            result['is_alert'] = True
            result['alert_severity'] = 'critical'
            result['metric_value'] = result['metric_value'] * random.uniform(2, 5)
            result['alert_message'] = f"ANOMALY: Abnormal {metric_config['name']} spike detected"
        
        return result
    
    def _generate_reading(self, device_state: Dict, metric_config: Dict, timestamp: datetime) -> Tuple[float, bool]:
        """Generate a realistic sensor reading"""
        base_value = device_state['base_value']
        current_value = device_state['current_value']
        drift_rate = device_state.get('drift_rate', 0)
        
        # Time-based variation (diurnal cycle for temperature)
        hour = timestamp.hour
        time_factor = math.sin((hour - 6) * math.pi / 12) * 2  # +/- 2 units
        
        # Random noise
        noise = random.gauss(0, abs(base_value) * 0.02)  # 2% of base value
        
        # Drift
        drift = drift_rate * random.randint(1, 10)
        
        # Mean reversion (tend back to base)
        mean_reversion = (base_value - current_value) * 0.1
        
        # Calculate new value
        new_value = current_value + drift + mean_reversion + noise + time_factor * 0.1
        
        # Clamp to physical limits
        new_value = max(metric_config['min'], min(metric_config['max'], new_value))
        
        # Check for anomaly
        is_anomaly = False
        if random.random() < 0.001:  # 0.1% natural anomaly rate
            new_value = random.uniform(
                metric_config['min'],
                metric_config['max']
            )
            is_anomaly = True
        
        return round(new_value, 3), is_anomaly
    
    def get_topic_name(self) -> str:
        return "supply_chain.iot_telemetry"