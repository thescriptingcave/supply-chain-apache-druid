"""
Supply Chain Data Models for Apache Druid Ingestion
These models represent the schema for each event type that will be ingested into Druid.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import json
import uuid


class InventoryEventType(str, Enum):
    STOCK_IN = "stock_in"
    STOCK_OUT = "stock_out"
    ADJUSTMENT = "adjustment"
    TRANSFER = "transfer"
    CYCLE_COUNT = "cycle_count"
    EXPIRY_REMOVAL = "expiry_removal"


class ShipmentStatus(str, Enum):
    CREATED = "created"
    PICKED_UP = "picked_up"
    IN_TRANSIT = "in_transit"
    AT_HUB = "at_hub"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED = "delivered"
    DELAYED = "delayed"
    CANCELLED = "cancelled"


class ProductionEventType(str, Enum):
    ORDER_STARTED = "order_started"
    BATCH_STARTED = "batch_started"
    BATCH_COMPLETED = "batch_completed"
    QUALITY_CHECK = "quality_check"
    ORDER_COMPLETED = "order_completed"
    MACHINE_DOWNTIME = "machine_downtime"
    REJECTION = "rejection"


class SupplierEventType(str, Enum):
    PO_CREATED = "po_created"
    PO_ACKNOWLEDGED = "po_acknowledged"
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    CUSTOMS_CLEARED = "customs_cleared"
    RECEIVED = "received"
    QUALITY_CHECKED = "quality_checked"
    REJECTED = "rejected"


class DemandChannel(str, Enum):
    ONLINE = "online"
    RETAIL = "retail"
    WHOLESALE = "wholesale"
    B2B = "b2b"


class DeviceType(str, Enum):
    TEMPERATURE_SENSOR = "temperature_sensor"
    HUMIDITY_SENSOR = "humidity_sensor"
    GPS_TRACKER = "gps_tracker"
    VIBRATION_SENSOR = "vibration_sensor"
    WEIGHT_SENSOR = "weight_sensor"
    PRESSURE_SENSOR = "pressure_sensor"


class LocationType(str, Enum):
    WAREHOUSE = "warehouse"
    TRUCK = "truck"
    CONTAINER = "container"
    PRODUCTION_LINE = "production_line"
    COLD_STORAGE = "cold_storage"
    PORT = "port"


@dataclass
class InventoryEvent:
    """Inventory movement events - optimized for Druid aggregation"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    warehouse_id: str = ""
    warehouse_region: str = ""
    warehouse_type: str = ""
    product_id: str = ""
    product_category: str = ""
    product_subcategory: str = ""
    sku: str = ""
    event_type: InventoryEventType = InventoryEventType.STOCK_IN
    quantity_change: int = 0
    unit_cost: float = 0.0
    total_value: float = 0.0
    current_stock_level: int = 0
    safety_stock_level: int = 0
    stock_status: str = ""  # normal, low, critical, overstock
    zone_id: str = ""
    aisle_id: str = ""
    shelf_id: str = ""
    lot_number: str = ""
    expiry_date: Optional[datetime] = None
    supplier_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    sales_order_id: Optional[str] = None
    transfer_destination: Optional[str] = None
    reason_code: str = ""
    operator_id: str = ""
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['event_type'] = self.event_type.value
        d['expiry_date'] = self.expiry_date.isoformat() if self.expiry_date else None
        return d


@dataclass
class ShipmentEvent:
    """Shipment tracking events with geospatial data"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    shipment_id: str = ""
    order_id: str = ""
    origin_warehouse_id: str = ""
    origin_region: str = ""
    destination_id: str = ""
    destination_type: str = ""  # warehouse, store, customer
    destination_region: str = ""
    carrier_id: str = ""
    carrier_name: str = ""
    vehicle_id: str = ""
    driver_id: str = ""
    status: ShipmentStatus = ShipmentStatus.CREATED
    previous_status: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    temperature_celsius: float = 0.0
    humidity_percent: float = 0.0
    speed_kmh: float = 0.0
    distance_remaining_km: float = 0.0
    distance_traveled_km: float = 0.0
    estimated_arrival: Optional[datetime] = None
    planned_arrival: Optional[datetime] = None
    delay_minutes: int = 0
    delay_reason: str = ""
    route_id: str = ""
    number_of_packages: int = 0
    total_weight_kg: float = 0.0
    total_volume_cbm: float = 0.0
    is_temperature_controlled: bool = False
    temperature_violation: bool = False
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['status'] = self.status.value
        d['estimated_arrival'] = self.estimated_arrival.isoformat() if self.estimated_arrival else None
        d['planned_arrival'] = self.planned_arrival.isoformat() if self.planned_arrival else None
        return d


@dataclass
class ProductionEvent:
    """Production/manufacturing events with OEE metrics"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    production_order_id: str = ""
    product_id: str = ""
    product_category: str = ""
    production_line_id: str = ""
    production_line_name: str = ""
    machine_id: str = ""
    machine_name: str = ""
    event_type: ProductionEventType = ProductionEventType.ORDER_STARTED
    batch_id: str = ""
    batch_size: int = 0
    quantity_produced: int = 0
    quantity_defective: int = 0
    defect_rate: float = 0.0
    cycle_time_seconds: float = 0.0
    target_cycle_time_seconds: float = 0.0
    downtime_minutes: float = 0.0
    downtime_reason: str = ""
    oee_availability: float = 0.0
    oee_performance: float = 0.0
    oee_quality: float = 0.0
    oee_overall: float = 0.0
    machine_temperature: float = 0.0
    machine_pressure: float = 0.0
    machine_vibration: float = 0.0
    energy_consumption_kwh: float = 0.0
    operator_id: str = ""
    shift_id: str = ""
    quality_score: float = 0.0
    quality_check_passed: bool = True
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['event_type'] = self.event_type.value
        return d


@dataclass
class DemandEvent:
    """Customer demand/order events for forecasting"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str = ""
    order_line_id: str = ""
    customer_id: str = ""
    customer_segment: str = ""
    customer_tier: str = ""
    channel: DemandChannel = DemandChannel.ONLINE
    product_id: str = ""
    product_category: str = ""
    product_subcategory: str = ""
    sku: str = ""
    quantity_ordered: int = 0
    unit_price: float = 0.0
    line_total: float = 0.0
    discount_percent: float = 0.0
    discount_amount: float = 0.0
    net_amount: float = 0.0
    region: str = ""
    city: str = ""
    fulfillment_warehouse_id: str = ""
    fulfillment_warehouse_region: str = ""
    promotion_id: Optional[str] = None
    promotion_type: str = ""
    is_recurring: bool = False
    order_priority: str = ""  # standard, expedited, emergency
    requested_delivery_date: Optional[datetime] = None
    customer_lead_time_days: int = 0
    seasonality_factor: float = 1.0
    is_backorder: bool = False
    backorder_quantity: int = 0
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['channel'] = self.channel.value
        d['requested_delivery_date'] = self.requested_delivery_date.isoformat() if self.requested_delivery_date else None
        return d


@dataclass
class SupplierEvent:
    """Supplier/purchase order events"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    purchase_order_id: str = ""
    po_line_id: str = ""
    supplier_id: str = ""
    supplier_name: str = ""
    supplier_region: str = ""
    supplier_tier: str = ""  # strategic, preferred, approved, tactical
    material_id: str = ""
    material_category: str = ""
    event_type: SupplierEventType = SupplierEventType.PO_CREATED
    quantity_ordered: int = 0
    quantity_received: int = 0
    quantity_rejected: int = 0
    unit_price: float = 0.0
    line_total: float = 0.0
    currency: str = "USD"
    promised_lead_time_days: int = 0
    actual_lead_time_days: int = 0
    lead_time_variance_days: int = 0
    quality_score: float = 0.0
    quality_check_passed: bool = True
    defect_rate: float = 0.0
    port_of_origin: str = ""
    port_of_entry: str = ""
    vessel_id: str = ""
    container_id: str = ""
    customs_clearance_hours: float = 0.0
    shipping_method: str = ""  # sea, air, rail, road
    incoterm: str = ""
    on_time_delivery: bool = True
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['event_type'] = self.event_type.value
        return d


@dataclass
class IoTTelemetryEvent:
    """IoT sensor data from supply chain assets"""
    timestamp: datetime
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    device_id: str = ""
    device_type: DeviceType = DeviceType.TEMPERATURE_SENSOR
    device_manufacturer: str = ""
    device_model: str = ""
    location_id: str = ""
    location_type: LocationType = LocationType.WAREHOUSE
    location_name: str = ""
    associated_asset_id: str = ""  # shipment_id, machine_id, etc.
    metric_name: str = ""
    metric_value: float = 0.0
    metric_unit: str = ""
    min_threshold: float = 0.0
    max_threshold: float = 0.0
    is_anomaly: bool = False
    is_alert: bool = False
    alert_severity: str = ""  # info, warning, critical
    alert_message: str = ""
    battery_level_percent: float = 100.0
    signal_strength_dbm: float = 0.0
    firmware_version: str = ""
    raw_payload: str = ""
    
    def to_druid_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['device_type'] = self.device_type.value
        d['location_type'] = self.location_type.value
        return d