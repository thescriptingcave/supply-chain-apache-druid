"""
State Manager - Tracks current state of the supply chain for realistic event generation
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from enum import Enum
import random
import threading
from collections import defaultdict


class ShipmentState(Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ProductionOrderState(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class InventoryState:
    """Current inventory state for a product at a warehouse"""
    product_id: str
    warehouse_id: str
    quantity: int
    safety_stock: int
    reserved_quantity: int = 0
    lot_numbers: List[Dict] = field(default_factory=list)  # [{lot: str, qty: int, expiry: datetime}]
    
    @property
    def available_quantity(self) -> int:
        return self.quantity - self.reserved_quantity
    
    @property
    def stock_status(self) -> str:
        if self.quantity <= 0:
            return "out_of_stock"
        elif self.quantity <= self.safety_stock * 0.5:
            return "critical"
        elif self.quantity <= self.safety_stock:
            return "low"
        elif self.quantity > self.safety_stock * 3:
            return "overstock"
        return "normal"


@dataclass
class ActiveShipment:
    """State of an active shipment"""
    shipment_id: str
    order_id: str
    origin_warehouse_id: str
    destination_id: str
    destination_type: str
    carrier_id: str
    vehicle_id: str
    status: str
    current_latitude: float
    current_longitude: float
    destination_latitude: float
    destination_longitude: float
    total_distance_km: float
    distance_traveled_km: float
    created_at: datetime
    estimated_arrival: datetime
    planned_arrival: datetime
    products: List[Dict] = field(default_factory=list)  # [{product_id, quantity}]
    is_temperature_controlled: bool = False
    current_temperature: float = 20.0
    current_humidity: float = 50.0
    delay_minutes: int = 0
    last_update: datetime = field(default_factory=datetime.now)
    
    @property
    def distance_remaining_km(self) -> float:
        return max(0, self.total_distance_km - self.distance_traveled_km)
    
    @property
    def progress_percent(self) -> float:
        if self.total_distance_km == 0:
            return 100
        return min(100, (self.distance_traveled_km / self.total_distance_km) * 100)


@dataclass
class ProductionOrderState:
    """State of an active production order"""
    order_id: str
    product_id: str
    production_line_id: str
    target_quantity: int
    produced_quantity: int
    defective_quantity: int
    status: str
    started_at: datetime
    estimated_completion: datetime
    current_batch_id: Optional[str] = None
    batch_size: int = 100
    oee_availability: float = 0.0
    oee_performance: float = 0.0
    oee_quality: float = 0.0
    last_update: datetime = field(default_factory=datetime.now)


@dataclass
class PurchaseOrderState:
    """State of an active purchase order"""
    po_id: str
    supplier_id: str
    material_id: str
    quantity_ordered: int
    quantity_received: int
    status: str
    created_at: datetime
    promised_delivery: datetime
    actual_delivery: Optional[datetime] = None
    unit_price: float = 0.0


class StateManager:
    """
    Manages the stateful aspects of the supply chain simulation.
    Ensures events are consistent and realistic.
    """
    
    def __init__(self, reference_data):
        self.ref = reference_data
        
        # Inventory state: {(product_id, warehouse_id): InventoryState}
        self.inventory: Dict[tuple, InventoryState] = {}
        
        # Active shipments
        self.active_shipments: Dict[str, ActiveShipment] = {}
        self.completed_shipment_ids: Set[str] = set()
        
        # Production orders
        self.active_production_orders: Dict[str, ProductionOrderState] = {}
        
        # Purchase orders
        self.active_purchase_orders: Dict[str, PurchaseOrderState] = {}
        
        # Vehicle assignments
        self.vehicle_assignments: Dict[str, str] = {}  # vehicle_id -> shipment_id
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        # Initialize inventory
        self._initialize_inventory()

    def _initialize_inventory(self):
        """Initialize inventory levels based on warehouse capacity and products"""
        for warehouse in self.ref.warehouses.values():
            for product in self.ref.products.values():
                # Skip cold storage products for warehouses without cold storage
                if product.requires_cold_storage and not warehouse.has_cold_storage:
                    continue
                
                # Calculate initial stock based on daily demand estimate and safety stock days
                daily_demand_estimate = random.randint(10, 100)
                initial_stock = daily_demand_estimate * product.safety_stock_days
                
                # Add some randomness
                initial_stock = int(initial_stock * random.uniform(0.7, 1.3))
                
                safety_stock = daily_demand_estimate * product.safety_stock_days // 2
                
                key = (product.product_id, warehouse.warehouse_id)
                self.inventory[key] = InventoryState(
                    product_id=product.product_id,
                    warehouse_id=warehouse.warehouse_id,
                    quantity=initial_stock,
                    safety_stock=safety_stock
                )

    def get_inventory(self, product_id: str, warehouse_id: str) -> Optional[InventoryState]:
        """Get current inventory state"""
        return self.inventory.get((product_id, warehouse_id))

    def get_inventory_status(self, product_id: str, warehouse_id: str) -> str:
        """Get stock status string"""
        inv = self.get_inventory(product_id, warehouse_id)
        return inv.stock_status if inv else "out_of_stock"

    def update_inventory(
        self,
        product_id: str,
        warehouse_id: str,
        quantity_change: int,
        event_type: str
    ) -> InventoryState:
        """Update inventory and return new state"""
        with self._lock:
            key = (product_id, warehouse_id)
            if key not in self.inventory:
                self.inventory[key] = InventoryState(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=0,
                    safety_stock=50
                )
            
            inv = self.inventory[key]
            inv.quantity += quantity_change
            
            # Ensure quantity doesn't go negative (for stock_out, just mark as 0)
            if inv.quantity < 0:
                inv.quantity = 0
            
            return inv

    def reserve_inventory(self, product_id: str, warehouse_id: str, quantity: int) -> bool:
        """Reserve inventory for an order. Returns True if successful."""
        with self._lock:
            inv = self.get_inventory(product_id, warehouse_id)
            if inv and inv.available_quantity >= quantity:
                inv.reserved_quantity += quantity
                return True
            return False

    def release_reservation(self, product_id: str, warehouse_id: str, quantity: int):
        """Release reserved inventory (e.g., if order cancelled)"""
        with self._lock:
            inv = self.get_inventory(product_id, warehouse_id)
            if inv:
                inv.reserved_quantity = max(0, inv.reserved_quantity - quantity)

    def create_shipment(
        self,
        shipment_id: str,
        order_id: str,
        origin_warehouse_id: str,
        destination_id: str,
        destination_type: str,
        carrier_id: str,
        products: List[Dict],
        is_temperature_controlled: bool = False
    ) -> ActiveShipment:
        """Create a new active shipment"""
        with self._lock:
            # Generate route coordinates
            origin_wh = self.ref.warehouses.get(origin_warehouse_id)
            origin_lat, origin_lon = self._get_location_coordinates(origin_wh.city if origin_wh else "New York")
            
            dest_lat, dest_lon = self._get_location_coordinates(destination_id)
            
            # Calculate distance (simplified)
            total_distance = self._calculate_distance(origin_lat, origin_lon, dest_lat, dest_lon)
            
            carrier = self.ref.carriers.get(carrier_id)
            avg_speed_kmh = 60 if carrier else 60
            
            # Calculate ETA
            travel_hours = total_distance / avg_speed_kmh
            now = datetime.now()
            estimated_arrival = now + timedelta(hours=travel_hours)
            planned_arrival = estimated_arrival
            
            # Assign vehicle
            vehicle_id = f"VEH-{random.randint(10000, 99999)}"
            
            shipment = ActiveShipment(
                shipment_id=shipment_id,
                order_id=order_id,
                origin_warehouse_id=origin_warehouse_id,
                destination_id=destination_id,
                destination_type=destination_type,
                carrier_id=carrier_id,
                vehicle_id=vehicle_id,
                status="created",
                current_latitude=origin_lat,
                current_longitude=origin_lon,
                destination_latitude=dest_lat,
                destination_longitude=dest_lon,
                total_distance_km=total_distance,
                distance_traveled_km=0,
                created_at=now,
                estimated_arrival=estimated_arrival,
                planned_arrival=planned_arrival,
                products=products,
                is_temperature_controlled=is_temperature_controlled
            )
            
            self.active_shipments[shipment_id] = shipment
            self.vehicle_assignments[vehicle_id] = shipment_id
            
            return shipment

    def update_shipment(
        self,
        shipment_id: str,
        new_status: str,
        distance_traveled_km: Optional[float] = None,
        temperature: Optional[float] = None,
        humidity: Optional[float] = None,
        delay_minutes: Optional[int] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None
    ) -> Optional[ActiveShipment]:
        """Update shipment state"""
        with self._lock:
            shipment = self.active_shipments.get(shipment_id)
            if not shipment:
                return None
            
            shipment.status = new_status
            shipment.last_update = datetime.now()
            
            if distance_traveled_km is not None:
                shipment.distance_traveled_km = distance_traveled_km
            if temperature is not None:
                shipment.current_temperature = temperature
            if humidity is not None:
                shipment.current_humidity = humidity
            if delay_minutes is not None:
                shipment.delay_minutes = delay_minutes
                shipment.estimated_arrival = shipment.planned_arrival + timedelta(minutes=delay_minutes)
            if latitude is not None:
                shipment.current_latitude = latitude
            if longitude is not None:
                shipment.current_longitude = longitude
            
            # Check if completed
            if new_status in ["delivered", "cancelled"]:
                self.completed_shipment_ids.add(shipment_id)
                if shipment.vehicle_id in self.vehicle_assignments:
                    del self.vehicle_assignments[shipment.vehicle_id]
                # Keep in active for a bit before cleanup
                # del self.active_shipments[shipment_id]
            
            return shipment

    def get_active_shipments_for_update(self) -> List[ActiveShipment]:
        """Get shipments that need status updates"""
        with self._lock:
            return [
                s for s in self.active_shipments.values()
                if s.status not in ["delivered", "cancelled"]
            ]

    def create_production_order(
        self,
        order_id: str,
        product_id: str,
        production_line_id: str,
        target_quantity: int
    ) -> ProductionOrderState:
        """Create a new production order"""
        with self._lock:
            line = self.ref.production_lines.get(production_line_id)
            capacity_per_hour = line.capacity_per_hour if line else 100
            
            now = datetime.now()
            hours_needed = target_quantity / capacity_per_hour
            estimated_completion = now + timedelta(hours=hours_needed)
            
            order = ProductionOrderState(
                order_id=order_id,
                product_id=product_id,
                production_line_id=production_line_id,
                target_quantity=target_quantity,
                produced_quantity=0,
                defective_quantity=0,
                status="in_progress",
                started_at=now,
                estimated_completion=estimated_completion,
                batch_size=100
            )
            
            self.active_production_orders[order_id] = order
            return order

    def update_production_order(
        self,
        order_id: str,
        quantity_produced: int,
        quantity_defective: int = 0,
        oee_availability: float = 0.0,
        oee_performance: float = 0.0,
        oee_quality: float = 0.0
    ) -> Optional[ProductionOrderState]:
        """Update production order state"""
        with self._lock:
            order = self.active_production_orders.get(order_id)
            if not order:
                return None
            
            order.produced_quantity += quantity_produced
            order.defective_quantity += quantity_defective
            order.oee_availability = oee_availability
            order.oee_performance = oee_performance
            order.oee_quality = oee_quality
            order.last_update = datetime.now()
            
            # Check if completed
            if order.produced_quantity >= order.target_quantity:
                order.status = "completed"
            
            return order

    def create_purchase_order(
        self,
        po_id: str,
        supplier_id: str,
        material_id: str,
        quantity_ordered: int,
        unit_price: float,
        promised_delivery: datetime
    ) -> PurchaseOrderState:
        """Create a new purchase order"""
        with self._lock:
            po = PurchaseOrderState(
                po_id=po_id,
                supplier_id=supplier_id,
                material_id=material_id,
                quantity_ordered=quantity_ordered,
                quantity_received=0,
                status="po_created",
                created_at=datetime.now(),
                promised_delivery=promised_delivery,
                unit_price=unit_price
            )
            
            self.active_purchase_orders[po_id] = po
            return po

    def get_low_stock_items(self, region: Optional[str] = None) -> List[Dict]:
        """Get items with low stock levels"""
        low_stock = []
        for (product_id, warehouse_id), inv in self.inventory.items():
            if region:
                wh = self.ref.warehouses.get(warehouse_id)
                if not wh or wh.region != region:
                    continue
            if inv.stock_status in ["low", "critical", "out_of_stock"]:
                low_stock.append({
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "quantity": inv.quantity,
                    "safety_stock": inv.safety_stock,
                    "status": inv.stock_status
                })
        return low_stock

    def get_inventory_summary(self) -> Dict:
        """Get summary of inventory state"""
        status_counts = defaultdict(int)
        total_value = 0.0
        total_items = 0
        
        for (product_id, warehouse_id), inv in self.inventory.items():
            status_counts[inv.stock_status] += 1
            total_items += inv.quantity
            product = self.ref.products.get(product_id)
            if product:
                total_value += inv.quantity * product.unit_cost
        
        return {
            "total_sku_locations": len(self.inventory),
            "status_distribution": dict(status_counts),
            "total_inventory_value": total_value,
            "total_inventory_items": total_items,
            "active_shipments": len(self.active_shipments),
            "active_production_orders": len(self.active_production_orders),
            "active_purchase_orders": len(self.active_purchase_orders)
        }

    def _get_location_coordinates(self, location: str) -> tuple:
        """Get approximate coordinates for a location"""
        # Simplified coordinate lookup
        coordinates = {
            "New York": (40.7128, -74.0060),
            "Atlanta": (33.7490, -84.3880),
            "Chicago": (41.8781, -87.6298),
            "Los Angeles": (34.0522, -118.2437),
            "Dallas": (32.7767, -96.7970),
            "Miami": (25.7617, -80.1918),
            "Seattle": (47.6062, -122.3321),
            "Boston": (42.3601, -71.0589),
            "Denver": (39.7392, -104.9903),
            "San Francisco": (37.7749, -122.4194),
        }
        
        for city, coords in coordinates.items():
            if city.lower() in location.lower():
                return coords
        
        # Default with some randomization
        return (39.8283 + random.uniform(-10, 10), -98.5795 + random.uniform(-20, 20))

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate approximate distance in km using Haversine formula"""
        import math
        
        R = 6371  # Earth's radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = math.sin(delta_lat / 2) ** 2 + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c

    def cleanup_completed_entities(self, max_age_hours: int = 24):
        """Clean up old completed entities to prevent memory growth"""
        with self._lock:
            cutoff = datetime.now() - timedelta(hours=max_age_hours)
            
            # Clean up completed shipments
            to_remove = [
                sid for sid, s in self.active_shipments.items()
                if s.status in ["delivered", "cancelled"] and s.last_update < cutoff
            ]
            for sid in to_remove:
                del self.active_shipments[sid]
            
            # Clean up completed production orders
            to_remove = [
                oid for oid, o in self.active_production_orders.items()
                if o.status == "completed" and o.last_update < cutoff
            ]
            for oid in to_remove:
                del self.active_production_orders[oid]
            
            # Clean up completed purchase orders
            to_remove = [
                pid for pid, p in self.active_purchase_orders.items()
                if p.status == "received" and (p.actual_delivery or datetime.min) < cutoff
            ]
            for pid in to_remove:
                del self.active_purchase_orders[pid]