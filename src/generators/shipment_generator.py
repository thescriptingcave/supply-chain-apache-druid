"""
Shipment Event Generator - Generates shipment tracking events with geospatial data
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import random
import math
from .base import BaseEventGenerator
from ..models import ShipmentEvent, ShipmentStatus


class ShipmentEventGenerator(BaseEventGenerator):
    """Generates shipment tracking events including GPS and sensor data"""
    
    # Status transition probabilities
    STATUS_TRANSITIONS = {
        "created": {
            "picked_up": 0.8,
            "cancelled": 0.05
        },
        "picked_up": {
            "in_transit": 0.9,
            "delayed": 0.05
        },
        "in_transit": {
            "at_hub": 0.2,
            "in_transit": 0.6,  # Stay in transit (progress update)
            "out_for_delivery": 0.1,
            "delayed": 0.05,
            "delivered": 0.05
        },
        "at_hub": {
            "in_transit": 0.6,
            "out_for_delivery": 0.35,
            "delayed": 0.05
        },
        "out_for_delivery": {
            "delivered": 0.9,
            "delayed": 0.1
        },
        "delayed": {
            "in_transit": 0.7,
            "at_hub": 0.1,
            "out_for_delivery": 0.1,
            "cancelled": 0.1
        }
    }
    
    DELAY_REASONS = [
        "traffic_congestion",
        "weather_conditions",
        "vehicle_breakdown",
        "loading_delay",
        "customs_hold",
        "port_congestion",
        "driver_unavailable",
        "route_detour",
        "mechanical_issue"
    ]
    
    CITIES = [
        ("New York", 40.7128, -74.0060),
        ("Los Angeles", 34.0522, -118.2437),
        ("Chicago", 41.8781, -87.6298),
        ("Houston", 29.7604, -95.3698),
        ("Phoenix", 33.4484, -112.0740),
        ("Philadelphia", 39.9526, -75.1652),
        ("San Antonio", 29.4241, -98.4936),
        ("San Diego", 32.7157, -117.1611),
        ("Dallas", 32.7767, -96.7970),
        ("San Jose", 37.3382, -121.8863),
        ("Austin", 30.2672, -97.7431),
        ("Jacksonville", 30.3322, -81.6557),
        ("Fort Worth", 32.7555, -97.3308),
        ("Columbus", 39.9612, -82.9988),
        ("Charlotte", 35.2271, -80.8431),
        ("San Francisco", 37.7749, -122.4194),
        ("Indianapolis", 39.7684, -86.1581),
        ("Seattle", 47.6062, -122.3321),
        ("Denver", 39.7392, -104.9903),
        ("Washington", 38.9072, -77.0369),
        ("Boston", 42.3601, -71.0589),
        ("Nashville", 36.1627, -86.7816),
        ("Detroit", 42.3314, -83.0458),
        ("Portland", 45.5152, -122.6784),
        ("Las Vegas", 36.1699, -115.1398),
        ("Miami", 25.7617, -80.1918),
        ("Atlanta", 33.7490, -84.3880),
        ("Minneapolis", 44.9778, -93.2650),
        ("Tampa", 27.9506, -82.4572),
        ("Orlando", 28.5383, -81.3792)
    ]
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate a shipment event - either new shipment or update existing"""
        
        # Decide: create new shipment or update existing
        active_shipments = self.state.get_active_shipments_for_update()
        
        if active_shipments and random.random() < 0.7:
            # Update existing shipment
            return self._generate_status_update(timestamp, random.choice(active_shipments))
        else:
            # Create new shipment
            return self._create_new_shipment(timestamp)
    
    def _create_new_shipment(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Create a new shipment event"""
        # Select origin warehouse
        origin_warehouse = self.ref.get_random_warehouse()
        
        # Select destination (city)
        destination_city = random.choice(self.CITIES)
        destination_id = destination_city[0]
        dest_lat, dest_lon = destination_city[1], destination_city[2]
        
        # Select products to ship
        num_products = random.randint(1, 5)
        products = []
        requires_cold = False
        total_weight = 0
        total_volume = 0
        total_packages = 0
        
        for _ in range(num_products):
            product = self.ref.get_random_product()
            if product.requires_cold_storage:
                requires_cold = True
            qty = random.randint(10, 100)
            products.append({
                "product_id": product.product_id,
                "quantity": qty
            })
            total_weight += qty * product.unit_weight_kg
            total_volume += qty * product.unit_volume_cbm
            total_packages += max(1, qty // 10)
        
        # Select carrier
        carrier = self.ref.get_random_carrier(requires_temp_control=requires_cold)
        
        # Generate IDs
        shipment_id = self.generate_id("SHP")
        order_id = self.generate_id("ORD")
        
        # Create shipment in state
        shipment_state = self.state.create_shipment(
            shipment_id=shipment_id,
            order_id=order_id,
            origin_warehouse_id=origin_warehouse.warehouse_id,
            destination_id=destination_id,
            destination_type="customer",
            carrier_id=carrier.carrier_id,
            products=products,
            is_temperature_controlled=requires_cold
        )
        
        # Get destination region
        dest_region = self._get_region_for_city(destination_id)
        
        # Build event
        event = ShipmentEvent(
            timestamp=timestamp,
            shipment_id=shipment_id,
            order_id=order_id,
            origin_warehouse_id=origin_warehouse.warehouse_id,
            origin_region=origin_warehouse.region,
            destination_id=destination_id,
            destination_type="customer",
            destination_region=dest_region,
            carrier_id=carrier.carrier_id,
            carrier_name=carrier.name,
            vehicle_id=shipment_state.vehicle_id,
            status=ShipmentStatus.CREATED,
            previous_status="",
            latitude=shipment_state.current_latitude,
            longitude=shipment_state.current_longitude,
            temperature_celsius=20.0,
            humidity_percent=50.0,
            speed_kmh=0.0,
            distance_remaining_km=shipment_state.total_distance_km,
            distance_traveled_km=0.0,
            estimated_arrival=shipment_state.estimated_arrival,
            planned_arrival=shipment_state.planned_arrival,
            delay_minutes=0,
            route_id=self.generate_id("RT"),
            number_of_packages=total_packages,
            total_weight_kg=round(total_weight, 2),
            total_volume_cbm=round(total_volume, 4),
            is_temperature_controlled=requires_cold,
            temperature_violation=False
        )
        
        return event.to_druid_dict()
    
    def _generate_status_update(self, timestamp: datetime, shipment: Any) -> Optional[Dict[str, Any]]:
        """Generate a status update event for an existing shipment"""
        current_status = shipment.status
        
        # Get possible next statuses
        transitions = self.STATUS_TRANSITIONS.get(current_status, {})
        if not transitions:
            return None
        
        # Select next status
        next_statuses = list(transitions.keys())
        weights = list(transitions.values())
        next_status = random.choices(next_statuses, weights=weights, k=1)[0]
        
        # Calculate progress
        progress_increment = random.uniform(0.05, 0.20) * shipment.total_distance_km
        new_distance_traveled = min(
            shipment.total_distance_km,
            shipment.distance_traveled_km + progress_increment
        )
        
        # Calculate new position (interpolate between origin and destination)
        progress_ratio = new_distance_traveled / shipment.total_distance_km if shipment.total_distance_km > 0 else 1
        new_lat = shipment.current_latitude + (shipment.destination_latitude - shipment.current_latitude) * progress_increment
        new_lon = shipment.current_longitude + (shipment.destination_longitude - shipment.current_longitude) * progress_increment
        
        # Add some randomness to position
        new_lat += random.uniform(-0.01, 0.01)
        new_lon += random.uniform(-0.01, 0.01)
        
        # Calculate speed (based on distance and time since last update)
        time_since_update = (timestamp - shipment.last_update).total_seconds() / 3600  # hours
        speed = progress_increment / max(0.1, time_since_update) if time_since_update > 0 else 0
        
        # Temperature (varies based on control)
        if shipment.is_temperature_controlled:
            temp = self.normal_distribution(4.0, 1.0, -2.0, 8.0)
            humidity = self.normal_distribution(85.0, 5.0, 70.0, 95.0)
            temp_violation = temp < 0 or temp > 8
        else:
            # Ambient temperature varies by time of day
            hour = timestamp.hour
            base_temp = 20 + 10 * math.sin((hour - 6) * math.pi / 12)
            temp = self.normal_distribution(base_temp, 3.0, -10.0, 45.0)
            humidity = self.normal_distribution(50.0, 15.0, 20.0, 90.0)
            temp_violation = False
        
        # Check for delay
        delay_minutes = shipment.delay_minutes
        delay_reason = ""
        if next_status == "delayed":
            additional_delay = random.randint(30, 480)  # 30 min to 8 hours
            delay_minutes += additional_delay
            delay_reason = random.choice(self.DELAY_REASONS)
        
        # Update state
        self.state.update_shipment(
            shipment.shipment_id,
            next_status,
            distance_traveled_km=new_distance_traveled,
            temperature=temp,
            humidity=humidity,
            delay_minutes=delay_minutes,
            latitude=new_lat,
            longitude=new_lon
        )
        
        # Get carrier info
        carrier = self.ref.carriers.get(shipment.carrier_id)
        carrier_name = carrier.name if carrier else ""
        
        # Build event
        event = ShipmentEvent(
            timestamp=timestamp,
            shipment_id=shipment.shipment_id,
            order_id=shipment.order_id,
            origin_warehouse_id=shipment.origin_warehouse_id,
            destination_id=shipment.destination_id,
            destination_type=shipment.destination_type,
            carrier_id=shipment.carrier_id,
            carrier_name=carrier_name,
            vehicle_id=shipment.vehicle_id,
            status=ShipmentStatus(next_status),
            previous_status=current_status,
            latitude=new_lat,
            longitude=new_lon,
            temperature_celsius=round(temp, 1),
            humidity_percent=round(humidity, 1),
            speed_kmh=round(speed, 1),
            distance_remaining_km=round(shipment.total_distance_km - new_distance_traveled, 1),
            distance_traveled_km=round(new_distance_traveled, 1),
            estimated_arrival=shipment.estimated_arrival,
            planned_arrival=shipment.planned_arrival,
            delay_minutes=delay_minutes,
            delay_reason=delay_reason,
            number_of_packages=sum(p['quantity'] for p in shipment.products),
            is_temperature_controlled=shipment.is_temperature_controlled,
            temperature_violation=temp_violation
        )
        
        result = event.to_druid_dict()
        
        # Inject anomaly if needed
        if self.should_inject_anomaly() and next_status == "in_transit":
            result = self.inject_anomaly(result, "shipment_delay")
            result['delay_minutes'] = delay_minutes + random.randint(720, 1440)
            result['delay_reason'] = "anomaly_major_disruption"
        
        return result
    
    def _get_region_for_city(self, city: str) -> str:
        """Map city to region"""
        northeast_cities = ["New York", "Boston", "Philadelphia", "Washington"]
        southeast_cities = ["Atlanta", "Miami", "Charlotte", "Tampa", "Orlando", "Jacksonville", "Nashville"]
        midwest_cities = ["Chicago", "Detroit", "Indianapolis", "Columbus", "Minneapolis"]
        west_cities = ["Los Angeles", "San Francisco", "Seattle", "Portland", "San Jose", "San Diego", "Las Vegas"]
        southwest_cities = ["Houston", "Dallas", "Phoenix", "San Antonio", "Austin", "Fort Worth", "Denver"]
        
        if city in northeast_cities:
            return "northeast"
        elif city in southeast_cities:
            return "southeast"
        elif city in midwest_cities:
            return "midwest"
        elif city in west_cities:
            return "west"
        elif city in southwest_cities:
            return "southwest"
        return "unknown"
    
    def get_topic_name(self) -> str:
        return "supply_chain.shipment_events"