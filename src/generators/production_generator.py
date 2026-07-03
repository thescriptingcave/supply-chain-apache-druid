"""
Production Event Generator - Generates manufacturing/production events
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import random
from .base import BaseEventGenerator
from ..models import ProductionEvent, ProductionEventType


class ProductionEventGenerator(BaseEventGenerator):
    """Generates production/manufacturing events with OEE metrics"""
    
    DOWNTIME_REASONS = [
        "equipment_malfunction",
        "material_shortage",
        "changeover",
        "maintenance_scheduled",
        "quality_hold",
        "operator_break",
        "power_outage",
        "calibration"
    ]
    
    SHIFT_IDS = ["SHIFT-A", "SHIFT-B", "SHIFT-C"]
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate a production event"""
        
        # Decide: create new production order or update existing
        active_orders = list(self.state.active_production_orders.values())
        
        if active_orders and random.random() < 0.8:
            # Update existing production order
            order = random.choice(active_orders)
            return self._generate_production_update(timestamp, order)
        else:
            # Create new production order
            return self._create_production_order(timestamp)
    
    def _create_production_order(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Create a new production order"""
        # Select production line
        line = self.ref.get_random_production_line()
        
        # Select product from line's products
        product_id = random.choice(line.products)
        product = self.ref.products.get(product_id)
        if not product:
            return None
        
        # Calculate target quantity
        base_quantity = random.randint(500, 5000)
        seasonal_mult = self.get_seasonal_multiplier(timestamp)
        target_quantity = int(base_quantity * seasonal_mult)
        
        # Generate IDs
        order_id = self.generate_id("PO")  # Production Order
        batch_id = self.generate_id("BATCH")
        
        # Select machine
        machine = random.choice(line.machines)
        
        # Get shift
        hour = timestamp.hour
        if 6 <= hour < 14:
            shift_id = "SHIFT-A"
        elif 14 <= hour < 22:
            shift_id = "SHIFT-B"
        else:
            shift_id = "SHIFT-C"
        
        # Create state
        order_state = self.state.create_production_order(
            order_id=order_id,
            product_id=product_id,
            production_line_id=line.line_id,
            target_quantity=target_quantity
        )
        order_state.current_batch_id = batch_id
        order_state.batch_size = 100
        
        # Build event
        event = ProductionEvent(
            timestamp=timestamp,
            production_order_id=order_id,
            product_id=product_id,
            product_category=product.category,
            production_line_id=line.line_id,
            production_line_name=line.name,
            machine_id=machine['machine_id'],
            machine_name=machine['name'],
            event_type=ProductionEventType.ORDER_STARTED,
            batch_id=batch_id,
            batch_size=100,
            quantity_produced=0,
            quantity_defective=0,
            defect_rate=0.0,
            cycle_time_seconds=0.0,
            target_cycle_time_seconds=3600 / line.capacity_per_hour,
            downtime_minutes=0.0,
            oee_availability=1.0,
            oee_performance=1.0,
            oee_quality=1.0,
            oee_overall=1.0,
            machine_temperature=20.0,
            machine_pressure=4.0,
            machine_vibration=3.0,
            energy_consumption_kwh=0.0,
            operator_id=f"OP-{random.randint(100, 999)}",
            shift_id=shift_id,
            quality_score=1.0,
            quality_check_passed=True
        )
        
        return event.to_druid_dict()
    
    def _generate_production_update(self, timestamp: datetime, order: Any) -> Optional[Dict[str, Any]]:
        """Generate a production update event"""
        
        # Determine event type
        event_type = self._determine_event_type(order)
        
        # Get line and machine info
        line = self.ref.production_lines.get(order.production_line_id)
        if not line:
            return None
        
        machine = random.choice(line.machines)
        product = self.ref.products.get(order.product_id)
        
        # Calculate production quantities
        batch_size = order.batch_size
        if event_type == ProductionEventType.BATCH_COMPLETED:
            # Calculate produced quantity with some variance
            efficiency = random.uniform(0.85, 1.05)
            quantity_produced = int(batch_size * efficiency)
            
            # Calculate defects
            defect_rate = random.uniform(0.01, 0.05)  # 1-5% defect rate
            if self.should_inject_anomaly():
                defect_rate = random.uniform(0.1, 0.3)  # 10-30% for anomaly
            quantity_defective = int(quantity_produced * defect_rate)
            
            # Calculate cycle time
            target_cycle = 3600 / line.capacity_per_hour
            cycle_time = target_cycle * random.uniform(0.9, 1.2)
            
            # Calculate OEE
            oee_availability = random.uniform(0.85, 0.98)
            oee_performance = target_cycle / cycle_time
            oee_quality = 1 - defect_rate
            oee_overall = oee_availability * oee_performance * oee_quality
            
            # Energy consumption
            energy = quantity_produced * random.uniform(0.05, 0.15)
            
            # Quality score
            quality_score = oee_quality * 100
            quality_passed = quality_score > 90
            
            # Update state
            self.state.update_production_order(
                order.order_id,
                quantity_produced,
                quantity_defective,
                oee_availability,
                oee_performance,
                oee_quality
            )
            
            # New batch ID for next batch
            new_batch_id = self.generate_id("BATCH")
            order.current_batch_id = new_batch_id
            
        elif event_type == ProductionEventType.MACHINE_DOWNTIME:
            quantity_produced = 0
            quantity_defective = 0
            defect_rate = 0.0
            cycle_time = 0.0
            oee_availability = order.oee_availability * 0.9
            oee_performance = order.oee_performance
            oee_quality = order.oee_quality
            oee_overall = oee_availability * oee_performance * oee_quality
            energy = 0.0
            quality_score = order.oee_quality * 100
            quality_passed = True
            batch_size = 0
        
        elif event_type == ProductionEventType.QUALITY_CHECK:
            quantity_produced = 0
            quantity_defective = 0
            defect_rate = order.defective_quantity / max(1, order.produced_quantity)
            cycle_time = 0.0
            oee_availability = order.oee_availability
            oee_performance = order.oee_performance
            oee_quality = 1 - defect_rate
            oee_overall = oee_availability * oee_performance * oee_quality
            energy = 0.0
            quality_score = oee_quality * 100
            quality_passed = quality_score > 90
            batch_size = 0
        
        else:  # BATCH_STARTED
            quantity_produced = 0
            quantity_defective = 0
            defect_rate = 0.0
            cycle_time = 0.0
            oee_availability = order.oee_availability
            oee_performance = order.oee_performance
            oee_quality = order.oee_quality
            oee_overall = oee_availability * oee_performance * oee_quality
            energy = 0.0
            quality_score = 100.0
            quality_passed = True
            batch_size = order.batch_size
        
        # Machine sensor readings
        machine_temp = self.normal_distribution(45, 10, 20, 80)
        machine_pressure = self.normal_distribution(4, 0.5, 2, 6)
        machine_vibration = self.normal_distribution(3, 1, 0, 15)
        
        # Downtime info
        downtime_minutes = 0.0
        downtime_reason = ""
        if event_type == ProductionEventType.MACHINE_DOWNTIME:
            downtime_minutes = random.uniform(5, 120)
            downtime_reason = random.choice(self.DOWNTIME_REASONS)
        
        # Get shift
        hour = timestamp.hour
        if 6 <= hour < 14:
            shift_id = "SHIFT-A"
        elif 14 <= hour < 22:
            shift_id = "SHIFT-B"
        else:
            shift_id = "SHIFT-C"
        
        # Build event
        event = ProductionEvent(
            timestamp=timestamp,
            production_order_id=order.order_id,
            product_id=order.product_id,
            product_category=product.category if product else "",
            production_line_id=line.line_id,
            production_line_name=line.name,
            machine_id=machine['machine_id'],
            machine_name=machine['name'],
            event_type=event_type,
            batch_id=order.current_batch_id or "",
            batch_size=batch_size,
            quantity_produced=quantity_produced,
            quantity_defective=quantity_defective,
            defect_rate=round(defect_rate, 4),
            cycle_time_seconds=round(cycle_time, 2),
            target_cycle_time_seconds=round(3600 / line.capacity_per_hour, 2),
            downtime_minutes=round(downtime_minutes, 1),
            downtime_reason=downtime_reason,
            oee_availability=round(oee_availability, 4),
            oee_performance=round(oee_performance, 4),
            oee_quality=round(oee_quality, 4),
            oee_overall=round(oee_overall, 4),
            machine_temperature=round(machine_temp, 1),
            machine_pressure=round(machine_pressure, 2),
            machine_vibration=round(machine_vibration, 2),
            energy_consumption_kwh=round(energy, 2),
            operator_id=f"OP-{random.randint(100, 999)}",
            shift_id=shift_id,
            quality_score=round(quality_score, 1),
            quality_check_passed=quality_passed
        )
        
        result = event.to_druid_dict()
        
        # Inject anomaly if needed
        if self.should_inject_anomaly() and event_type == ProductionEventType.BATCH_COMPLETED:
            result = self.inject_anomaly(result, "quality_failure")
            result['quantity_defective'] = int(quantity_produced * 0.5)
            result['defect_rate'] = 0.5
            result['quality_check_passed'] = False
            result['quality_score'] = 50.0
        
        return result
    
    def _determine_event_type(self, order: Any) -> ProductionEventType:
        """Determine event type based on order state"""
        if order.status == "completed":
            return ProductionEventType.ORDER_COMPLETED
        
        weights = {
            ProductionEventType.BATCH_STARTED: 0.15,
            ProductionEventType.BATCH_COMPLETED: 0.40,
            ProductionEventType.QUALITY_CHECK: 0.20,
            ProductionEventType.MACHINE_DOWNTIME: 0.10,
            ProductionEventType.ORDER_COMPLETED: 0.15 if order.produced_quantity >= order.target_quantity * 0.9 else 0.0
        }
        
        # Filter out zero weights
        choices = [k for k, v in weights.items() if v > 0]
        weight_values = [v for v in weights.values() if v > 0]
        
        return random.choices(choices, weights=weight_values, k=1)[0]
    
    def get_topic_name(self) -> str:
        return "supply_chain.production_events"