"""
Inventory Event Generator - Generates inventory movement events
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import random
from .base import BaseEventGenerator
from ..models import InventoryEvent, InventoryEventType


class InventoryEventGenerator(BaseEventGenerator):
    """Generates realistic inventory movement events"""
    
    # Reason codes for different event types
    REASON_CODES = {
        InventoryEventType.STOCK_IN: [
            "purchase_order_receipt",
            "production_input",
            "customer_return",
            "transfer_in",
            "correction"
        ],
        InventoryEventType.STOCK_OUT: [
            "sales_order_fulfillment",
            "production_consumption",
            "transfer_out",
            "damage_disposal",
            "theft_loss"
        ],
        InventoryEventType.ADJUSTMENT: [
            "cycle_count_correction",
            "system_correction",
            "damaged_goods",
            "expiry_removal",
            "reclassification"
        ],
        InventoryEventType.TRANSFER: [
            "rebalancing",
            "stock_rotation",
            "zone_transfer",
            "warehouse_transfer"
        ],
        InventoryEventType.CYCLE_COUNT: [
            "scheduled_count",
            "random_count",
            "triggered_count"
        ],
        InventoryEventType.EXPIRY_REMOVAL: [
            "expired_perishable",
            "shelf_life_exceeded",
            "quality_expiry"
        ]
    }
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate an inventory event based on current state and probabilities"""
        
        # Determine event type based on weighted probabilities
        event_type = self._determine_event_type()
        
        # Select warehouse and product
        warehouse = self._select_warehouse(event_type)
        product = self._select_product(warehouse, event_type)
        
        if not warehouse or not product:
            return None
        
        # Get zone
        zone = self._select_zone(warehouse, product, event_type)
        
        # Generate quantities based on event type
        quantity, unit_cost = self._generate_quantity_and_cost(product, event_type, timestamp)
        
        # Update state
        inv_state = self.state.update_inventory(
            product.product_id,
            warehouse.warehouse_id,
            quantity if event_type == InventoryEventType.STOCK_IN else -quantity,
            event_type.value
        )
        
        # Get reason code
        reason_code = random.choice(self.REASON_CODES.get(event_type, ["unknown"]))
        
        # Generate lot number for stock-in events
        lot_number = None
        expiry_date = None
        if event_type == InventoryEventType.STOCK_IN:
            lot_number = f"LOT-{timestamp.strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
            if product.shelf_life_days:
                expiry_date = timestamp + timedelta(days=product.shelf_life_days)
        
        # Check for anomaly
        is_anomaly = self.should_inject_anomaly()
        if is_anomaly:
            quantity = int(quantity * random.uniform(5, 20))  # Abnormal quantity
            reason_code = "anomaly_investigation_required"
        
        # Build event
        event = InventoryEvent(
            timestamp=timestamp,
            warehouse_id=warehouse.warehouse_id,
            warehouse_region=warehouse.region,
            warehouse_type=warehouse.type,
            product_id=product.product_id,
            product_category=product.category,
            product_subcategory=product.subcategory,
            sku=product.sku,
            event_type=event_type,
            quantity_change=quantity if event_type == InventoryEventType.STOCK_IN else -quantity,
            unit_cost=unit_cost,
            total_value=abs(quantity) * unit_cost,
            current_stock_level=inv_state.quantity,
            safety_stock_level=inv_state.safety_stock,
            stock_status=inv_state.stock_status,
            zone_id=zone.get('zone_id', '') if zone else '',
            aisle_id=f"A-{random.randint(1, 20):02d}",
            shelf_id=f"S-{random.randint(1, 50):02d}",
            lot_number=lot_number,
            expiry_date=expiry_date,
            reason_code=reason_code,
            operator_id=f"OP-{random.randint(100, 999)}"
        )
        
        # Add related IDs based on event type
        if event_type == InventoryEventType.STOCK_IN:
            event.supplier_id = product.supplier_id
            event.purchase_order_id = f"PO-{random.randint(100000, 999999)}"
        elif event_type == InventoryEventType.STOCK_OUT:
            event.sales_order_id = f"SO-{random.randint(100000, 999999)}"
        elif event_type == InventoryEventType.TRANSFER:
            # Get a different warehouse for transfer destination
            other_warehouses = [w for w in self.ref.warehouses.values() 
                              if w.warehouse_id != warehouse.warehouse_id]
            if other_warehouses:
                event.transfer_destination = random.choice(other_warehouses).warehouse_id
        
        result = event.to_druid_dict()
        if is_anomaly:
            result = self.inject_anomaly(result, "inventory_discrepancy")
        
        return result
    
    def _determine_event_type(self) -> InventoryEventType:
        """Determine event type based on realistic probabilities"""
        # Higher probability of stock_out during peak hours
        weights = {
            InventoryEventType.STOCK_IN: 0.30,
            InventoryEventType.STOCK_OUT: 0.40,
            InventoryEventType.ADJUSTMENT: 0.15,
            InventoryEventType.TRANSFER: 0.10,
            InventoryEventType.CYCLE_COUNT: 0.03,
            InventoryEventType.EXPIRY_REMOVAL: 0.02
        }
        
        choices = list(weights.keys())
        weight_values = list(weights.values())
        return random.choices(choices, weights=weight_values, k=1)[0]
    
    def _select_warehouse(self, event_type: InventoryEventType):
        """Select a warehouse, considering event type requirements"""
        if event_type == InventoryEventType.STOCK_IN:
            # Prefer warehouses near ports/suppliers
            return self.ref.get_random_warehouse()
        elif event_type == InventoryEventType.TRANSFER:
            # Prefer warehouses with high utilization for rebalancing
            warehouses = list(self.ref.warehouses.values())
            return random.choice(warehouses)
        else:
            return self.ref.get_random_warehouse()
    
    def _select_product(self, warehouse, event_type: InventoryEventType):
        """Select a product appropriate for the warehouse and event type"""
        # Filter products that can be stored in this warehouse
        valid_products = [
            p for p in self.ref.products.values()
            if not p.requires_cold_storage or warehouse.has_cold_storage
        ]
        
        if not valid_products:
            return None
        
        # For stock_out, prefer products with higher stock
        if event_type == InventoryEventType.STOCK_OUT:
            # Weight by current stock level (more stock = more likely to be picked)
            weights = []
            for p in valid_products:
                inv = self.state.get_inventory(p.product_id, warehouse.warehouse_id)
                stock = inv.quantity if inv else 0
                weights.append(max(1, stock))
            return random.choices(valid_products, weights=weights, k=1)[0]
        
        # For expiry_removal, only select perishable products
        if event_type == InventoryEventType.EXPIRY_REMOVAL:
            perishable = [p for p in valid_products if p.is_perishable]
            if perishable:
                return random.choice(perishable)
        
        return random.choice(valid_products)
    
    def _select_zone(self, warehouse, product, event_type: InventoryEventType) -> Optional[Dict]:
        """Select an appropriate zone for the event"""
        zones = self.ref.get_warehouse_zones(warehouse.warehouse_id)
        
        if not zones:
            return None
        
        if event_type == InventoryEventType.STOCK_IN:
            # Prefer receiving zone
            receiving = [z for z in zones if z['type'] == 'receiving']
            return receiving[0] if receiving else zones[0]
        elif event_type == InventoryEventType.STOCK_OUT:
            # Prefer pick_pack or shipping zone
            pick_ship = [z for z in zones if z['type'] in ['pick_pack', 'shipping', 'ecommerce_pick']]
            return pick_ship[0] if pick_ship else zones[0]
        elif product.requires_cold_storage:
            cold = [z for z in zones if z['type'] == 'cold']
            return cold[0] if cold else zones[0]
        else:
            return random.choice(zones)
    
    def _generate_quantity_and_cost(self, product, event_type: InventoryEventType, timestamp: datetime) -> tuple:
        """Generate realistic quantity and unit cost"""
        # Base quantity depends on product type
        base_quantities = {
            "electronics": (10, 100),
            "food_beverage": (50, 500),
            "health_beauty": (20, 200),
            "home_garden": (15, 150),
            "apparel": (30, 300)
        }
        
        min_q, max_q = base_quantities.get(product.category, (10, 100))
        
        # Apply seasonality
        seasonal_mult = self.get_seasonal_multiplier(timestamp)
        adjusted_min = int(min_q * seasonal_mult)
        adjusted_max = int(max_q * seasonal_mult)
        
        quantity = random.randint(adjusted_min, adjusted_max)
        
        # Unit cost with some variance
        cost_variance = random.uniform(0.95, 1.05)
        unit_cost = round(product.unit_cost * cost_variance, 2)
        
        return quantity, unit_cost
    
    def get_topic_name(self) -> str:
        return "supply_chain.inventory_events"