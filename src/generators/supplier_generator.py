"""
Supplier Event Generator - Generates purchase order and supplier events
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import random
from .base import BaseEventGenerator
from ..models import SupplierEvent, SupplierEventType


class SupplierEventGenerator(BaseEventGenerator):
    """Generates supplier/purchase order events"""
    
    STATUS_FLOW = [
        SupplierEventType.PO_CREATED,
        SupplierEventType.PO_ACKNOWLEDGED,
        SupplierEventType.SHIPPED,
        SupplierEventType.IN_TRANSIT,
        SupplierEventType.CUSTOMS_CLEARED,
        SupplierEventType.RECEIVED,
        SupplierEventType.QUALITY_CHECKED
    ]
    
    PORTS_OF_ENTRY = {
        "asia_pacific": ["USLAX", "USLGB", "USOAK"],
        "europe": ["USNYC", "USHOU", "USMIA"],
        "south_america": ["USMIA", "USHOU", "USLAX"],
        "north_america": []  # No port needed for domestic
    }
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate a supplier event"""
        
        # Decide: create new PO or update existing
        active_pos = list(self.state.active_purchase_orders.values())
        
        if active_pos and random.random() < 0.6:
            # Update existing PO
            po = random.choice(active_pos)
            return self._update_purchase_order(timestamp, po)
        else:
            # Create new PO
            return self._create_purchase_order(timestamp)
    
    def _create_purchase_order(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Create a new purchase order"""
        # Select supplier
        supplier = self.ref.get_random_supplier()
        
        # Select product from this supplier
        products = [p for p in self.ref.products.values() if p.supplier_id == supplier.supplier_id]
        if not products:
            products = list(self.ref.products.values())
        product = random.choice(products)
        
        # Calculate quantity (based on demand and lead time)
        base_qty = random.randint(500, 5000)
        seasonal_mult = self.get_seasonal_multiplier(timestamp)
        quantity = int(base_qty * seasonal_mult)
        
        # Unit price with some variance
        unit_price = round(product.unit_cost * random.uniform(0.95, 1.10), 2)
        
        # Lead time
        promised_lead_time = supplier.lead_time_days + random.randint(-5, 10)
        promised_delivery = timestamp + timedelta(days=promised_lead_time)
        
        # Currency
        currency = "USD" if supplier.region == "north_america" else "USD"  # Simplified
        
        # Generate IDs
        po_id = self.generate_id("PO")
        po_line_id = f"{po_id}-L01"
        
        # Determine port of entry for international suppliers
        port_of_entry = ""
        if supplier.region != "north_america" and supplier.port_of_origin:
            ports = self.PORTS_OF_ENTRY.get(supplier.region, [])
            port_of_entry = random.choice(ports) if ports else ""
        
        # Create state
        self.state.create_purchase_order(
            po_id=po_id,
            supplier_id=supplier.supplier_id,
            material_id=product.product_id,
            quantity_ordered=quantity,
            unit_price=unit_price,
            promised_delivery=promised_delivery
        )
        
        # Build event
        event = SupplierEvent(
            timestamp=timestamp,
            purchase_order_id=po_id,
            po_line_id=po_line_id,
            supplier_id=supplier.supplier_id,
            supplier_name=supplier.name,
            supplier_region=supplier.region,
            supplier_tier=supplier.tier,
            material_id=product.product_id,
            material_category=product.category,
            event_type=SupplierEventType.PO_CREATED,
            quantity_ordered=quantity,
            quantity_received=0,
            quantity_rejected=0,
            unit_price=unit_price,
            line_total=round(quantity * unit_price, 2),
            currency=currency,
            promised_lead_time_days=promised_lead_time,
            actual_lead_time_days=0,
            lead_time_variance_days=0,
            quality_score=0.0,
            quality_check_passed=True,
            defect_rate=0.0,
            port_of_origin=supplier.port_of_origin or "",
            port_of_entry=port_of_entry,
            vessel_id="",
            container_id="",
            customs_clearance_hours=0.0,
            shipping_method=supplier.shipping_method,
            incoterm=supplier.incoterm,
            on_time_delivery=True
        )
        
        return event.to_druid_dict()
    def _update_purchase_order(self, timestamp: datetime, po: Any) -> Optional[Dict[str, Any]]:
        """Update an existing purchase order through its lifecycle"""
        
        current_status = SupplierEventType(po.status)
        
        # Find next status
        try:
            current_idx = self.STATUS_FLOW.index(current_status)
            next_idx = current_idx + 1
            if next_idx >= len(self.STATUS_FLOW):
                return None
            next_status = self.STATUS_FLOW[next_idx]
        except ValueError:
            next_status = SupplierEventType.RECEIVED
        
        # Get supplier info
        supplier = self.ref.suppliers.get(po.supplier_id)
        product = self.ref.products.get(po.material_id)
        
        # 1. Calculate lead times as LOCAL variables (not on the 'po' object)
        actual_lead_time = (timestamp - po.created_at).days
        promised_lead_time_days = (po.promised_delivery - po.created_at).days
        lead_time_variance = actual_lead_time - promised_lead_time_days
        
        # On-time delivery
        on_time = lead_time_variance <= 0
        
        # Quality metrics (only for quality_checked)
        quality_score = 0.0
        quality_passed = True
        defect_rate = 0.0
        quantity_received = po.quantity_received
        quantity_rejected = 0
        
        if next_status == SupplierEventType.QUALITY_CHECKED:
            quality_score = random.uniform(0.85, 0.99)
            if supplier:
                quality_score = min(1.0, quality_score * (supplier.quality_score / 0.9))
            
            defect_rate = random.uniform(0.01, 0.05)
            if self.should_inject_anomaly():
                defect_rate = random.uniform(0.1, 0.3)
                quality_score = max(0.5, quality_score - 0.3)
            
            quality_passed = quality_score > 0.85
            quantity_received = po.quantity_ordered
            quantity_rejected = int(quantity_received * defect_rate) if not quality_passed else 0
        elif next_status == SupplierEventType.RECEIVED:
            quantity_received = po.quantity_ordered
        
        # Customs clearance time
        customs_hours = 0.0
        if next_status == SupplierEventType.CUSTOMS_CLEARED:
            customs_hours = random.uniform(2, 48)
        
        # Vessel/container for shipping
        vessel_id = ""
        container_id = ""
        if next_status in [SupplierEventType.SHIPPED, SupplierEventType.IN_TRANSIT]:
            vessel_id = f"VES-{random.randint(1000, 9999)}"
            container_id = f"CTR-{random.choice(['MSKU', 'HLCU', 'CSLU', 'TCLU'])}{random.randint(1000000, 9999999)}"
        
        # Port of entry
        port_of_entry = ""
        if supplier and supplier.region != "north_america":
            ports = self.PORTS_OF_ENTRY.get(supplier.region, [])
            port_of_entry = random.choice(ports) if ports else ""
        
        # Update state
        po.status = next_status.value
        po.quantity_received = quantity_received
        if next_status == SupplierEventType.RECEIVED:
            po.actual_delivery = timestamp
        
        # Add received inventory to warehouse
        if next_status == SupplierEventType.RECEIVED and quantity_received > 0:
            # Add to a warehouse
            warehouse = self.ref.get_random_warehouse()
            if product and (not product.requires_cold_storage or warehouse.has_cold_storage):
                self.state.update_inventory(
                    product.product_id,
                    warehouse.warehouse_id,
                    quantity_received,
                    "purchase_order_receipt"
                )
        
        # 2. Use the LOCAL variable 'promised_lead_time_days' here
        # Build event
        event = SupplierEvent(
            timestamp=timestamp,
            purchase_order_id=po.po_id,
            po_line_id=f"{po.po_id}-L01",
            supplier_id=po.supplier_id,
            supplier_name=supplier.name if supplier else "",
            supplier_region=supplier.region if supplier else "",
            supplier_tier=supplier.tier if supplier else "",
            material_id=po.material_id,
            material_category=product.category if product else "",
            event_type=next_status,
            quantity_ordered=po.quantity_ordered,
            quantity_received=quantity_received,
            quantity_rejected=quantity_rejected,
            unit_price=po.unit_price,
            line_total=round(po.quantity_ordered * po.unit_price, 2),
            currency="USD",
            promised_lead_time_days=promised_lead_time_days,
            actual_lead_time_days=actual_lead_time,
            lead_time_variance_days=lead_time_variance,
            quality_score=round(quality_score, 4),
            quality_check_passed=quality_passed,
            defect_rate=round(defect_rate, 4),
            port_of_origin=supplier.port_of_origin if supplier else "",
            port_of_entry=port_of_entry,
            vessel_id=vessel_id,
            container_id=container_id,
            customs_clearance_hours=round(customs_hours, 1),
            shipping_method=supplier.shipping_method if supplier else "",
            incoterm=supplier.incoterm if supplier else "",
            on_time_delivery=on_time
        )
        
        result = event.to_druid_dict()
        
        # Inject supply disruption anomaly
        if self.should_inject_anomaly() and next_status == SupplierEventType.IN_TRANSIT:
            result = self.inject_anomaly(result, "supply_disruption")
            result['lead_time_variance_days'] = result['lead_time_variance_days'] + random.randint(14, 30)
            result['on_time_delivery'] = False
        
        return result    
    
    
    def get_topic_name(self) -> str:
        return "supply_chain.supplier_events"