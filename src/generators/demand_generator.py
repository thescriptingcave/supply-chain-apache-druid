"""
Demand Event Generator - Generates customer order/demand events for forecasting
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import random
from .base import BaseEventGenerator
from ..models import DemandEvent, DemandChannel


class DemandEventGenerator(BaseEventGenerator):
    """Generates customer demand/order events with realistic patterns"""
    
    CHANNEL_DISTRIBUTION = {
        DemandChannel.ONLINE: 0.55,
        DemandChannel.RETAIL: 0.25,
        DemandChannel.WHOLESALE: 0.15,
        DemandChannel.B2B: 0.05
    }
    
    ORDER_PRIORITY_DISTRIBUTION = {
        "standard": 0.70,
        "expedited": 0.25,
        "emergency": 0.05
    }
    
    CITIES_BY_REGION = {
        "northeast": ["New York", "Boston", "Philadelphia", "Hartford", "Pittsburgh"],
        "southeast": ["Atlanta", "Miami", "Charlotte", "Nashville", "Jacksonville"],
        "midwest": ["Chicago", "Detroit", "Indianapolis", "Cleveland", "Minneapolis"],
        "west": ["Los Angeles", "San Francisco", "Seattle", "Portland", "San Diego"],
        "southwest": ["Houston", "Dallas", "Phoenix", "Austin", "Denver"]
    }
    
    def generate_event(self, timestamp: datetime) -> Optional[Dict[str, Any]]:
        """Generate a demand event"""
        
        # Select customer segment
        segment = self.ref.get_random_customer_segment()
        
        # Select channel (weighted)
        channels = list(self.CHANNEL_DISTRIBUTION.keys())
        channel_weights = list(self.CHANNEL_DISTRIBUTION.values())
        channel = random.choices(channels, weights=channel_weights, k=1)[0]
        
        # Select region based on population weight
        region = self.ref.get_random_region()
        
        # Select city
        cities = self.CITIES_BY_REGION.get(region.region_id, ["Unknown"])
        city = random.choice(cities)
        
        # Select fulfillment warehouse (prefer same region)
        warehouse = self.ref.get_random_warehouse(region.region_id)
        if not warehouse:
            warehouse = self.ref.get_random_warehouse()
        
        # Select product
        product = self._select_product(timestamp, channel)
        
        # Calculate quantity based on segment and channel
        quantity = self._calculate_quantity(segment, channel, product, timestamp)
        
        # Calculate pricing
        unit_price = product.retail_price
        discount_percent = 0.0
        discount_amount = 0.0
        promotion_id = None
        promotion_type = ""
        
        # Check for active promotions
        active_promotions = self.ref.get_active_promotions(timestamp)
        applicable_promotions = [
            p for p in active_promotions
            if product.category in p.get('applicable_categories', []) or 
               'all' in p.get('applicable_categories', [])
        ]
        
        if applicable_promotions and random.random() < 0.3:  # 30% chance to apply promo
            promo = random.choice(applicable_promotions)
            promotion_id = promo['promotion_id']
            promotion_type = promo['type']
            # Use higher of segment discount or promo discount
            discount_percent = max(segment.discount_rate, promo['discount_percent'])
        
        # Apply segment discount if no promo
        if not promotion_id:
            discount_percent = segment.discount_rate
        
        # Apply seasonal adjustment to price
        seasonal_mult = self.get_seasonal_multiplier(timestamp)
        if seasonal_mult > 1.2:  # High demand period
            unit_price = round(unit_price * 1.05, 2)  # Slight price increase
        
        discount_amount = round(unit_price * discount_percent * quantity, 2)
        line_total = round(unit_price * quantity, 2)
        net_amount = round(line_total - discount_amount, 2)
        
        # Check for backorder
        is_backorder = False
        backorder_quantity = 0
        inv = self.state.get_inventory(product.product_id, warehouse.warehouse_id)
        if inv and inv.available_quantity < quantity:
            backorder_quantity = quantity - inv.available_quantity
            is_backorder = backorder_quantity > 0
        
        # Order priority
        priorities = list(self.ORDER_PRIORITY_DISTRIBUTION.keys())
        priority_weights = list(self.ORDER_PRIORITY_DISTRIBUTION.values())
        order_priority = random.choices(priorities, weights=priority_weights, k=1)[0]
        
        # If backorder and not emergency, might downgrade priority
        if is_backorder and order_priority == "emergency":
            order_priority = "expedited"
        
        # Calculate delivery date
        if order_priority == "emergency":
            lead_time_days = 1
        elif order_priority == "expedited":
            lead_time_days = random.randint(2, 3)
        else:
            lead_time_days = random.randint(5, 7)
        
        requested_delivery = timestamp + timedelta(days=lead_time_days)
        
        # Generate order IDs
        order_id = self.generate_id("ORD")
        order_line_id = f"{order_id}-L{random.randint(1, 10):02d}"
        customer_id = self.generate_id(f"CUST-{segment.tier[:3].upper()}")
        
        # Build event
        event = DemandEvent(
            timestamp=timestamp,
            order_id=order_id,
            order_line_id=order_line_id,
            customer_id=customer_id,
            customer_segment=segment.name,
            customer_tier=segment.tier,
            channel=channel,
            product_id=product.product_id,
            product_category=product.category,
            product_subcategory=product.subcategory,
            sku=product.sku,
            quantity_ordered=quantity,
            unit_price=unit_price,
            line_total=line_total,
            discount_percent=discount_percent,
            discount_amount=discount_amount,
            net_amount=net_amount,
            region=region.region_id,
            city=city,
            fulfillment_warehouse_id=warehouse.warehouse_id,
            fulfillment_warehouse_region=warehouse.region,
            promotion_id=promotion_id,
            promotion_type=promotion_type,
            is_recurring=random.random() < 0.15,  # 15% recurring orders
            order_priority=order_priority,
            requested_delivery_date=requested_delivery,
            customer_lead_time_days=lead_time_days,
            seasonality_factor=round(seasonal_mult, 3),
            is_backorder=is_backorder,
            backorder_quantity=backorder_quantity
        )
        
        result = event.to_druid_dict()
        
        # Inject demand surge anomaly
        if self.should_inject_anomaly() and random.random() < 0.3:
            result = self.inject_anomaly(result, "demand_surge")
            surge_multiplier = random.uniform(5, 15)
            result['quantity_ordered'] = int(quantity * surge_multiplier)
            result['line_total'] = round(unit_price * result['quantity_ordered'], 2)
            result['net_amount'] = round(result['line_total'] - discount_amount * surge_multiplier, 2)
        
        return result
    
    def _select_product(self, timestamp: datetime, channel: DemandChannel):
        """Select product based on channel and seasonality"""
        # Different products popular in different channels
        if channel == DemandChannel.WHOLESALE:
            # Prefer high-volume, lower-cost items
            products = [p for p in self.ref.products.values() if p.retail_price < 50]
            if products:
                return random.choice(products)
        
        elif channel == DemandChannel.B2B:
            # Prefer business-oriented products
            preferred_categories = ["electronics", "home_garden"]
            products = [p for p in self.ref.products.values() if p.category in preferred_categories]
            if products:
                return random.choice(products)
        
        # Check for seasonal products
        month = timestamp.month
        if month in [11, 12, 1, 2]:  # Winter
            winter_products = [p for p in self.ref.products.values() if getattr(p, 'season', None) == 'winter']
            if winter_products and random.random() < 0.4:
                return random.choice(winter_products)
        
        return self.ref.get_random_product()
    
    def _calculate_quantity(self, segment, channel: DemandChannel, product, timestamp: datetime) -> int:
        """Calculate order quantity based on multiple factors"""
        # Base quantity from segment
        base_qty = segment.avg_order_value / product.retail_price
        
        # Channel adjustment
        channel_multiplier = {
            DemandChannel.ONLINE: 1.0,
            DemandChannel.RETAIL: 1.2,
            DemandChannel.WHOLESALE: 5.0,
            DemandChannel.B2B: 10.0
        }
        
        quantity = base_qty * channel_multiplier.get(channel, 1.0)
        
        # Seasonality
        seasonal_mult = self.get_seasonal_multiplier(timestamp)
        quantity *= seasonal_mult
        
        # Region demand multiplier
        region = self.ref.get_random_region()
        quantity *= region.demand_multiplier
        
        # Add randomness
        quantity *= random.uniform(0.5, 1.5)
        
        return max(1, int(quantity))
    
    def get_topic_name(self) -> str:
        return "supply_chain.demand_events"