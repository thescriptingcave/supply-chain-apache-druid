"""
Reference Data Loader - Loads and manages master data for the generator
"""

import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
import random


@dataclass
class Warehouse:
    warehouse_id: str
    name: str
    region: str
    city: str
    state: str
    country: str
    type: str
    capacity_units: int
    current_utilization: float
    zones: List[Dict[str, Any]]
    operating_hours: str
    has_cold_storage: bool


@dataclass
class Product:
    product_id: str
    name: str
    sku: str
    category: str
    subcategory: str
    unit_weight_kg: float
    unit_volume_cbm: float
    unit_cost: float
    retail_price: float
    is_perishable: bool
    requires_cold_storage: bool
    shelf_life_days: Optional[int]
    safety_stock_days: int
    lead_time_days: int
    supplier_id: str
    has_variants: bool = False
    variants: Optional[List[Dict]] = None
    seasonal: bool = False
    season: Optional[str] = None


@dataclass
class Supplier:
    supplier_id: str
    name: str
    region: str
    country: str
    city: str
    state: Optional[str]
    tier: str
    reliability_score: float
    on_time_delivery_rate: float
    quality_score: float
    lead_time_days: int
    shipping_method: str
    port_of_origin: Optional[str]
    incoterm: str


@dataclass
class Carrier:
    carrier_id: str
    name: str
    service_level: str
    avg_delivery_days: int
    on_time_rate: float
    cost_per_km: float
    has_gps_tracking: bool
    has_temp_control: bool
    fleet_size: int


@dataclass
class CustomerSegment:
    segment_id: str
    name: str
    tier: str
    avg_order_value: float
    order_frequency_days: int
    discount_rate: float
    payment_terms_days: int
    percentage: float


@dataclass
class ProductionLine:
    line_id: str
    name: str
    facility_id: str
    facility_name: str
    region: str
    products: List[str]
    capacity_per_hour: int
    shift_pattern: str
    machines: List[Dict[str, Any]]


@dataclass
class Region:
    region_id: str
    name: str
    states: List[str]
    population_weight: float
    demand_multiplier: float


class ReferenceDataManager:
    """Manages all reference/master data for the supply chain generator"""
    
    def __init__(self, config_path: str = "config/reference_data.yaml"):
        self.config_path = Path(config_path)
        self.warehouses: Dict[str, Warehouse] = {}
        self.products: Dict[str, Product] = {}
        self.suppliers: Dict[str, Supplier] = {}
        self.carriers: Dict[str, Carrier] = {}
        self.customer_segments: List[CustomerSegment] = []
        self.production_lines: Dict[str, ProductionLine] = {}
        self.regions: Dict[str, Region] = {}
        self.seasonality: Dict[str, Any] = {}
        self.promotions: List[Dict[str, Any]] = []
        
        # Indexes for fast lookups
        self.products_by_category: Dict[str, List[Product]] = {}
        self.warehouses_by_region: Dict[str, List[Warehouse]] = {}
        self.production_lines_by_product: Dict[str, List[ProductionLine]] = {}
        
        self._load_data()

    def _load_data(self):
        """Load reference data from YAML configuration"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Reference data config not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        self._load_warehouses(data.get('warehouses', []))
        self._load_products(data.get('products', []))
        self._load_suppliers(data.get('suppliers', []))
        self._load_carriers(data.get('carriers', []))
        self._load_customer_segments(data.get('customers', {}).get('segments', []))
        self._load_production_lines(data.get('production_lines', []))
        self._load_regions(data.get('regions', []))
        self._load_seasonality(data.get('seasonality', {}))
        self._load_promotions(data.get('promotions', []))
        
        self._build_indexes()

    def _load_warehouses(self, warehouses_data: List[Dict]):
        for wh in warehouses_data:
            warehouse = Warehouse(
                warehouse_id=wh['warehouse_id'],
                name=wh['name'],
                region=wh['region'],
                city=wh['city'],
                state=wh['state'],
                country=wh['country'],
                type=wh['type'],
                capacity_units=wh['capacity_units'],
                current_utilization=wh['current_utilization'],
                zones=wh.get('zones', []),
                operating_hours=wh.get('operating_hours', '24/7'),
                has_cold_storage=wh.get('has_cold_storage', False)
            )
            self.warehouses[warehouse.warehouse_id] = warehouse

    def _load_products(self, products_data: List[Dict]):
        for prod in products_data:
            product = Product(
                product_id=prod['product_id'],
                name=prod['name'],
                sku=prod['sku'],
                category=prod['category'],
                subcategory=prod['subcategory'],
                unit_weight_kg=prod['unit_weight_kg'],
                unit_volume_cbm=prod['unit_volume_cbm'],
                unit_cost=prod['unit_cost'],
                retail_price=prod['retail_price'],
                is_perishable=prod['is_perishable'],
                requires_cold_storage=prod['requires_cold_storage'],
                shelf_life_days=prod.get('shelf_life_days'),
                safety_stock_days=prod['safety_stock_days'],
                lead_time_days=prod['lead_time_days'],
                supplier_id=prod['supplier_id'],
                has_variants=prod.get('has_variants', False),
                variants=prod.get('variants'),
                seasonal=prod.get('seasonal', False),
                season=prod.get('season')
            )
            self.products[product.product_id] = product

    def _load_suppliers(self, suppliers_data: List[Dict]):
        for sup in suppliers_data:
            supplier = Supplier(
                supplier_id=sup['supplier_id'],
                name=sup['name'],
                region=sup['region'],
                country=sup['country'],
                city=sup['city'],
                state=sup.get('state'),
                tier=sup['tier'],
                reliability_score=sup['reliability_score'],
                on_time_delivery_rate=sup['on_time_delivery_rate'],
                quality_score=sup['quality_score'],
                lead_time_days=sup['lead_time_days'],
                shipping_method=sup['shipping_method'],
                port_of_origin=sup.get('port_of_origin'),
                incoterm=sup['incoterm']
            )
            self.suppliers[supplier.supplier_id] = supplier

    def _load_carriers(self, carriers_data: List[Dict]):
        for car in carriers_data:
            carrier = Carrier(
                carrier_id=car['carrier_id'],
                name=car['name'],
                service_level=car['service_level'],
                avg_delivery_days=car['avg_delivery_days'],
                on_time_rate=car['on_time_rate'],
                cost_per_km=car['cost_per_km'],
                has_gps_tracking=car['has_gps_tracking'],
                has_temp_control=car['has_temp_control'],
                fleet_size=car['fleet_size']
            )
            self.carriers[carrier.carrier_id] = carrier

    def _load_customer_segments(self, segments_data: List[Dict]):
        for seg in segments_data:
            segment = CustomerSegment(
                segment_id=seg['segment_id'],
                name=seg['name'],
                tier=seg['tier'],
                avg_order_value=seg['avg_order_value'],
                order_frequency_days=seg['order_frequency_days'],
                discount_rate=seg['discount_rate'],
                payment_terms_days=seg['payment_terms_days'],
                percentage=seg['percentage']
            )
            self.customer_segments.append(segment)

    def _load_production_lines(self, lines_data: List[Dict]):
        for line in lines_data:
            prod_line = ProductionLine(
                line_id=line['line_id'],
                name=line['name'],
                facility_id=line['facility_id'],
                facility_name=line['facility_name'],
                region=line['region'],
                products=line['products'],
                capacity_per_hour=line['capacity_per_hour'],
                shift_pattern=line['shift_pattern'],
                machines=line.get('machines', [])
            )
            self.production_lines[prod_line.line_id] = prod_line

    def _load_regions(self, regions_data: List[Dict]):
        for reg in regions_data:
            region = Region(
                region_id=reg['region_id'],
                name=reg['name'],
                states=reg['states'],
                population_weight=reg['population_weight'],
                demand_multiplier=reg['demand_multiplier']
            )
            self.regions[region.region_id] = region

    def _load_seasonality(self, seasonality_data: Dict):
        self.seasonality = seasonality_data

    def _load_promotions(self, promotions_data: List[Dict]):
        self.promotions = promotions_data

    def _build_indexes(self):
        """Build lookup indexes for faster access"""
        # Products by category
        self.products_by_category = {}
        for product in self.products.values():
            if product.category not in self.products_by_category:
                self.products_by_category[product.category] = []
            self.products_by_category[product.category].append(product)
        
        # Warehouses by region
        self.warehouses_by_region = {}
        for warehouse in self.warehouses.values():
            if warehouse.region not in self.warehouses_by_region:
                self.warehouses_by_region[warehouse.region] = []
            self.warehouses_by_region[warehouse.region].append(warehouse)
        
        # Production lines by product
        self.production_lines_by_product = {}
        for line in self.production_lines.values():
            for product_id in line.products:
                if product_id not in self.production_lines_by_product:
                    self.production_lines_by_product[product_id] = []
                self.production_lines_by_product[product_id].append(line)

    # Convenience methods for random selection
    def get_random_warehouse(self, region: Optional[str] = None) -> Warehouse:
        if region and region in self.warehouses_by_region:
            return random.choice(self.warehouses_by_region[region])
        return random.choice(list(self.warehouses.values()))

    def get_random_product(self, category: Optional[str] = None) -> Product:
        if category and category in self.products_by_category:
            return random.choice(self.products_by_category[category])
        return random.choice(list(self.products.values()))

    def get_random_supplier(self) -> Supplier:
        return random.choice(list(self.suppliers.values()))

    def get_random_carrier(self, requires_temp_control: bool = False) -> Carrier:
        if requires_temp_control:
            temp_carriers = [c for c in self.carriers.values() if c.has_temp_control]
            if temp_carriers:
                return random.choice(temp_carriers)
        return random.choice(list(self.carriers.values()))

    def get_random_customer_segment(self) -> CustomerSegment:
        # Weighted random selection based on percentage
        rand = random.random()
        cumulative = 0
        for segment in self.customer_segments:
            cumulative += segment.percentage
            if rand <= cumulative:
                return segment
        return self.customer_segments[-1]

    def get_random_production_line(self, product_id: Optional[str] = None) -> ProductionLine:
        if product_id and product_id in self.production_lines_by_product:
            return random.choice(self.production_lines_by_product[product_id])
        return random.choice(list(self.production_lines.values()))

    def get_random_region(self) -> Region:
        return random.choice(list(self.regions.values()))

    def get_seasonality_multiplier(self, dt) -> float:
        """Get combined seasonality multiplier for a given datetime"""
        monthly_mult = self.seasonality.get('monthly_multipliers', {}).get(dt.month, 1.0)
        dow_mult = self.seasonality.get('dow_multipliers', {}).get(dt.weekday(), 1.0)
        hod_mult = self.seasonality.get('hod_multipliers', {}).get(dt.hour, 1.0)
        return monthly_mult * dow_mult * hod_mult

    def get_active_promotions(self, dt) -> List[Dict]:
        """Get promotions active at a given datetime"""
        active = []
        for promo in self.promotions:
            start_month = promo.get('start_month')
            end_month = promo.get('end_month')
            if start_month and end_month and start_month <= dt.month <= end_month:
                # Check day constraints if present
                start_day = promo.get('start_day')
                end_day = promo.get('end_day')
                if start_day and end_day:
                    if start_day <= dt.day <= end_day:
                        active.append(promo)
                # Check day of week if present
                elif 'day_of_week' in promo:
                    if dt.weekday() == promo['day_of_week']:
                        active.append(promo)
                else:
                    active.append(promo)
        return active

    def get_cold_storage_warehouses(self) -> List[Warehouse]:
        """Get all warehouses with cold storage capability"""
        return [wh for wh in self.warehouses.values() if wh.has_cold_storage]
    
    def get_warehouse_zones(self, warehouse_id: str, zone_type: Optional[str] = None) -> List[Dict]:
        """Get zones for a warehouse, optionally filtered by type"""
        if warehouse_id not in self.warehouses:
            return []
        zones = self.warehouses[warehouse_id].zones
        if zone_type:
            return [z for z in zones if z['type'] == zone_type]
        return zones