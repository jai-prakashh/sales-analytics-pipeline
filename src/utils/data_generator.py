# ========================
# src/utils/data_generator.py
# ========================

"""
Data Generation Utilities

Enhanced data generation with more realistic patterns and error injection.
"""

import csv
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class DataGenerator:
    """
    Enhanced data generator for creating realistic test datasets.
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize data generator.
        
        Args:
            seed (int): Random seed for reproducible data generation
        """
        if seed is not None:
            random.seed(seed)
        
        self._initialize_data_patterns()
        logger.info(f"DataGenerator initialized with seed: {seed}")
    
    def _initialize_data_patterns(self) -> None:
        """Initialize data patterns and distributions."""
        # Product catalog with realistic pricing
        self.products = [
            {"name": "Laptop Pro 15", "category": "Electronics", "base_price": 1200, "variants": ["laptop pro", "Laptop Pro 15"]},
            {"name": "Smartphone X", "category": "Electronics", "base_price": 800, "variants": ["smartfone x", "smart-phone x", "smartphone-x"]},
            {"name": "Wireless Headphones", "category": "Electronics", "base_price": 150, "variants": ["wirless headphones", "Wireless headphones"]},
            {"name": "4K LED TV", "category": "Home Appliance", "base_price": 2000, "variants": ["4k led tv", "4KTV", "4k led tv"]},
            {"name": "Blender Pro", "category": "Home Appliance", "base_price": 80, "variants": ["blender pro", "blenderpro"]},
            {"name": "Coffee Maker Elite", "category": "Home Appliance", "base_price": 120, "variants": ["cofee maker elite"]},
            {"name": "Men's T-shirt (Blue)", "category": "Fashion", "base_price": 25, "variants": ["Mens t-shirt (blue)", "T-shirt (Blue)"]},
            {"name": "Running Shoes", "category": "Fashion", "base_price": 95, "variants": ["running shoes", "Running Shoes"]},
            {"name": "Gaming Mouse", "category": "Electronics", "base_price": 45, "variants": ["gaming mouse", "Gaming Mouse"]},
            {"name": "Office Chair", "category": "Home Appliance", "base_price": 200, "variants": ["office chair", "Office Chair"]}
        ]
        
        # Regional patterns
        self.regions = [
            {"name": "North", "weight": 0.3, "variants": ["North", "nort", "nOrth"]},
            {"name": "South", "weight": 0.25, "variants": ["South", "sout", "south"]},
            {"name": "East", "weight": 0.25, "variants": ["East", "eas", "east"]},
            {"name": "West", "weight": 0.2, "variants": ["West", "wes", "west"]}
        ]
        
        # Customer patterns
        self.customers = [
            "user1@example.com", "user2@example.com", "customer@test.com",
            "buyer@email.com", "shopper@domain.com", None, None  # Include nulls
        ]
        
        # Seasonal patterns (month -> demand multiplier)
        self.seasonal_patterns = {
            1: 0.8,   # January - post-holiday low
            2: 0.9,   # February
            3: 1.0,   # March
            4: 1.1,   # April
            5: 1.0,   # May
            6: 0.9,   # June
            7: 0.8,   # July - summer low
            8: 0.9,   # August
            9: 1.1,   # September - back to school
            10: 1.2,  # October
            11: 1.4,  # November - pre-holiday
            12: 1.3   # December - holiday season
        }
    
    def generate_dataset(self, 
                        file_path: str, 
                        num_rows: int,
                        error_rate: float = 0.15,
                        start_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Generate a realistic dataset with controlled error injection.
        
        Args:
            file_path (str): Output CSV file path
            num_rows (int): Number of rows to generate
            error_rate (float): Fraction of records with intentional errors
            start_date (datetime): Start date for date range
            
        Returns:
            dict: Generation statistics
        """
        logger.info(f"Generating {num_rows:,} rows with {error_rate:.1%} error rate...")
        
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        
        stats = {
            'total_rows': num_rows,
            'error_rate': error_rate,
            'start_date': start_date,
            'records_with_errors': 0,
            'error_types': {}
        }
        
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'order_id', 'product_name', 'category', 'quantity', 'unit_price',
                'discount_percent', 'region', 'sale_date', 'customer_email'
            ])
            
            for i in range(num_rows):
                record = self._generate_single_record(i, start_date, error_rate, stats)
                writer.writerow(record)
                
                if (i + 1) % 10000 == 0:
                    logger.debug(f"Generated {i + 1:,} records")
        
        # Finalize stats
        stats['error_rate_actual'] = stats['records_with_errors'] / num_rows
        
        logger.info(f"Dataset generated: {file_path}")
        logger.info(f"Actual error rate: {stats['error_rate_actual']:.1%}")
        logger.info(f"Error breakdown: {stats['error_types']}")
        
        return stats
    
    def _generate_single_record(self, 
                               index: int, 
                               start_date: datetime, 
                               error_rate: float,
                               stats: Dict[str, Any]) -> List[Any]:
        """Generate a single record with potential errors."""
        # Base record generation
        order_id = f"ORD-{index:08d}-{random.randint(1000, 9999)}"
        
        # Select product with seasonal adjustment
        product = random.choice(self.products)
        product_name = random.choice(product["variants"])
        category = product["category"]
        
        # Generate date with seasonal patterns
        days_offset = random.randint(0, 365)
        sale_date = start_date + timedelta(
            days=days_offset,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Seasonal demand adjustment
        seasonal_multiplier = self.seasonal_patterns.get(sale_date.month, 1.0)
        
        # Base quantity (seasonal adjusted)
        base_quantity = max(1, int(random.randint(1, 20) * seasonal_multiplier))
        quantity = base_quantity
        
        # Price with variation
        base_price = product["base_price"]
        price_variation = random.uniform(0.8, 1.2)  # ±20% variation
        unit_price = round(base_price * price_variation, 2)
        
        # Discount (higher discounts for higher quantities)
        if quantity >= 10:
            discount_percent = round(random.uniform(0.05, 0.3), 2)  # 5-30%
        elif quantity >= 5:
            discount_percent = round(random.uniform(0.02, 0.15), 2)  # 2-15%
        else:
            discount_percent = round(random.uniform(0.0, 0.1), 2)   # 0-10%
        
        # Regional selection (weighted)
        region_weights = [r["weight"] for r in self.regions]
        selected_region = random.choices(self.regions, weights=region_weights)[0]
        region = random.choice(selected_region["variants"])
        
        # Customer
        customer_email = random.choice(self.customers)
        
        # Inject errors based on error_rate
        has_error = random.random() < error_rate
        if has_error:
            stats['records_with_errors'] += 1
            self._inject_errors(locals(), stats)
        
        # Format date
        date_formats = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d", "%d-%b-%Y"]
        formatted_date = sale_date.strftime(random.choice(date_formats))
        
        # Apply null date error occasionally
        if has_error and random.random() < 0.1:
            formatted_date = None
            self._track_error_type(stats, 'null_date')
        
        return [
            order_id, product_name, category, quantity, unit_price,
            discount_percent, region, formatted_date, customer_email
        ]
    
    def _inject_errors(self, record_vars: Dict[str, Any], stats: Dict[str, Any]) -> None:
        """Inject various types of errors into the record."""
        error_type = random.choice([
            'negative_quantity', 'invalid_discount', 'string_quantity', 
            'malformed_price', 'extreme_values'
        ])
        
        if error_type == 'negative_quantity':
            record_vars['quantity'] = -random.randint(1, 5)
            self._track_error_type(stats, 'negative_quantity')
            
        elif error_type == 'invalid_discount':
            record_vars['discount_percent'] = round(random.uniform(1.1, 2.0), 2)  # > 1.0
            self._track_error_type(stats, 'invalid_discount')
            
        elif error_type == 'string_quantity':
            record_vars['quantity'] = f"{record_vars['quantity']} units"
            self._track_error_type(stats, 'string_quantity')
            
        elif error_type == 'malformed_price':
            if random.choice([True, False]):
                record_vars['unit_price'] = f"${record_vars['unit_price']}"
            else:
                record_vars['unit_price'] = None
            self._track_error_type(stats, 'malformed_price')
            
        elif error_type == 'extreme_values':
            # Occasionally create extreme values for anomaly detection
            if random.random() < 0.3:  # 30% chance of extreme high values
                record_vars['quantity'] = random.randint(50, 100)
                record_vars['unit_price'] = round(random.uniform(5000, 10000), 2)
                self._track_error_type(stats, 'extreme_high_values')
    
    def _track_error_type(self, stats: Dict[str, Any], error_type: str) -> None:
        """Track error types for statistics."""
        if error_type not in stats['error_types']:
            stats['error_types'][error_type] = 0
        stats['error_types'][error_type] += 1
    
    def generate_large_dataset_chunked(self, 
                                     file_path: str, 
                                     total_rows: int,
                                     chunk_size: int = 100000) -> Dict[str, Any]:
        """
        Generate very large datasets in chunks to manage memory usage.
        
        Args:
            file_path (str): Output file path
            total_rows (int): Total number of rows to generate
            chunk_size (int): Rows per chunk
            
        Returns:
            dict: Generation statistics
        """
        logger.info(f"Generating large dataset: {total_rows:,} rows in chunks of {chunk_size:,}")
        
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        start_date = datetime.now() - timedelta(days=365)
        chunks_written = 0
        total_stats = {
            'total_rows': total_rows,
            'chunk_size': chunk_size,
            'chunks_written': 0,
            'records_with_errors': 0,
            'error_types': {}
        }
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'order_id', 'product_name', 'category', 'quantity', 'unit_price',
                'discount_percent', 'region', 'sale_date', 'customer_email'
            ])
            
            rows_written = 0
            while rows_written < total_rows:
                chunk_rows = min(chunk_size, total_rows - rows_written)
                
                for i in range(chunk_rows):
                    record = self._generate_single_record(
                        rows_written + i, start_date, 0.15, total_stats
                    )
                    writer.writerow(record)
                
                rows_written += chunk_rows
                chunks_written += 1
                
                logger.info(f"Chunk {chunks_written} complete: {rows_written:,}/{total_rows:,} rows")
        
        total_stats['chunks_written'] = chunks_written
        total_stats['error_rate_actual'] = total_stats['records_with_errors'] / total_rows
        
        logger.info(f"Large dataset generation complete: {file_path}")
        return total_stats
