# ========================
# src/pipeline/cleaning.py
# ========================

"""
Data Cleaning Module

Applies comprehensive cleaning and standardization rules to raw data records.
"""

import re
import logging
from datetime import datetime
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Applies a series of cleaning and standardization rules to raw data records.
    Each method handles a specific type of data inconsistency.
    """
    
    # Mapping dictionaries for standardizing messy data
    REGION_MAP = {
        "nort": "North", "north": "North", "nOrth": "North",
        "sout": "South", "south": "South", "sOuth": "South", 
        "eas": "East", "east": "East", "eAst": "East",
        "wes": "West", "west": "West", "wEst": "West",
    }
    
    PRODUCT_NAME_MAP = {
        "laptop pro": "Laptop Pro 15",
        "smartfone x": "Smartphone X",
        "smart-phone x": "Smartphone X", 
        "smartphone-x": "Smartphone X",
        "wirless headphones": "Wireless Headphones",
        "wireless headphones": "Wireless Headphones",
        "4k led tv": "4K LED TV",
        "4ktv": "4K LED TV",
        "4k led tv": "4K LED TV",
        "blenderpro": "Blender Pro",
        "blender pro": "Blender Pro",
        "cofee maker elite": "Coffee Maker Elite",
        "coffee maker elite": "Coffee Maker Elite",
        "mens t-shirt (blue)": "Men's T-shirt (Blue)",
        "t-shirt (blue)": "Men's T-shirt (Blue)"
    }
    
    CATEGORY_MAP = {
        "electronics": "Electronics",
        "home appliance": "Home Appliance",
        "home appliances": "Home Appliance", 
        "fashion": "Fashion",
        "clothing": "Fashion"
    }

    def __init__(self):
        """Initialize the data cleaner."""
        self.records_processed = 0
        self.records_dropped = 0
        logger.info("DataCleaner initialized")

    def clean_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Applies all cleaning rules to a single record and returns the cleaned version.
        
        Args:
            record (dict): A dictionary representing a single row of data.
            
        Returns:
            dict or None: The cleaned record dictionary, or None if the record
                          is invalid and should be dropped.
        """
        try:
            self.records_processed += 1
            
            # Create a copy to avoid modifying the original
            cleaned_record = record.copy()
            
            # 1. Standardize and clean strings
            cleaned_record['product_name'] = self._standardize_string(
                cleaned_record.get('product_name'), self.PRODUCT_NAME_MAP
            )
            cleaned_record['category'] = self._standardize_string(
                cleaned_record.get('category'), self.CATEGORY_MAP
            )
            cleaned_record['region'] = self._standardize_string(
                cleaned_record.get('region'), self.REGION_MAP
            )
            
            # 2. Convert and validate numeric fields
            cleaned_record['quantity'] = self._clean_quantity(cleaned_record.get('quantity'))
            cleaned_record['unit_price'] = self._clean_float(cleaned_record.get('unit_price'))
            cleaned_record['discount_percent'] = self._clean_discount(cleaned_record.get('discount_percent'))
            
            # 3. Convert and validate date
            cleaned_record['sale_date'] = self._clean_date(cleaned_record.get('sale_date'))
            
            # 4. Handle customer email
            cleaned_record['customer_email'] = self._clean_email(cleaned_record.get('customer_email'))
            
            # 5. Validate all critical fields
            if any(val is None for val in [
                cleaned_record['quantity'], 
                cleaned_record['unit_price'], 
                cleaned_record['discount_percent'], 
                cleaned_record['sale_date']
            ]):
                self.records_dropped += 1
                logger.debug(f"Record dropped due to missing critical fields: {record}")
                return None
            
            # 6. Calculate derived revenue field
            cleaned_record['revenue'] = (
                cleaned_record['quantity'] * 
                cleaned_record['unit_price'] * 
                (1 - cleaned_record['discount_percent'])
            )
            
            return cleaned_record
            
        except Exception as e:
            self.records_dropped += 1
            logger.warning(f"Error cleaning record: {e}, Record: {record}")
            return None

    def _standardize_string(self, value: Any, mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Standardize a string by stripping whitespace and applying mappings."""
        if not isinstance(value, str) or not value.strip():
            return None
            
        clean_value = value.strip().lower()
        
        if mapping and clean_value in mapping:
            return mapping[clean_value]
        
        return clean_value.title()

    def _clean_quantity(self, value: Any) -> Optional[int]:
        """
        Cleans and validates the quantity field.
        Converts to integer and ensures it's a positive number.
        """
        if isinstance(value, (int, float)):
            return int(value) if value > 0 else None
        
        if isinstance(value, str):
            # First check if it's a negative number
            if value.strip().startswith('-'):
                return None
            # Remove any non-digit characters and try to convert
            cleaned_value = re.sub(r'[^\d]', '', value)
            try:
                quantity = int(cleaned_value)
                return quantity if quantity > 0 else None
            except ValueError:
                return None
        
        return None

    def _clean_float(self, value: Any) -> Optional[float]:
        """Converts a value to a float, handling common errors."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _clean_discount(self, value: Any) -> Optional[float]:
        """
        Validates the discount field.
        Returns a float between 0.0 and 1.0, or None if invalid.
        """
        try:
            discount = float(value)
            return discount if 0.0 <= discount <= 1.0 else None
        except (ValueError, TypeError):
            return None
    
    def _clean_date(self, value: Any) -> Optional[datetime]:
        """
        Parses a date string in various formats.
        Returns a datetime object or None if malformed.
        """
        if not value or not isinstance(value, str):
            return None
        
        date_formats = [
            "%Y-%m-%d %H:%M:%S", 
            "%Y/%m/%d", 
            "%Y-%m-%d", 
            "%d-%b-%Y",
            "%m/%d/%Y",
            "%d/%m/%Y"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue
        return None
    
    def _clean_email(self, value: Any) -> str:
        """Clean and validate email field."""
        if not value or not isinstance(value, str) or not value.strip():
            return "anonymous"
        
        email = value.strip().lower()
        # Basic email validation
        if '@' in email and '.' in email.split('@')[-1]:
            return email
        
        return "anonymous"
    
    def get_statistics(self) -> Dict[str, int]:
        """Get cleaning statistics."""
        return {
            'records_processed': self.records_processed,
            'records_dropped': self.records_dropped,
            'records_cleaned': self.records_processed - self.records_dropped,
            'success_rate': (self.records_processed - self.records_dropped) / self.records_processed * 100 if self.records_processed > 0 else 0
        }
