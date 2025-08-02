# ========================
# src/pipeline/storage.py
# ========================

"""
Data Storage Module

Handles saving aggregated data to various output formats.
"""

import csv
import os
import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class DataSaver:
    """
    Saves the aggregated data from the DataAggregator to various output formats.
    """
    
    def __init__(self, output_dir: str = "data/processed"):
        """
        Initialize the data saver.
        
        Args:
            output_dir (str): Directory to save output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"DataSaver initialized with output directory: {self.output_dir}")

    def save_all_data(self, aggregator) -> Dict[str, str]:
        """
        Save all aggregated data to files.
        
        Args:
            aggregator: DataAggregator instance with processed data
            
        Returns:
            dict: Mapping of data type to saved file path
        """
        saved_files = {}
        
        try:
            saved_files['monthly_sales'] = self.save_monthly_sales(aggregator.monthly_sales)
            saved_files['top_products'] = self.save_top_products(aggregator.top_products)
            saved_files['region_performance'] = self.save_region_performance(aggregator.region_sales)
            saved_files['category_discounts'] = self.save_category_discounts(aggregator.category_discounts)
            saved_files['anomaly_records'] = self.save_anomaly_records(aggregator.anomaly_records)
            
            # Also save a summary JSON
            saved_files['summary'] = self._save_summary(aggregator.get_aggregation_summary())
            
            logger.info(f"All data saved successfully to {len(saved_files)} files")
            return saved_files
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise

    def save_monthly_sales(self, data: Dict) -> str:
        """Save monthly sales summary data."""
        file_path = self.output_dir / "monthly_sales_summary.csv"
        headers = ['month', 'revenue', 'quantity', 'avg_discount']
        rows = [{'month': k, **v} for k, v in data.items()]
        
        # Sort by month for better readability
        rows.sort(key=lambda x: x['month'])
        
        self._write_csv(file_path, headers, rows)
        return str(file_path)

    def save_top_products(self, data: List) -> str:
        """Save top products data."""
        file_path = self.output_dir / "top_products.csv"
        headers = ['product_name', 'revenue', 'quantity']
        rows = [{'product_name': k, **v} for k, v in data]
        self._write_csv(file_path, headers, rows)
        return str(file_path)
    
    def save_region_performance(self, data: Dict) -> str:
        """Save regional performance data."""
        file_path = self.output_dir / "region_wise_performance.csv"
        headers = ['region', 'revenue', 'quantity']
        rows = [{'region': k, **v} for k, v in data.items()]
        
        # Sort by revenue for better readability
        rows.sort(key=lambda x: x['revenue'], reverse=True)
        
        self._write_csv(file_path, headers, rows)
        return str(file_path)

    def save_category_discounts(self, data: Dict) -> str:
        """Save category discount analysis data."""
        file_path = self.output_dir / "category_discount_map.csv"
        headers = ['category', 'avg_discount']
        rows = [{'category': k, 'avg_discount': v['avg_discount']} for k, v in data.items()]
        
        # Sort by discount rate
        rows.sort(key=lambda x: x['avg_discount'], reverse=True)
        
        self._write_csv(file_path, headers, rows)
        return str(file_path)

    def save_anomaly_records(self, data: List) -> str:
        """Save anomaly records data."""
        file_path = self.output_dir / "anomaly_records.csv"
        
        if not data:
            logger.warning("No anomaly records to save")
            # Create empty file with headers
            headers = ['order_id', 'product_name', 'category', 'quantity', 'unit_price', 
                      'discount_percent', 'region', 'sale_date', 'customer_email', 'revenue']
            self._write_csv(file_path, headers, [])
            return str(file_path)
        
        # Get headers from the first record
        headers = list(data[0].keys())
        
        # Format the data, handling datetime objects
        formatted_data = []
        for record in data:
            formatted_record = {}
            for key, value in record.items():
                if hasattr(value, 'strftime'):  # datetime object
                    formatted_record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_record[key] = value
            formatted_data.append(formatted_record)
        
        self._write_csv(file_path, headers, formatted_data)
        return str(file_path)

    def _save_summary(self, summary_data: Dict) -> str:
        """Save aggregation summary as JSON."""
        file_path = self.output_dir / "aggregation_summary.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Summary saved to {file_path}")
        return str(file_path)

    def _write_csv(self, file_path: Path, headers: List[str], data_items: List[Dict]) -> None:
        """Write data to CSV file."""
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data_items)
            
            logger.info(f"Saved {len(data_items)} records to {file_path}")
            
        except Exception as e:
            logger.error(f"Error writing CSV file {file_path}: {e}")
            raise

    def create_data_dictionary(self) -> str:
        """Create a data dictionary explaining all output files."""
        file_path = self.output_dir / "DATA_DICTIONARY.md"
        
        content = """# Data Dictionary

This document describes the structure and content of all generated data files.

## Files Overview

### 1. monthly_sales_summary.csv
Monthly aggregated sales data showing trends over time.

| Column | Type | Description |
|--------|------|-------------|
| month | string | YYYY-MM format (e.g., "2024-01") |
| revenue | float | Total revenue for the month |
| quantity | integer | Total units sold |
| avg_discount | float | Average discount rate (0.0-1.0) |

### 2. top_products.csv
Top performing products ranked by revenue.

| Column | Type | Description |
|--------|------|-------------|
| product_name | string | Standardized product name |
| revenue | float | Total revenue generated |
| quantity | integer | Total units sold |

### 3. region_wise_performance.csv
Sales performance broken down by geographic region.

| Column | Type | Description |
|--------|------|-------------|
| region | string | Geographic region (North, South, East, West) |
| revenue | float | Total revenue for the region |
| quantity | integer | Total units sold in the region |

### 4. category_discount_map.csv
Discount analysis by product category.

| Column | Type | Description |
|--------|------|-------------|
| category | string | Product category |
| avg_discount | float | Average discount rate for the category |

### 5. anomaly_records.csv
High-value transactions flagged as potential anomalies.

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier |
| product_name | string | Product name |
| category | string | Product category |
| quantity | integer | Number of units |
| unit_price | float | Price per unit |
| discount_percent | float | Discount rate applied |
| region | string | Sales region |
| sale_date | datetime | Date of sale |
| customer_email | string | Customer email (anonymized if missing) |
| revenue | float | Calculated revenue |

### 6. aggregation_summary.json
Summary statistics about the data processing.

Contains counts of records processed, unique values, and other metadata.

## Data Quality Notes

- All monetary values are in the original currency units
- Dates are standardized to YYYY-MM-DD HH:MM:SS format
- Missing or invalid records are excluded from aggregations
- Discount rates are normalized to 0.0-1.0 range
- Anonymous customers are marked as "anonymous"
"""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Data dictionary created at {file_path}")
        return str(file_path)
