# ========================
# src/pipeline/transformation.py
# ========================

"""
Data Transformation Module

Performs in-memory aggregations on cleaned data chunks for analytical insights.
"""

import logging
from collections import defaultdict
from operator import itemgetter
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class DataAggregator:
    """
    Performs in-memory aggregations on cleaned data chunks.
    Designed to be memory-efficient by only storing summary data.
    """
    
    def __init__(self, anomaly_limit: int = 5, top_products_limit: int = 10):
        """
        Initialize the data aggregator.
        
        Args:
            anomaly_limit (int): Number of top anomaly records to keep
            top_products_limit (int): Number of top products to track
        """
        self.anomaly_limit = anomaly_limit
        self.top_products_limit = top_products_limit
        
        # Initialize aggregation data structures
        self._reset_aggregations()
        logger.info(f"DataAggregator initialized with anomaly_limit={anomaly_limit}, top_products_limit={top_products_limit}")
    
    def _reset_aggregations(self):
        """Reset all aggregation data structures."""
        self.monthly_sales = defaultdict(lambda: {
            'revenue': 0.0, 
            'quantity': 0, 
            'total_discount': 0.0, 
            'num_transactions': 0
        })
        
        self.product_sales = defaultdict(lambda: {
            'revenue': 0.0, 
            'quantity': 0
        })
        
        self.region_sales = defaultdict(lambda: {
            'revenue': 0.0, 
            'quantity': 0
        })
        
        self.category_discounts = defaultdict(lambda: {
            'total_discount_revenue': 0.0, 
            'total_revenue': 0.0, 
            'num_transactions': 0
        })
        
        self.anomaly_records = []
        self.records_processed = 0
        
        # Statistics for anomaly detection
        self.revenue_stats = {'total': 0.0, 'count': 0, 'values': []}
        self.quantity_stats = {'total': 0, 'count': 0, 'values': []}
        self.price_stats = {'total': 0.0, 'count': 0, 'values': []}

    def process_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        """
        Process a cleaned chunk and update all aggregated data structures.

        Args:
            chunk (list[dict]): A list of cleaned and valid records.
        """
        logger.debug(f"Processing chunk with {len(chunk)} records")
        
        for record in chunk:
            try:
                self._process_single_record(record)
                self.records_processed += 1
            except Exception as e:
                logger.warning(f"Error processing record: {e}, Record: {record}")
                continue
        
        logger.debug(f"Chunk processed. Total records so far: {self.records_processed}")

    def _process_single_record(self, record: Dict[str, Any]) -> None:
        """Process a single record and update aggregations."""
        # Validate required fields
        required_fields = ['quantity', 'unit_price', 'discount_percent', 'sale_date', 'revenue']
        if not all(record.get(field) is not None for field in required_fields):
            raise ValueError("Missing required fields in record")
        
        # Extract values
        revenue = record['revenue']
        quantity = record['quantity']
        unit_price = record['unit_price']
        discount_percent = record['discount_percent']
        product_name = record.get('product_name', 'Unknown')
        region = record.get('region', 'Unknown')
        category = record.get('category', 'Unknown')
        
        # Update statistics for anomaly detection
        self._update_statistics(revenue, quantity, unit_price)
        
        # Generate month key
        month_key = record['sale_date'].strftime("%Y-%m")
        
        # Update monthly sales
        self.monthly_sales[month_key]['revenue'] += revenue
        self.monthly_sales[month_key]['quantity'] += quantity
        self.monthly_sales[month_key]['total_discount'] += discount_percent
        self.monthly_sales[month_key]['num_transactions'] += 1
        
        # Update product sales
        self.product_sales[product_name]['revenue'] += revenue
        self.product_sales[product_name]['quantity'] += quantity
        
        # Update regional sales
        self.region_sales[region]['revenue'] += revenue
        self.region_sales[region]['quantity'] += quantity
        
        # Update category discounts
        self.category_discounts[category]['total_discount_revenue'] += revenue * discount_percent
        self.category_discounts[category]['total_revenue'] += revenue
        self.category_discounts[category]['num_transactions'] += 1
        
        # Check for anomalies using multiple criteria
        self._detect_and_update_anomalies(record)

    def _update_statistics(self, revenue: float, quantity: int, unit_price: float) -> None:
        """Update running statistics for anomaly detection."""
        # Update revenue stats
        self.revenue_stats['total'] += revenue
        self.revenue_stats['count'] += 1
        if len(self.revenue_stats['values']) < 1000:  # Keep sample for percentile calculation
            self.revenue_stats['values'].append(revenue)
        
        # Update quantity stats
        self.quantity_stats['total'] += quantity
        self.quantity_stats['count'] += 1
        if len(self.quantity_stats['values']) < 1000:
            self.quantity_stats['values'].append(quantity)
        
        # Update price stats
        self.price_stats['total'] += unit_price
        self.price_stats['count'] += 1
        if len(self.price_stats['values']) < 1000:
            self.price_stats['values'].append(unit_price)

    def _detect_and_update_anomalies(self, record: Dict[str, Any]) -> None:
        """
        Detect anomalies using multiple criteria and update anomaly records.
        """
        revenue = record['revenue']
        quantity = record['quantity']
        unit_price = record['unit_price']
        discount = record['discount_percent']

        anomaly_score = 0
        anomaly_reasons = []

        # Statistical-based anomalies
        if self.records_processed > 100:
            score, reasons = self._statistical_anomaly_checks(revenue, quantity, unit_price)
            anomaly_score += score
            anomaly_reasons.extend(reasons)

        # Discount-based anomalies
        score, reasons = self._discount_anomaly_checks(unit_price, discount)
        anomaly_score += score
        anomaly_reasons.extend(reasons)

        # Revenue threshold anomaly
        score, reasons = self._revenue_threshold_anomaly(revenue)
        anomaly_score += score
        anomaly_reasons.extend(reasons)

        # Extreme combination anomaly
        score, reasons = self._extreme_combination_anomaly(quantity, unit_price)
        anomaly_score += score
        anomaly_reasons.extend(reasons)

        # Record as anomaly if score is high enough
        if anomaly_score >= 2:
            anomaly_record = record.copy()
            anomaly_record['anomaly_score'] = anomaly_score
            anomaly_record['anomaly_reasons'] = '; '.join(anomaly_reasons)
            self._update_anomaly_records(anomaly_record, anomaly_score, revenue)

    def _statistical_anomaly_checks(self, revenue, quantity, unit_price):
        score = 0
        reasons = []
        avg_revenue = self.revenue_stats['total'] / self.revenue_stats['count']
        if revenue > avg_revenue * 10:
            score += 3
            reasons.append(f"Extremely high revenue: ${revenue:,.2f} (avg: ${avg_revenue:,.2f})")
        avg_quantity = self.quantity_stats['total'] / self.quantity_stats['count']
        if quantity > avg_quantity * 5:
            score += 2
            reasons.append(f"Unusually high quantity: {quantity} (avg: {avg_quantity:.1f})")
        avg_price = self.price_stats['total'] / self.price_stats['count']
        if unit_price > avg_price * 5:
            score += 2
            reasons.append(f"Extremely high unit price: ${unit_price:,.2f} (avg: ${avg_price:,.2f})")
        return score, reasons

    def _discount_anomaly_checks(self, unit_price, discount):
        score = 0
        reasons = []
        if discount > 0.7:
            score += 1
            reasons.append(f"Suspicious high discount: {discount*100:.1f}%")
        if unit_price > 1000 and discount > 0.5:
            score += 2
            reasons.append(f"High-value item (${unit_price:,.2f}) with large discount ({discount*100:.1f}%)")
        return score, reasons

    def _revenue_threshold_anomaly(self, revenue):
        score = 0
        reasons = []
        if revenue > 50000:
            score += 1
            reasons.append(f"High-value transaction: ${revenue:,.2f}")
        return score, reasons

    def _extreme_combination_anomaly(self, quantity, unit_price):
        score = 0
        reasons = []
        if quantity > 100 and unit_price > 500:
            score += 2
            reasons.append(f"Large quantity ({quantity}) of expensive items (${unit_price:,.2f})")
        return score, reasons

    def _update_anomaly_records(self, anomaly_record, anomaly_score, revenue):
        # Maintain top N anomalies by score, then by revenue
        if len(self.anomaly_records) < self.anomaly_limit:
            self.anomaly_records.append(anomaly_record)
            self.anomaly_records.sort(key=lambda x: (x['anomaly_score'], x['revenue']), reverse=True)
        elif (anomaly_score > self.anomaly_records[-1]['anomaly_score'] or 
              (anomaly_score == self.anomaly_records[-1]['anomaly_score'] and 
               revenue > self.anomaly_records[-1]['revenue'])):
            self.anomaly_records.pop()
            self.anomaly_records.append(anomaly_record)
            self.anomaly_records.sort(key=lambda x: (x['anomaly_score'], x['revenue']), reverse=True)

    def finalize_aggregations(self) -> None:
        """
        Perform final calculations and formatting after all chunks are processed.
        """
        logger.info("Finalizing aggregations...")
        
        # Calculate average discount for monthly sales
        for month, data in self.monthly_sales.items():
            if data['num_transactions'] > 0:
                data['avg_discount'] = data['total_discount'] / data['num_transactions']
            else:
                data['avg_discount'] = 0.0
            # Remove temporary fields
            data.pop('total_discount', None)
            data.pop('num_transactions', None)
        
        # Calculate average discount for category discounts
        for category, data in self.category_discounts.items():
            if data['total_revenue'] > 0:
                data['avg_discount'] = data['total_discount_revenue'] / data['total_revenue']
            else:
                data['avg_discount'] = 0.0
            # Remove temporary fields
            data.pop('total_discount_revenue', None)
            data.pop('total_revenue', None)
            data.pop('num_transactions', None)

        # Generate top products list
        self.top_products = sorted(
            self.product_sales.items(),
            key=lambda item: item[1]['revenue'],
            reverse=True
        )[:self.top_products_limit]
        
        logger.info(f"Aggregation complete. Processed {self.records_processed} records")
        self._log_summary_statistics()
    
    def _log_summary_statistics(self) -> None:
        """Log summary statistics of the aggregations."""
        logger.info(f"Monthly sales periods: {len(self.monthly_sales)}")
        logger.info(f"Unique products: {len(self.product_sales)}")
        logger.info(f"Regions: {len(self.region_sales)}")
        logger.info(f"Categories: {len(self.category_discounts)}")
        logger.info(f"Anomaly records: {len(self.anomaly_records)}")
        
        # Log anomaly detection statistics
        if len(self.anomaly_records) > 0:
            avg_anomaly_score = sum(r.get('anomaly_score', 0) for r in self.anomaly_records) / len(self.anomaly_records)
            max_anomaly_revenue = max(r['revenue'] for r in self.anomaly_records)
            logger.info(f"Average anomaly score: {avg_anomaly_score:.1f}")
            logger.info(f"Highest anomaly revenue: ${max_anomaly_revenue:,.2f}")
        
        # Log overall statistics for context
        if self.revenue_stats['count'] > 0:
            avg_revenue = self.revenue_stats['total'] / self.revenue_stats['count']
            logger.info(f"Average transaction revenue: ${avg_revenue:,.2f}")
            anomaly_rate = len(self.anomaly_records) / self.records_processed * 100
            logger.info(f"Anomaly detection rate: {anomaly_rate:.3f}%")
    
    def get_aggregation_summary(self) -> Dict[str, Any]:
        """Get a summary of all aggregations."""
        return {
            'records_processed': self.records_processed,
            'monthly_periods': len(self.monthly_sales),
            'unique_products': len(self.product_sales),
            'regions': len(self.region_sales),
            'categories': len(self.category_discounts),
            'anomaly_records': len(self.anomaly_records),
            'top_products': len(self.top_products) if hasattr(self, 'top_products') else 0
        }
