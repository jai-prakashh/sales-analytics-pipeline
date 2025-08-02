# ========================
# tests/test_pipeline.py
# ========================

import unittest
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.cleaning import DataCleaner
from src.pipeline.transformation import DataAggregator

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # Create instances of the classes to be tested
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator()

    def test_cleaner_valid_record(self):
        """
        Tests cleaning logic with a valid record.
        """
        raw_record = {
            'order_id': 'ORD-12345',
            'product_name': 'laptop pro',
            'category': 'electronics',
            'quantity': '10',
            'unit_price': 1000.0,
            'discount_percent': 0.1,
            'region': 'nort',
            'sale_date': '2024-01-15 10:00:00',
            'customer_email': 'test@example.com'
        }
        
        cleaned_record = self.cleaner.clean_record(raw_record)
        
        self.assertIsNotNone(cleaned_record)
        self.assertEqual(cleaned_record['product_name'], 'Laptop Pro 15')
        self.assertEqual(cleaned_record['region'], 'North')
        self.assertEqual(cleaned_record['quantity'], 10)
        self.assertEqual(cleaned_record['discount_percent'], 0.1)
        self.assertEqual(cleaned_record['sale_date'], datetime(2024, 1, 15, 10, 0, 0))

    def test_cleaner_invalid_quantity(self):
        """
        Tests cleaning logic with an invalid quantity.
        """
        raw_record = {
            'order_id': 'ORD-12345',
            'product_name': 'Laptop Pro',
            'category': 'electronics',
            'quantity': '-5',
            'unit_price': 1000.0,
            'discount_percent': 0.1,
            'region': 'North',
            'sale_date': '2024-01-15 10:00:00',
            'customer_email': 'test@example.com'
        }
        
        cleaned_record = self.cleaner.clean_record(raw_record)
        self.assertIsNone(cleaned_record)

    def test_cleaner_invalid_discount(self):
        """
        Tests cleaning logic with an invalid discount percentage.
        """
        raw_record = {
            'order_id': 'ORD-12345',
            'product_name': 'Laptop Pro',
            'category': 'electronics',
            'quantity': '10',
            'unit_price': 1000.0,
            'discount_percent': 1.5, # > 1.0
            'region': 'North',
            'sale_date': '2024-01-15 10:00:00',
            'customer_email': 'test@example.com'
        }
        
        cleaned_record = self.cleaner.clean_record(raw_record)
        self.assertIsNone(cleaned_record)

    def test_aggregator_logic(self):
        """
        Tests the aggregation logic with a small, clean dataset.
        """
        # A small, pre-cleaned chunk of data
        cleaned_chunk = [
            {
                'order_id': 'ORD-001', 'product_name': 'Laptop Pro 15', 'category': 'Electronics',
                'quantity': 2, 'unit_price': 1000.0, 'discount_percent': 0.1,
                'region': 'North', 'sale_date': datetime(2024, 1, 15), 'customer_email': 'test1@example.com',
                'revenue': 2 * 1000.0 * (1 - 0.1)  # 1800.0
            },
            {
                'order_id': 'ORD-002', 'product_name': 'Smartphone X', 'category': 'Electronics',
                'quantity': 1, 'unit_price': 500.0, 'discount_percent': 0.0,
                'region': 'South', 'sale_date': datetime(2024, 1, 20), 'customer_email': 'test2@example.com',
                'revenue': 1 * 500.0 * (1 - 0.0)  # 500.0 
            },
            {
                'order_id': 'ORD-003', 'product_name': 'Blender Pro', 'category': 'Home Appliance',
                'quantity': 3, 'unit_price': 50.0, 'discount_percent': 0.2,
                'region': 'North', 'sale_date': datetime(2024, 2, 5), 'customer_email': 'test3@example.com',
                'revenue': 3 * 50.0 * (1 - 0.2)  # 120.0
            },
        ]
        
        self.aggregator.process_chunk(cleaned_chunk)
        self.aggregator.finalize_aggregations()
        
        # Test monthly sales
        self.assertIn('2024-01', self.aggregator.monthly_sales)
        self.assertIn('2024-02', self.aggregator.monthly_sales)
        self.assertAlmostEqual(self.aggregator.monthly_sales['2024-01']['revenue'], (2*1000*0.9) + (1*500*1.0))
        self.assertEqual(self.aggregator.monthly_sales['2024-01']['quantity'], 3)
        self.assertAlmostEqual(self.aggregator.monthly_sales['2024-02']['revenue'], (3*50*0.8))

        # Test top products
        self.assertEqual(len(self.aggregator.top_products), 3) # Only 3 products in test data
        self.assertEqual(self.aggregator.top_products[0][0], 'Laptop Pro 15')
        self.assertAlmostEqual(self.aggregator.top_products[0][1]['revenue'], 1800.0)

        # Test region sales
        self.assertIn('North', self.aggregator.region_sales)
        self.assertIn('South', self.aggregator.region_sales)
        self.assertAlmostEqual(self.aggregator.region_sales['North']['revenue'], (2*1000*0.9) + (3*50*0.8))

    def test_date_parsing_edge_cases(self):
        """
        Tests date parsing with various formats and edge cases.
        """
        test_dates = [
            ('2024-01-15 10:00:00', datetime(2024, 1, 15, 10, 0, 0)),
            ('2024/01/15', datetime(2024, 1, 15, 0, 0, 0)),
            ('15-Jan-2024', datetime(2024, 1, 15, 0, 0, 0)),
            ('invalid-date', None),
            ('', None),
            (None, None)
        ]
        
        for input_date, expected in test_dates:
            result = self.cleaner._clean_date(input_date)
            self.assertEqual(result, expected, f"Failed for input: {input_date}")

    def test_string_standardization(self):
        """
        Tests string standardization and mapping functionality.
        """
        # Test region mapping
        region_tests = [
            ('nort', 'North'),
            ('SOUTH', 'South'),
            ('  east  ', 'East'),
            ('unknown_region', 'Unknown_Region')
        ]
        
        for input_val, expected in region_tests:
            result = self.cleaner._standardize_string(input_val, self.cleaner.REGION_MAP)
            self.assertEqual(result, expected, f"Failed for region: {input_val}")

    def test_discount_validation(self):
        """
        Tests discount validation logic with edge cases.
        """
        discount_tests = [
            (0.0, 0.0),
            (0.5, 0.5),
            (1.0, 1.0),
            (1.1, None),  # Invalid > 1.0
            (-0.1, None),  # Invalid < 0.0
            ('0.5', 0.5),  # String conversion
            ('invalid', None),
            (None, None)
        ]
        
        for input_val, expected in discount_tests:
            result = self.cleaner._clean_discount(input_val)
            self.assertEqual(result, expected, f"Failed for discount: {input_val}")

    def test_memory_efficiency_simulation(self):
        """
        Tests that the aggregator handles large chunks efficiently.
        """
        # Simulate processing multiple large chunks
        large_chunk = []
        for i in range(100):
            record = {
                'order_id': f'ORD-{i:03d}', 
                'product_name': f'Product {i % 5}', 
                'category': 'Electronics',
                'quantity': 1, 
                'unit_price': 100.0, 
                'discount_percent': 0.1,
                'region': 'North', 
                'sale_date': datetime(2024, 1, 1),
                'customer_email': f'test{i}@example.com',
                'revenue': 1 * 100.0 * (1 - 0.1)  # 90.0
            }
            large_chunk.append(record)
        
        # Process the large chunk
        self.aggregator.process_chunk(large_chunk)
        self.aggregator.finalize_aggregations()
        
        # Verify aggregation worked correctly
        self.assertEqual(len(self.aggregator.product_sales), 5)  # 5 unique products
        total_revenue = sum(data['revenue'] for data in self.aggregator.product_sales.values())
        expected_revenue = 100 * 100.0 * 0.9  # 100 records * $100 * (1-0.1 discount)
        self.assertAlmostEqual(total_revenue, expected_revenue)

if __name__ == '__main__':
    unittest.main()
