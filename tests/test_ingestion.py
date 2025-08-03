# ========================
# tests/test_ingestion.py
# ========================

import unittest
import tempfile
import os
import sys
import csv

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.ingestion import CSVReader

class TestDataIngestion(unittest.TestCase):
    """Test the CSV ingestion module."""

    def test_csv_reader_chunked_processing(self):
        """Test that CSVReader properly chunks data."""
        # Create a temporary CSV file with test data
        test_data = [
            ['order_id', 'product_name', 'category', 'quantity', 'unit_price', 'discount_percent', 'region', 'sale_date', 'customer_email'],
            ['ORD-001', 'Product A', 'Electronics', '1', '100.0', '0.1', 'North', '2024-01-01', 'test1@example.com'],
            ['ORD-002', 'Product B', 'Electronics', '2', '200.0', '0.05', 'South', '2024-01-02', 'test2@example.com'],
            ['ORD-003', 'Product C', 'Fashion', '1', '50.0', '0.2', 'East', '2024-01-03', 'test3@example.com'],
            ['ORD-004', 'Product D', 'Fashion', '3', '75.0', '0.15', 'West', '2024-01-04', 'test4@example.com'],
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(test_data)
            temp_file_path = f.name
        
        try:
            # Test the CSV reader
            reader = CSVReader(temp_file_path)
            
            # Test chunked reading with chunk size of 2
            chunks = list(reader.read_in_chunks(chunk_size=2))
            
            # Should have 2 chunks: both with 2 records each (4 total records)
            self.assertEqual(len(chunks), 2)
            self.assertEqual(len(chunks[0]), 2)  # First chunk has 2 records
            self.assertEqual(len(chunks[1]), 2)  # Second chunk has 2 records
            
            # Verify header was read correctly
            expected_header = ['order_id', 'product_name', 'category', 'quantity', 'unit_price', 'discount_percent', 'region', 'sale_date', 'customer_email']
            self.assertEqual(reader.header, expected_header)
            
            # Verify data content
            first_record = chunks[0][0]
            self.assertEqual(first_record['order_id'], 'ORD-001')
            self.assertEqual(first_record['product_name'], 'Product A')
            
        finally:
            os.unlink(temp_file_path)

    def test_csv_reader_file_not_found(self):
        """Test CSVReader behavior with non-existent file."""
        reader = CSVReader("non_existent_file.csv")
        
        with self.assertRaises(FileNotFoundError):
            list(reader.read_in_chunks(chunk_size=10))

    def test_csv_reader_empty_file(self):
        """Test CSVReader behavior with empty CSV file."""
        # Create an empty CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_file_path = f.name
        
        try:
            reader = CSVReader(temp_file_path)
            
            # Should handle empty file gracefully
            chunks = list(reader.read_in_chunks(chunk_size=10))
            self.assertEqual(len(chunks), 0)  # No chunks for empty file
            
        finally:
            os.unlink(temp_file_path)

    def test_csv_reader_single_row(self):
        """Test CSVReader with a single data row."""
        test_data = [
            ['order_id', 'product_name', 'category'],
            ['ORD-001', 'Test Product', 'Electronics']
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(test_data)
            temp_file_path = f.name
        
        try:
            reader = CSVReader(temp_file_path)
            chunks = list(reader.read_in_chunks(chunk_size=5))
            
            # Should have 1 chunk with 1 record
            self.assertEqual(len(chunks), 1)
            self.assertEqual(len(chunks[0]), 1)
            
            record = chunks[0][0]
            self.assertEqual(record['order_id'], 'ORD-001')
            self.assertEqual(record['product_name'], 'Test Product')
            
        finally:
            os.unlink(temp_file_path)

    def test_csv_reader_large_chunk_size(self):
        """Test CSVReader with chunk size larger than data."""
        test_data = [
            ['order_id', 'product_name'],
            ['ORD-001', 'Product A'],
            ['ORD-002', 'Product B']
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(test_data)
            temp_file_path = f.name
        
        try:
            reader = CSVReader(temp_file_path)
            chunks = list(reader.read_in_chunks(chunk_size=100))  # Chunk size larger than data
            
            # Should have 1 chunk containing all records
            self.assertEqual(len(chunks), 1)
            self.assertEqual(len(chunks[0]), 2)  # All 2 records in one chunk
            
        finally:
            os.unlink(temp_file_path)

if __name__ == '__main__':
    unittest.main()
