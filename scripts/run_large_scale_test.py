#!/usr/bin/env python3
# ========================
# scripts/run_large_scale_test.py
# ========================

"""
Script to test the pipeline with a large dataset (closer to 100M records).
This script allows you to test scalability without generating the full 100M records at once.
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.orchestrator import DataPipeline
from src.utils.data_generator import DataGenerator

def main():
    """Run a large-scale test of the data pipeline."""
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            num_rows = int(sys.argv[1])
        except ValueError:
            print("Usage: python run_large_scale_test.py [num_rows]")
            print("Example: python run_large_scale_test.py 1000000")
            sys.exit(1)
    else:
        num_rows = 1_000_000  # Default to 1M rows for testing
    
    # Configuration for large-scale processing
    input_file = 'data/raw/large_sales_data.csv'
    output_dir = 'dashboard_app/transformed_data'
    chunk_size = 10000  # Larger chunks for better performance on large data
    
    print(f"="*60)
    print(f"LARGE SCALE DATA PIPELINE TEST")
    print(f"="*60)
    print(f"Target dataset size: {num_rows:,} rows")
    print(f"Chunk size: {chunk_size:,} rows")
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print(f"="*60)

    # Step 1: Generate large sample data
    print(f"\n🔄 Step 1: Generating {num_rows:,} rows of sample data...")
    generator = DataGenerator(seed=42)
    
    if os.path.exists(input_file):
        response = input(f"File {input_file} already exists. Regenerate? (y/N): ")
        if response.lower() != 'y':
            print("Using existing data file.")
        else:
            if num_rows > 100000:
                stats = generator.generate_large_dataset_chunked(input_file, num_rows, chunk_size=10000)
            else:
                stats = generator.generate_dataset(input_file, num_rows, error_rate=0.15)
    else:
        if num_rows > 100000:
            stats = generator.generate_large_dataset_chunked(input_file, num_rows, chunk_size=10000)
        else:
            stats = generator.generate_dataset(input_file, num_rows, error_rate=0.15)
    
    # Step 2: Run the pipeline
    print("\n🔄 Step 2: Running data pipeline...")
    pipeline = DataPipeline(input_file, output_dir, chunk_size)
    pipeline.run()
    
    # Step 3: Verify outputs
    print("\n🔄 Step 3: Verifying outputs...")
    expected_files = [
        'monthly_sales_summary.csv',
        'top_products.csv', 
        'region_wise_performance.csv',
        'category_discount_map.csv',
        'anomaly_records.csv'
    ]
    
    missing_files = []
    for filename in expected_files:
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            print(f"✅ {filename}: {size:,} bytes")
        else:
            missing_files.append(filename)
            print(f"❌ {filename}: MISSING")
    
    if missing_files:
        print(f"\n⚠️  Warning: {len(missing_files)} output files are missing!")
    else:
        print("\n✅ All output files generated successfully!")
        print("\n💡 You can now open dashboard_app/index.html to view the results.")
        print("   Note: You may need to start a local web server to view the dashboard.")
        print("   Example: python -m http.server 8000 --directory dashboard_app")

if __name__ == '__main__':
    main()
