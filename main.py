#!/usr/bin/env python3
# ========================
# main.py
# ========================

"""
Main Entry Point for Scalable Data Engineering Pipeline

This script demonstrates the complete data pipeline from data generation
through processing to dashboard-ready outputs.
"""

import sys
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.pipeline import DataPipeline
from src.utils import Config, setup_logging, DataGenerator

def main():
    """Main execution function."""
    # Initialize configuration
    config = Config()
    
    # Setup logging
    setup_logging(
        log_level=config.LOG_LEVEL,
        log_file="pipeline.log",
        log_dir="logs"
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*60)
    logger.info("SCALABLE DATA ENGINEERING PIPELINE - MAIN EXECUTION")
    logger.info("="*60)
    
    try:
        # Ensure directories exist
        config.ensure_directories()
        
        # Step 1: Generate sample data
        input_file = "data/raw/sales_data.csv"
        logger.info("Step 1: Generating sample data...")
        
        generator = DataGenerator(seed=42)  # Reproducible data
        generation_stats = generator.generate_dataset(
            file_path=input_file,
            num_rows=config.DEFAULT_SAMPLE_ROWS,
            error_rate=0.15  # 15% error rate for realistic testing
        )
        
        logger.info(f"Sample data generated: {generation_stats}")
        
        # Step 2: Configure and run the pipeline
        logger.info("Step 2: Running data pipeline...")
        
        output_dir = config.DEFAULT_OUTPUT_DIR
        dashboard_dir = config.DASHBOARD_DATA_DIR
        
        # Create pipeline instance
        pipeline = DataPipeline(
            input_file=input_file,
            output_dir=output_dir,
            chunk_size=config.DEFAULT_CHUNK_SIZE,
            config=config
        )
        
        # Validate input before processing
        if not pipeline.validate_input():
            logger.error("Input validation failed. Exiting.")
            return 1
        
        # Show processing estimates
        estimates = pipeline.estimate_processing_time()
        if estimates:
            logger.info(f"Processing estimates: {estimates}")
        
        # Run the pipeline
        results = pipeline.run()
        
        # Step 3: Copy outputs to dashboard directory (for backward compatibility)
        logger.info("Step 3: Preparing dashboard data...")
        _copy_data_for_dashboard(output_dir, dashboard_dir)
        
        # Step 4: Print summary
        logger.info("Step 4: Pipeline execution summary")
        _print_execution_summary(results, generation_stats)
        
        logger.info("Pipeline execution completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1

def _copy_data_for_dashboard(output_dir: str, dashboard_dir: str) -> None:
    """Copy processed data to dashboard directory for compatibility."""
    import shutil
    from pathlib import Path
    
    output_path = Path(output_dir)
    dashboard_path = Path(dashboard_dir)
    
    dashboard_path.mkdir(parents=True, exist_ok=True)
    
    # Copy CSV files to dashboard directory
    csv_files = output_path.glob("*.csv")
    for csv_file in csv_files:
        dest_file = dashboard_path / csv_file.name
        shutil.copy2(csv_file, dest_file)
        logging.info(f"Copied {csv_file.name} to dashboard directory")

def _print_execution_summary(results: dict, generation_stats: dict) -> None:
    """Print final execution summary."""
    print("\n" + "="*70)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*70)
    
    # Data generation summary
    print("📊 Data Generation:")
    print(f"   • Records generated: {generation_stats['total_rows']:,}")
    print(f"   • Error rate injected: {generation_stats['error_rate']:.1%}")
    print(f"   • Actual error types: {len(generation_stats['error_types'])}")
    
    # Processing summary
    processing_stats = results['processing_stats']
    quality_stats = results['data_quality_stats']
    
    print("\n🔄 Data Processing:")
    print(f"   • Records processed: {processing_stats['records_processed']:,}")
    print(f"   • Data quality rate: {quality_stats['success_rate']:.1f}%")
    print(f"   • Clean records: {quality_stats['records_cleaned']:,}")
    print(f"   • Dropped records: {quality_stats['records_dropped']:,}")
    
    # Output summary
    print("\n📁 Generated Outputs:")
    for dataset_type, file_path in results['saved_files'].items():
        if dataset_type != 'summary':  # Skip JSON summary in main display
            print(f"   • {dataset_type.replace('_', ' ').title()}: {Path(file_path).name}")
    
    # Next steps
    print("\n🚀 Next Steps:")
    print("   1. View dashboard: open dashboard_app/index.html")
    print("   2. Start web server: python scripts/serve_dashboard.py")
    print("   3. Run large scale test: python scripts/run_large_scale_test.py")
    print("   4. Check logs: logs/pipeline.log")
    
    print("="*70)

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)