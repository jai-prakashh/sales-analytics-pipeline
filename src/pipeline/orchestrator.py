# ========================
# src/pipeline/orchestrator.py
# ========================

"""
Pipeline Orchestrator Module

Main orchestrator class that coordinates the entire data processing pipeline.
"""

import logging
from typing import Optional
from pathlib import Path

from .ingestion import CSVReader
from .cleaning import DataCleaner
from .transformation import DataAggregator
from .storage import DataSaver
from ..utils.performance_monitor import monitor_performance
from ..utils.config import Config

logger = logging.getLogger(__name__)

class DataPipeline:
    """
    Orchestrates the entire data engineering pipeline.
    Coordinates reading, cleaning, transforming, and storing data.
    """
    
    def __init__(self, 
                 input_file: str, 
                 output_dir: str, 
                 chunk_size: int = 1000,
                 config: Optional[Config] = None):
        """
        Initialize the data pipeline.
        
        Args:
            input_file (str): Path to input CSV file
            output_dir (str): Directory for output files
            chunk_size (int): Number of rows to process per chunk
            config (Config): Configuration object
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.config = config or Config()
        
        # Initialize pipeline components
        self.reader = CSVReader(self.input_file)
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator(
            anomaly_limit=self.config.ANOMALY_RECORDS_LIMIT,
            top_products_limit=self.config.TOP_PRODUCTS_LIMIT
        )
        self.saver = DataSaver(self.output_dir)
        
        logger.info("DataPipeline initialized:")
        logger.info(f"  Input: {self.input_file}")
        logger.info(f"  Output: {self.output_dir}")
        logger.info(f"  Chunk size: {self.chunk_size}")

    def run(self) -> dict:
        """
        Execute the complete pipeline from start to finish.
        
        Returns:
            dict: Summary of processing results and saved files
        """
        logger.info(f"Starting data pipeline for '{self.input_file}'...")
        
        with monitor_performance() as monitor:
            # Process data in chunks
            self._process_chunks(monitor)
            
            # Finalize aggregations
            logger.info("All chunks processed. Finalizing aggregations...")
            self.aggregator.finalize_aggregations()
            
            # Save results
            logger.info("Saving transformed data...")
            saved_files = self.saver.save_all_data(self.aggregator)
            
            # Create data dictionary
            data_dict_path = self.saver.create_data_dictionary()
            saved_files['data_dictionary'] = data_dict_path
        
        # Compile results
        results = {
            'pipeline_status': 'completed',
            'input_file': self.input_file,
            'output_directory': self.output_dir,
            'saved_files': saved_files,
            'processing_stats': self._get_processing_stats(),
            'data_quality_stats': self.cleaner.get_statistics()
        }
        
        logger.info("Pipeline finished successfully.")
        self._log_final_summary(results)
        
        return results

    def _process_chunks(self, monitor) -> None:
        """Process input data in chunks."""
        chunk_num = 0
        
        for raw_chunk in self.reader.read_in_chunks(self.chunk_size):
            chunk_num += 1
            logger.info(f"Processing chunk {chunk_num} with {len(raw_chunk)} rows...")
            
            # Clean each record in the chunk
            cleaned_chunk = []
            for record in raw_chunk:
                cleaned_record = self.cleaner.clean_record(record)
                if cleaned_record is not None:
                    cleaned_chunk.append(cleaned_record)
            
            logger.info(f"Chunk {chunk_num}: {len(cleaned_chunk)}/{len(raw_chunk)} records passed validation")
            
            # Aggregate the valid records
            if cleaned_chunk:
                self.aggregator.process_chunk(cleaned_chunk)
            
            # Update performance monitoring
            monitor.update_progress(len(raw_chunk))

    def _get_processing_stats(self) -> dict:
        """Get processing statistics."""
        aggregator_stats = self.aggregator.get_aggregation_summary()
        return {
            **aggregator_stats,
            'chunk_size': self.chunk_size,
            'input_file_size': Path(self.input_file).stat().st_size if Path(self.input_file).exists() else 0
        }

    def _log_final_summary(self, results: dict) -> None:
        """Log final pipeline summary."""
        logger.info("="*60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*60)
        
        processing_stats = results['processing_stats']
        quality_stats = results['data_quality_stats']
        
        logger.info(f"Input file: {results['input_file']}")
        logger.info(f"Records processed: {processing_stats['records_processed']:,}")
        logger.info(f"Data quality rate: {quality_stats['success_rate']:.1f}%")
        logger.info(f"Output files generated: {len(results['saved_files'])}")
        logger.info(f"Output directory: {results['output_directory']}")
        
        logger.info("\nGenerated datasets:")
        for dataset_type, file_path in results['saved_files'].items():
            logger.info(f"  • {dataset_type}: {file_path}")
        
        logger.info("="*60)

    def validate_input(self) -> bool:
        """
        Validate input file exists and is readable.
        
        Returns:
            bool: True if input is valid
        """
        input_path = Path(self.input_file)
        if not input_path.exists():
            logger.error(f"Input file does not exist: {self.input_file}")
            return False
        
        if not input_path.is_file():
            logger.error(f"Input path is not a file: {self.input_file}")
            return False
        
        try:
            with open(self.input_file, 'r') as f:
                f.readline()  # Try to read first line
        except Exception as e:
            logger.error(f"Cannot read input file: {e}")
            return False
        
        logger.info(f"Input validation passed: {self.input_file}")
        return True

    def estimate_processing_time(self) -> dict:
        """
        Estimate processing time based on file size and configuration.
        
        Returns:
            dict: Processing time estimates
        """
        try:
            file_size = Path(self.input_file).stat().st_size
            estimated_rows = file_size // 100  # Rough estimate: 100 bytes per row
            
            # Base processing rate (rows per second) - conservative estimate
            base_rate = 50000
            
            estimated_seconds = estimated_rows / base_rate
            estimated_minutes = estimated_seconds / 60
            
            return {
                'file_size_mb': file_size / (1024 * 1024),
                'estimated_rows': estimated_rows,
                'estimated_processing_time_seconds': estimated_seconds,
                'estimated_processing_time_minutes': estimated_minutes,
                'chunk_count_estimate': estimated_rows // self.chunk_size
            }
        except Exception as e:
            logger.warning(f"Could not estimate processing time: {e}")
            return {}
