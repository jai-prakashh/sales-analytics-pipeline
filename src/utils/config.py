# ========================
# src/utils/config.py
# ========================

"""
Configuration Management

Centralized configuration for the data pipeline with environment support.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional

class Config:
    """
    Configuration class for the data pipeline.
    Supports environment variables and default values.
    """
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration.
        
        Args:
            config_dict (dict): Optional configuration overrides
        """
        # Data Processing Configuration
        self.DEFAULT_CHUNK_SIZE = int(os.getenv('PIPELINE_CHUNK_SIZE', '1000'))
        self.MAX_MEMORY_USAGE_MB = int(os.getenv('PIPELINE_MAX_MEMORY_MB', '512'))
        
        # File Paths
        self.DEFAULT_INPUT_FILE = os.getenv('PIPELINE_INPUT_FILE', 'data/raw/sales_data.csv')
        self.DEFAULT_OUTPUT_DIR = os.getenv('PIPELINE_OUTPUT_DIR', 'data/processed')
        self.DASHBOARD_DATA_DIR = os.getenv('DASHBOARD_DATA_DIR', 'dashboard_app/transformed_data')
        
        # Data Generation Settings
        self.DEFAULT_SAMPLE_ROWS = int(os.getenv('SAMPLE_ROWS', '10000'))
        self.LARGE_DATASET_ROWS = int(os.getenv('LARGE_DATASET_ROWS', '100000000'))
        
        # Business Logic Thresholds
        self.MAX_DISCOUNT_PERCENT = float(os.getenv('MAX_DISCOUNT', '1.0'))
        self.MIN_QUANTITY = int(os.getenv('MIN_QUANTITY', '1'))
        self.MIN_UNIT_PRICE = float(os.getenv('MIN_UNIT_PRICE', '0.01'))
        
        # Performance Settings
        self.ANOMALY_RECORDS_LIMIT = int(os.getenv('ANOMALY_LIMIT', '5'))
        self.TOP_PRODUCTS_LIMIT = int(os.getenv('TOP_PRODUCTS_LIMIT', '10'))
        
        # Dashboard Settings
        self.DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8000'))
        
        # Logging Configuration
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_CHUNK_INTERVAL = int(os.getenv('LOG_CHUNK_INTERVAL', '100'))
        
        # Regional Settings
        self.SUPPORTED_REGIONS = ['North', 'South', 'East', 'West']
        self.CURRENCY_SYMBOL = os.getenv('CURRENCY_SYMBOL', '₹')
        self.LOCALE = os.getenv('LOCALE', 'en-IN')
        
        # Data Quality Settings
        self.MIN_DATA_QUALITY_RATE = float(os.getenv('MIN_DATA_QUALITY_RATE', '0.4'))  # 40%
        self.ENABLE_DATA_PROFILING = os.getenv('ENABLE_DATA_PROFILING', 'true').lower() == 'true'
        
        # Override with provided config
        if config_dict:
            self._update_from_dict(config_dict)
    
    def _update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """Update configuration from dictionary."""
        for key, value in config_dict.items():
            if hasattr(self, key.upper()):
                setattr(self, key.upper(), value)
    
    def get_data_paths(self) -> Dict[str, Path]:
        """Get all configured data paths as Path objects."""
        return {
            'input_file': Path(self.DEFAULT_INPUT_FILE),
            'output_dir': Path(self.DEFAULT_OUTPUT_DIR),
            'dashboard_data_dir': Path(self.DASHBOARD_DATA_DIR),
            'raw_data_dir': Path('data/raw'),
            'processed_data_dir': Path('data/processed'),
            'logs_dir': Path('logs')
        }
    
    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        paths = self.get_data_paths()
        for path_name, path in paths.items():
            if path_name.endswith('_dir'):
                path.mkdir(parents=True, exist_ok=True)
    
    def validate_config(self) -> Dict[str, bool]:
        """
        Validate configuration values.
        
        Returns:
            dict: Validation results for each setting
        """
        validations = {}
        
        # Validate numeric ranges
        validations['chunk_size'] = self.DEFAULT_CHUNK_SIZE > 0
        validations['memory_limit'] = self.MAX_MEMORY_USAGE_MB > 0
        validations['sample_rows'] = self.DEFAULT_SAMPLE_ROWS > 0
        validations['discount_range'] = 0.0 <= self.MAX_DISCOUNT_PERCENT <= 1.0
        validations['quantity_min'] = self.MIN_QUANTITY >= 0
        validations['price_min'] = self.MIN_UNIT_PRICE >= 0
        validations['anomaly_limit'] = self.ANOMALY_RECORDS_LIMIT > 0
        validations['top_products_limit'] = self.TOP_PRODUCTS_LIMIT > 0
        validations['dashboard_port'] = 1000 <= self.DASHBOARD_PORT <= 65535
        
        # Validate log level
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        validations['log_level'] = self.LOG_LEVEL.upper() in valid_log_levels
        
        return validations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            attr: getattr(self, attr) 
            for attr in dir(self) 
            if not attr.startswith('_') and not callable(getattr(self, attr))
        }
    
    def save_to_file(self, file_path: str) -> None:
        """Save configuration to JSON file."""
        import json
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'Config':
        """Load configuration from JSON file."""
        import json
        with open(file_path, 'r') as f:
            config_dict = json.load(f)
        return cls(config_dict)
    
    def __str__(self) -> str:
        """String representation of configuration."""
        lines = ["Configuration Settings:"]
        config_dict = self.to_dict()
        for key, value in sorted(config_dict.items()):
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)
