# ========================
# src/pipeline/__init__.py
# ========================

"""
Data Pipeline Package

This package contains all core components for the scalable data engineering pipeline:
- ingestion: Memory-efficient CSV reading
- cleaning: Data validation and normalization  
- transformation: In-memory aggregations
- storage: Output management
- orchestrator: Pipeline coordination
"""

from .ingestion import CSVReader
from .cleaning import DataCleaner
from .transformation import DataAggregator
from .storage import DataSaver
from .orchestrator import DataPipeline

__all__ = [
    'CSVReader',
    'DataCleaner', 
    'DataAggregator',
    'DataSaver',
    'DataPipeline'
]

__version__ = "1.0.0"
