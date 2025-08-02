# ========================
# src/utils/job_metadata.py
# ========================

"""
Job Metadata Management

Handles persistent storage and discovery of pipeline job metadata.
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


class JobMetadataManager:
    """Manages persistent job metadata storage."""
    
    def __init__(self, metadata_file: str = "data/job_metadata.json"):
        self.metadata_file = Path(metadata_file)
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
    
    def save_job_metadata(self, job_status_dict: Dict[str, Dict[str, Any]]) -> None:
        """Save all job metadata to persistent storage."""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(job_status_dict, f, indent=2, default=str)
            logger.debug(f"Saved job metadata for {len(job_status_dict)} jobs")
        except Exception as e:
            logger.error(f"Failed to save job metadata: {e}")
    
    def load_job_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Load job metadata from persistent storage."""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                logger.info(f"Loaded metadata for {len(data)} persisted jobs")
                return data
            return {}
        except Exception as e:
            logger.error(f"Failed to load job metadata: {e}")
            return {}
    
    def discover_existing_jobs(self) -> Dict[str, Dict[str, Any]]:
        """Discover jobs from existing data directories."""
        discovered_jobs = {}
        
        # Check processed data directory for job-specific folders
        processed_dir = Path("data/processed")
        uploaded_dir = Path("data/uploaded")
        
        if processed_dir.exists():
            for job_dir in processed_dir.iterdir():
                if job_dir.is_dir() and self._is_valid_uuid(job_dir.name):
                    job_id = job_dir.name
                    
                    # Look for corresponding uploaded file
                    input_file = None
                    filename = "unknown_file.csv"
                    
                    if uploaded_dir.exists():
                        for uploaded_file in uploaded_dir.iterdir():
                            if uploaded_file.name.startswith(job_id):
                                input_file = str(uploaded_file)
                                filename = uploaded_file.name.replace(f"{job_id}_", "")
                                break
                    
                    # Check if aggregation_summary.json exists to determine completion
                    summary_file = job_dir / "aggregation_summary.json"
                    status = "completed" if summary_file.exists() else "unknown"
                    
                    # Try to get file modification time as completion time
                    try:
                        completed_at = datetime.fromtimestamp(
                            summary_file.stat().st_mtime if summary_file.exists() 
                            else job_dir.stat().st_mtime
                        ).isoformat()
                    except Exception:
                        completed_at = None
                    
                    # Create job metadata
                    discovered_jobs[job_id] = {
                        'job_id': job_id,
                        'filename': filename,
                        'status': status,
                        'created_at': completed_at,  # Use completion time as best guess
                        'completed_at': completed_at,
                        'input_file': input_file or f"data/uploaded/{job_id}_{filename}",
                        'output_dir': str(job_dir),
                        'chunk_size': 1000,  # Default value
                        'type': 'discovered',
                        'discovered_on_startup': True
                    }
                    
                    # Try to read results from aggregation_summary.json
                    if summary_file.exists():
                        try:
                            with open(summary_file, 'r') as f:
                                summary_data = json.load(f)
                            
                            # Create results structure
                            discovered_jobs[job_id]['results'] = {
                                'processing_stats': summary_data,
                                'saved_files': self._get_saved_files(job_dir),
                                'data_quality_stats': {
                                    'success_rate': 95.0  # Default estimate
                                }
                            }
                        except Exception as e:
                            logger.warning(f"Could not read summary for job {job_id}: {e}")
        
        if discovered_jobs:
            logger.info(f"Discovered {len(discovered_jobs)} existing jobs from data directories")
        
        return discovered_jobs
    
    def _is_valid_uuid(self, uuid_string: str) -> bool:
        """Check if string is a valid UUID."""
        try:
            uuid.UUID(uuid_string)
            return True
        except ValueError:
            return False
    
    def _get_saved_files(self, job_dir: Path) -> Dict[str, str]:
        """Get dictionary of saved files for a job."""
        saved_files = {}
        
        # Map file patterns to types
        file_mappings = {
            'monthly_sales_summary.csv': 'monthly_sales_summary',
            'top_products.csv': 'top_products',
            'region_wise_performance.csv': 'region_wise_performance',
            'anomaly_records.csv': 'anomaly_records',
            'category_discount_map.csv': 'category_discount_map',
            'aggregation_summary.json': 'summary'
        }
        
        for filename, file_type in file_mappings.items():
            file_path = job_dir / filename
            if file_path.exists():
                saved_files[file_type] = str(file_path)
        
        return saved_files
