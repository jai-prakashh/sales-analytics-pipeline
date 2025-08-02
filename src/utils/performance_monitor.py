# ========================
# src/utils/performance_monitor.py
# ========================

"""
Performance Monitoring Utilities

Provides performance monitoring and profiling for the data pipeline.
"""

import time
import os
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to import psutil, but provide fallback if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available. Memory monitoring will be limited.")

class PerformanceMonitor:
    """
    Performance monitoring utility for the data pipeline.
    Tracks memory usage, processing time, and throughput.
    """
    
    def __init__(self, name: str = "Pipeline"):
        """
        Initialize performance monitor.
        
        Args:
            name (str): Name for this monitoring session
        """
        self.name = name
        self.start_time = None
        self.end_time = None
        self.peak_memory_mb = 0
        self.records_processed = 0
        self.chunks_processed = 0
        self.checkpoints = []
        
        logger.debug(f"PerformanceMonitor initialized: {name}")
        
    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        self.start_time = time.time()
        self.peak_memory_mb = self._get_memory_usage_mb()
        
        logger.info(f"{self.name} - Performance monitoring started")
        logger.info(f"Initial memory usage: {self.peak_memory_mb:.2f} MB")
        
    def update_progress(self, records_in_chunk: int) -> None:
        """
        Update progress tracking.
        
        Args:
            records_in_chunk (int): Number of records processed in this chunk
        """
        self.records_processed += records_in_chunk
        self.chunks_processed += 1
        current_memory = self._get_memory_usage_mb()
        self.peak_memory_mb = max(self.peak_memory_mb, current_memory)
        
        # Log progress periodically
        if self.chunks_processed % 100 == 0:
            self._log_progress(current_memory)
    
    def add_checkpoint(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a performance checkpoint.
        
        Args:
            name (str): Checkpoint name
            metadata (dict): Optional metadata to store
        """
        checkpoint = {
            'name': name,
            'timestamp': time.time(),
            'memory_mb': self._get_memory_usage_mb(),
            'records_processed': self.records_processed,
            'chunks_processed': self.chunks_processed,
            'metadata': metadata or {}
        }
        self.checkpoints.append(checkpoint)
        logger.debug(f"Checkpoint '{name}': {checkpoint}")
    
    def _log_progress(self, current_memory: float) -> None:
        """Log current progress."""
        if self.start_time:
            elapsed = time.time() - self.start_time
            throughput = self.records_processed / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"{self.name} - Progress: {self.chunks_processed} chunks, "
                f"{self.records_processed:,} records, "
                f"{throughput:.0f} records/sec, "
                f"Memory: {current_memory:.2f} MB"
            )
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """
        Stop monitoring and return performance summary.
        
        Returns:
            dict: Performance statistics
        """
        self.end_time = time.time()
        total_time = self.end_time - self.start_time if self.start_time else 0
        throughput = self.records_processed / total_time if total_time > 0 else 0
        
        summary = {
            'name': self.name,
            'total_processing_time_seconds': total_time,
            'records_processed': self.records_processed,
            'chunks_processed': self.chunks_processed,
            'average_throughput_records_per_second': throughput,
            'peak_memory_usage_mb': self.peak_memory_mb,
            'memory_efficiency_records_per_mb': self.records_processed / self.peak_memory_mb if self.peak_memory_mb > 0 else 0,
            'checkpoints': self.checkpoints
        }
        
        self._print_summary(summary)
        return summary
    
    def _print_summary(self, summary: Dict[str, Any]) -> None:
        """Print formatted performance summary."""
        print("\n" + "="*60)
        print(f"PERFORMANCE SUMMARY - {summary['name']}")
        print("="*60)
        print(f"Total processing time: {summary['total_processing_time_seconds']:.2f} seconds")
        print(f"Records processed: {summary['records_processed']:,}")
        print(f"Chunks processed: {summary['chunks_processed']:,}")
        print(f"Average throughput: {summary['average_throughput_records_per_second']:.0f} records/second")
        print(f"Peak memory usage: {summary['peak_memory_usage_mb']:.2f} MB")
        
        if summary['memory_efficiency_records_per_mb'] > 0:
            print(f"Memory efficiency: {summary['memory_efficiency_records_per_mb']:.0f} records/MB")
        else:
            print("Memory efficiency: Unable to calculate (memory monitoring unavailable)")
        
        if summary['checkpoints']:
            print(f"Checkpoints recorded: {len(summary['checkpoints'])}")
        
        print("="*60)
        
    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process(os.getpid())
                memory_bytes = process.memory_info().rss
                memory_mb = memory_bytes / (1024 * 1024)
                # Return at least 1 MB to avoid division by zero
                return max(memory_mb, 1.0)
            except Exception as e:
                logger.debug(f"Could not get memory usage: {e}")
                return 10.0  # Fallback value
        else:
            # Fallback: return a reasonable default
            return 10.0  # Assume 10 MB baseline
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        current_time = time.time()
        elapsed = current_time - self.start_time if self.start_time else 0
        current_memory = self._get_memory_usage_mb()
        
        return {
            'elapsed_seconds': elapsed,
            'records_processed': self.records_processed,
            'chunks_processed': self.chunks_processed,
            'current_memory_mb': current_memory,
            'peak_memory_mb': self.peak_memory_mb,
            'current_throughput': self.records_processed / elapsed if elapsed > 0 else 0
        }

@contextmanager
def monitor_performance(name: str = "Pipeline"):
    """
    Context manager for easy performance monitoring.
    
    Args:
        name (str): Name for this monitoring session
        
    Yields:
        PerformanceMonitor: Monitor instance
    """
    monitor = PerformanceMonitor(name)
    monitor.start_monitoring()
    try:
        yield monitor
    finally:
        monitor.stop_monitoring()

class SystemResourceMonitor:
    """Monitor system-wide resource usage."""
    
    @staticmethod
    def get_system_stats() -> Dict[str, Any]:
        """Get current system resource statistics."""
        stats = {}
        
        if PSUTIL_AVAILABLE:
            try:
                # CPU usage
                stats['cpu_percent'] = psutil.cpu_percent(interval=1)
                stats['cpu_count'] = psutil.cpu_count()
                
                # Memory usage
                memory = psutil.virtual_memory()
                stats['memory_total_gb'] = memory.total / (1024**3)
                stats['memory_available_gb'] = memory.available / (1024**3)
                stats['memory_used_percent'] = memory.percent
                
                # Disk usage
                disk = psutil.disk_usage('/')
                stats['disk_total_gb'] = disk.total / (1024**3)
                stats['disk_free_gb'] = disk.free / (1024**3)
                stats['disk_used_percent'] = (disk.used / disk.total) * 100
                
            except Exception as e:
                logger.warning(f"Could not get system stats: {e}")
        
        return stats
    
    @staticmethod
    def check_resource_availability(min_memory_gb: float = 1.0, 
                                  min_disk_gb: float = 1.0) -> Dict[str, bool]:
        """
        Check if system has sufficient resources.
        
        Args:
            min_memory_gb (float): Minimum required memory in GB
            min_disk_gb (float): Minimum required disk space in GB
            
        Returns:
            dict: Resource availability status
        """
        system_stats = SystemResourceMonitor.get_system_stats()
        
        checks = {
            'sufficient_memory': True,
            'sufficient_disk': True,
            'cpu_available': True
        }
        
        if system_stats:
            if 'memory_available_gb' in system_stats:
                checks['sufficient_memory'] = system_stats['memory_available_gb'] >= min_memory_gb
            
            if 'disk_free_gb' in system_stats:
                checks['sufficient_disk'] = system_stats['disk_free_gb'] >= min_disk_gb
            
            if 'cpu_percent' in system_stats:
                checks['cpu_available'] = system_stats['cpu_percent'] < 90  # CPU not overloaded
        
        return checks
