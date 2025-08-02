# ========================
# api_server.py
# ========================

"""
FastAPI Server for Sales Data Pipeline

Provides REST API endpoints for uploading sales data files and triggering the pipeline.
"""

import os
import logging
import asyncio
import tempfile
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

from src.pipeline.orchestrator import DataPipeline
from src.utils.config import Config
from src.utils.logging_setup import setup_logging
from src.utils.job_metadata import JobMetadataManager

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Initialize job metadata manager
job_metadata_manager = JobMetadataManager()

# Initialize FastAPI app
app = FastAPI(
    title="Sales Data Pipeline API",
    description="Upload and process sales data through a scalable data pipeline",
    version="1.0.0"
)

# Add CORS middleware to allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for dashboard
dashboard_path = Path(__file__).parent / "dashboard_app"
if dashboard_path.exists():
    app.mount("/dashboard", StaticFiles(directory=str(dashboard_path), html=True), name="dashboard")
    logger.info(f"Dashboard mounted at /dashboard from {dashboard_path}")
else:
    logger.warning(f"Dashboard directory not found: {dashboard_path}")

# Add endpoint for job-specific data files (before StaticFiles mount conflicts)
@app.get("/job-data/{job_id}/{filename}")
async def get_job_data_file(job_id: str, filename: str):
    """Serve job-specific data files for dashboard charts."""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail=JOB_NOT_COMPLETED_MSG)
    
    # Get the job's output directory
    output_dir = Path(job['output_dir'])
    file_path = output_dir / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {filename} not found for job {job_id}")
    
    return FileResponse(path=file_path, media_type='text/csv')

# Job-specific interactive dashboard (use different path to avoid StaticFiles conflict)
@app.get("/job-dashboard/{job_id}")
async def get_job_interactive_dashboard(job_id: str):
    """
    Get interactive dashboard for a specific job (same as main dashboard but with job data).
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        HTMLResponse: Interactive dashboard page with charts for the specific job
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Job not completed yet. Please wait for processing to finish.")
    
    if 'results' not in job:
        raise HTTPException(status_code=404, detail="No results available for this job")
    
    # Read the main dashboard HTML template
    dashboard_template_path = Path(__file__).parent / "dashboard_app" / "index.html"
    if not dashboard_template_path.exists():
        raise HTTPException(status_code=500, detail="Dashboard template not found")
    
    with open(dashboard_template_path, 'r') as f:
        dashboard_html = f.read()
    
    # Modify the dashboard to use job-specific data
    # Replace the data loading URLs to point to job-specific endpoints
    modified_html = dashboard_html.replace(
        'transformed_data/', 
        f'/job-data/{job_id}/'
    )
    
    # Update the title to show job information
    job_title = f"Job {job_id[:8]}... Dashboard - {job['filename']}"
    modified_html = modified_html.replace(
        '<title>E-Commerce Sales Dashboard</title>',
        f'<title>{job_title}</title>'
    )
    
    # Add job information to the header
    header_addition = f'''
        <div class="mt-4 p-4 bg-blue-50 rounded-lg">
            <h3 class="font-semibold text-blue-900">Job Information</h3>
            <p class="text-blue-700"><strong>Job ID:</strong> <code>{job_id}</code></p>
            <p class="text-blue-700"><strong>File:</strong> {job['filename']}</p>
            <p class="text-blue-700"><strong>Completed:</strong> {job.get('completed_at', 'N/A')}</p>
        </div>
        '''
    
    # Insert job info after the main header
    modified_html = modified_html.replace(
        '<p class="mt-2 text-gray-600">Key insights from the sales data engineering pipeline.</p>',
        f'<p class="mt-2 text-gray-600">Key insights from the sales data engineering pipeline.</p>{header_addition}'
    )
    
    # Add navigation back to main dashboard
    nav_addition = '''
        <div class="mb-4">
            <a href="/dashboard/" class="bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors">
                ← Back to Main Dashboard
            </a>
            <a href="/jobs" class="ml-2 bg-gray-600 text-white px-4 py-2 rounded-lg hover:bg-gray-700 transition-colors">
                View All Jobs
            </a>
        </div>
        '''
    
    # Insert navigation before the loading indicator
    modified_html = modified_html.replace(
        '<!-- Loading indicator -->',
        f'{nav_addition}<!-- Loading indicator -->'
    )
    
    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=modified_html)

# Initialize job status from persistent storage and discovery
def initialize_job_status() -> Dict[str, Dict[str, Any]]:
    """Initialize job status by loading from metadata and discovering existing jobs."""
    # Load saved metadata first
    job_status = job_metadata_manager.load_job_metadata()

    # Discover jobs from data directories that might not be in metadata
    discovered_jobs = job_metadata_manager.discover_existing_jobs()

    # Merge discovered jobs (only add if not already in metadata)
    for job_id, job_data in discovered_jobs.items():
        if job_id not in job_status:
            job_status[job_id] = job_data
            logger.info(f"Added discovered job {job_id}: {job_data['filename']}")

    # Save the merged state
    if job_status:
        job_metadata_manager.save_job_metadata(job_status)

    return job_status

# Global state for tracking jobs
job_status: Dict[str, Dict[str, Any]] = initialize_job_status()
executor = ThreadPoolExecutor(max_workers=3)  # Limit concurrent pipeline jobs

def persist_job_status():
    """Save current job status to persistent storage."""
    job_metadata_manager.save_job_metadata(job_status)

# Configuration
config = Config()
UPLOAD_DIR = Path("data/uploaded")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Constants
JOB_NOT_FOUND_MSG = "Job not found"
JOB_NOT_COMPLETED_MSG = "Job not completed yet"

class PipelineJobManager:
    """Manages background pipeline jobs."""
    
    @staticmethod
    def run_pipeline(job_id: str, input_file: str, output_dir: str, chunk_size: int = 1000) -> None:
        """Run pipeline in background thread."""
        try:
            logger.info(f"Starting pipeline job {job_id}")
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['started_at'] = datetime.now().isoformat()
            
            # Create pipeline and run
            pipeline = DataPipeline(
                input_file=input_file,
                output_dir=output_dir,
                chunk_size=chunk_size,
                config=config
            )
            
            # Validate input
            if not pipeline.validate_input():
                raise ValueError("Input file validation failed")
            
            # Run pipeline
            results = pipeline.run()
            
            # Copy data to dashboard directory for all jobs
            dashboard_dir = config.DASHBOARD_DATA_DIR
            PipelineJobManager._copy_data_for_dashboard(output_dir, dashboard_dir)
            
            # Update job status
            job_status[job_id]['status'] = 'completed'
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            job_status[job_id]['results'] = results
            
            # Persist job status
            persist_job_status()
            
            logger.info(f"Pipeline job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline job {job_id} failed: {e}")
            job_status[job_id]['status'] = 'failed'
            job_status[job_id]['error'] = str(e)
            job_status[job_id]['failed_at'] = datetime.now().isoformat()
            # Persist job status
            persist_job_status()
    
    @staticmethod
    def run_main_pipeline(job_id: str, num_rows: int, chunk_size: int) -> None:
        """Run main pipeline (equivalent to main.py) in background thread."""
        try:
            logger.info(f"Starting main pipeline job {job_id} with {num_rows} rows")
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['started_at'] = datetime.now().isoformat()
            
            # Import main pipeline functionality
            from src.utils.data_generator import DataGenerator
            
            # Generate sample data
            input_file = "data/raw/sales_data.csv"
            generator = DataGenerator(seed=42)
            generation_stats = generator.generate_dataset(
                file_path=input_file,
                num_rows=num_rows,
                error_rate=0.15
            )
            
            # Run pipeline
            output_dir = config.DEFAULT_OUTPUT_DIR
            dashboard_dir = config.DASHBOARD_DATA_DIR
            
            pipeline = DataPipeline(
                input_file=input_file,
                output_dir=output_dir,
                chunk_size=chunk_size,
                config=config
            )
            
            if not pipeline.validate_input():
                raise ValueError("Input file validation failed")
            
            results = pipeline.run()
            
            # Copy data to dashboard directory
            PipelineJobManager._copy_data_for_dashboard(output_dir, dashboard_dir)
            
            # Update job status
            job_status[job_id]['status'] = 'completed'
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            job_status[job_id]['results'] = results
            job_status[job_id]['generation_stats'] = generation_stats
            
            # Persist job status
            persist_job_status()
            
            logger.info(f"Main pipeline job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Main pipeline job {job_id} failed: {e}")
            job_status[job_id]['status'] = 'failed'
            job_status[job_id]['error'] = str(e)
            job_status[job_id]['failed_at'] = datetime.now().isoformat()
            # Persist job status
            persist_job_status()
    
    @staticmethod
    def run_large_scale_test(job_id: str, num_rows: int) -> None:
        """Run large scale test in background thread."""
        try:
            logger.info(f"Starting large scale test job {job_id} with {num_rows} rows")
            job_status[job_id]['status'] = 'processing'
            job_status[job_id]['started_at'] = datetime.now().isoformat()
            
            # Import required modules
            from src.utils.data_generator import DataGenerator
            from src.utils.performance_monitor import monitor_performance
            
            # Generate large dataset
            input_file = f"data/raw/large_sales_data_{num_rows}.csv"
            generator = DataGenerator(seed=42)
            
            with monitor_performance() as monitor:
                generation_stats = generator.generate_dataset(
                    file_path=input_file,
                    num_rows=num_rows,
                    error_rate=0.15
                )
            
            # Run pipeline with performance monitoring
            output_dir = "data/processed"
            chunk_size = max(1000, num_rows // 100)  # Dynamic chunk size based on data size
            
            pipeline = DataPipeline(
                input_file=input_file,
                output_dir=output_dir,
                chunk_size=chunk_size,
                config=config
            )
            
            if not pipeline.validate_input():
                raise ValueError("Input file validation failed")
            
            results = pipeline.run()
            
            # Copy data to dashboard directory
            dashboard_dir = config.DASHBOARD_DATA_DIR
            PipelineJobManager._copy_data_for_dashboard(output_dir, dashboard_dir)
            
            # Update job status
            job_status[job_id]['status'] = 'completed'
            job_status[job_id]['completed_at'] = datetime.now().isoformat()
            job_status[job_id]['results'] = results
            job_status[job_id]['generation_stats'] = generation_stats
            
            # Persist job status
            persist_job_status()
            
            logger.info(f"Large scale test job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Large scale test job {job_id} failed: {e}")
            job_status[job_id]['status'] = 'failed'
            job_status[job_id]['error'] = str(e)
            job_status[job_id]['failed_at'] = datetime.now().isoformat()
            # Persist job status
            persist_job_status()
    
    @staticmethod
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
            logger.info(f"Copied {csv_file.name} to dashboard directory")
        
        # Copy JSON files (like aggregation_summary.json)
        json_files = output_path.glob("*.json")
        for json_file in json_files:
            dest_file = dashboard_path / json_file.name
            shutil.copy2(json_file, dest_file)
            logger.info(f"Copied {json_file.name} to dashboard directory")
        
        # Copy MD files (like DATA_DICTIONARY.md)
        md_files = output_path.glob("*.md")
        for md_file in md_files:
            dest_file = dashboard_path / md_file.name
            shutil.copy2(md_file, dest_file)
            logger.info(f"Copied {md_file.name} to dashboard directory")

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Sales Data Pipeline API",
        "version": "1.0.0",
        "endpoints": {
            "upload": "/upload - Upload CSV file",
            "run_pipeline": "/run-pipeline - Run main pipeline (like main.py)",
            "run_large_test": "/run-large-scale-test - Run large scale test",
            "status": "/status/{job_id} - Check job status",
            "jobs": "/jobs - List all jobs",
            "health": "/health - Health check",
            "dashboard": "/dashboard - Main interactive dashboard",
            "job_dashboard": "/job-dashboard/{job_id} - Interactive dashboard for specific job",
            "job_summary": "/dashboard-view/{job_id} - Job summary with downloads",
            "api_docs": "/docs - API documentation"
        },
        "dashboard_url": "/dashboard",
        "api_docs_url": "/docs",
        "quick_start": {
            "run_main_pipeline": "POST /run-pipeline",
            "run_large_test": "POST /run-large-scale-test?num_rows=100000", 
            "upload_file": "POST /upload (with file)",
            "view_dashboard": "GET /dashboard",
            "view_job_dashboard": "GET /job-dashboard/{job_id}",
            "view_job_summary": "GET /dashboard-view/{job_id}"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": len([j for j in job_status.values() if j['status'] == 'processing'])
    }

@app.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    chunk_size: int = Query(1000, description="Number of rows to process per chunk", ge=100, le=10000)
):
    """
    Upload a CSV file and trigger the data pipeline.
    
    Args:
        file: CSV file to upload
        chunk_size: Number of rows to process per chunk (100-10000)
        
    Returns:
        dict: Job ID and status information
    """
    try:
        # Validate file type
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save uploaded file
        file_path = UPLOAD_DIR / f"{job_id}_{file.filename}"
        content = await file.read()
        
        # Write file synchronously in a thread (FastAPI handles this properly)
        def write_file():
            with open(file_path, "wb") as buffer:
                buffer.write(content)
        
        # Execute file write in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, write_file)
        
        # Create output directory for this job
        output_dir = Path("data/processed") / job_id
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize job status
        job_status[job_id] = {
            'job_id': job_id,
            'filename': file.filename,
            'status': 'queued',
            'created_at': datetime.now().isoformat(),
            'input_file': str(file_path),
            'output_dir': str(output_dir),
            'chunk_size': chunk_size,
            'file_size': len(content)
        }
        
        # Persist job status
        persist_job_status()
        
        # Start pipeline in background
        background_tasks.add_task(
            PipelineJobManager.run_pipeline,
            job_id,
            str(file_path),
            str(output_dir),
            chunk_size
        )
        
        logger.info(f"Started pipeline job {job_id} for file {file.filename}")
        
        return {
            "job_id": job_id,
            "filename": file.filename,
            "status": "queued",
            "message": "File uploaded successfully. Pipeline processing started.",
            "estimated_processing_info": "Use /status/{job_id} to check progress"
        }
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status of a pipeline job.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        dict: Job status and results
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id].copy()
    
    # Add additional info for completed jobs
    if job['status'] == 'completed' and 'results' in job:
        results = job['results']
        job['summary'] = {
            'records_processed': results.get('processing_stats', {}).get('records_processed', 0),
            'output_files': len(results.get('saved_files', {})),
            'data_quality_rate': results.get('data_quality_stats', {}).get('success_rate', 0)
        }
    
    return job

@app.get("/jobs")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status: queued, processing, completed, failed"),
    limit: int = Query(50, description="Maximum number of jobs to return", ge=1, le=100)
):
    """
    List all pipeline jobs with optional filtering.
    
    Args:
        status: Filter jobs by status
        limit: Maximum number of jobs to return
        
    Returns:
        dict: List of jobs
    """
    jobs = list(job_status.values())
    
    # Filter by status if specified
    if status:
        jobs = [job for job in jobs if job['status'] == status]
    
    # Sort by creation time (newest first)
    jobs.sort(key=lambda x: x['created_at'], reverse=True)
    
    # Limit results
    jobs = jobs[:limit]
    
    return {
        "jobs": jobs,
        "total_count": len(job_status),
        "filtered_count": len(jobs)
    }

@app.get("/download/{job_id}")
async def download_results(job_id: str, file_type: str = Query(..., description="Type of file to download")):
    """
    Download processed results for a completed job.
    
    Args:
        job_id: Unique job identifier
        file_type: Type of file to download (e.g., 'monthly_sales_summary', 'top_products', etc.)
        
    Returns:
        FileResponse: The requested file
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail=JOB_NOT_COMPLETED_MSG)
    
    if 'results' not in job or 'saved_files' not in job['results']:
        raise HTTPException(status_code=404, detail="No results available")
    
    saved_files = job['results']['saved_files']
    if file_type not in saved_files:
        available_types = list(saved_files.keys())
        raise HTTPException(
            status_code=404, 
            detail=f"File type '{file_type}' not found. Available types: {available_types}"
        )
    
    file_path = Path(saved_files[file_type])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")
    
    return FileResponse(
        path=file_path,
        filename=f"{job_id}_{file_type}.{file_path.suffix.lstrip('.')}",
        media_type='application/octet-stream'
    )

@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a job and its associated files.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        dict: Deletion status
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    
    try:
        # Remove input file
        input_file = Path(job['input_file'])
        if input_file.exists():
            input_file.unlink()
        
        # Remove output directory
        output_dir = Path(job['output_dir'])
        if output_dir.exists():
            import shutil
            shutil.rmtree(output_dir)
        
        # Remove from job status
        del job_status[job_id]
        
        # Persist the updated job status
        persist_job_status()
        
        logger.info(f"Deleted job {job_id} and associated files")
        
        return {
            "message": f"Job {job_id} and associated files deleted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to delete job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

@app.get("/dashboard-data/{job_id}")
async def get_dashboard_data(job_id: str):
    """
    Get dashboard data for a completed job (JSON API).
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        dict: Dashboard data
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail=JOB_NOT_COMPLETED_MSG)
    
    if 'results' not in job:
        raise HTTPException(status_code=404, detail="No results available")
    
    results = job['results']
    
    # Extract key metrics for dashboard
    processing_stats = results.get('processing_stats', {})
    quality_stats = results.get('data_quality_stats', {})
    
    dashboard_data = {
        'job_info': {
            'job_id': job_id,
            'filename': job['filename'],
            'completed_at': job.get('completed_at'),
            'processing_time': job.get('completed_at')  # Could calculate duration
        },
        'metrics': {
            'records_processed': processing_stats.get('records_processed', 0),
            'data_quality_rate': quality_stats.get('success_rate', 0),
            'anomalies_detected': processing_stats.get('anomaly_records', 0),
            'top_products_count': processing_stats.get('top_products', 0)
        },
        'available_files': list(results.get('saved_files', {}).keys()),
        'data_quality_details': quality_stats
    }
    
    return dashboard_data

@app.get("/dashboard-view/{job_id}")
async def get_job_dashboard_view(job_id: str):
    """
    Get job-specific dashboard view with data files.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        HTMLResponse: Job-specific dashboard page
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail=JOB_NOT_FOUND_MSG)
    
    job = job_status[job_id]
    if job['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Job not completed yet. Please wait for processing to finish.")
    
    if 'results' not in job:
        raise HTTPException(status_code=404, detail="No results available for this job")
    
    # Create a job-specific dashboard HTML
    from fastapi.responses import HTMLResponse
    
    results = job['results']
    processing_stats = results.get('processing_stats', {})
    quality_stats = results.get('data_quality_stats', {})
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Job {job_id[:8]}... Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            body {{ font-family: 'Inter', sans-serif; }}
            .chart-container {{ position: relative; height: 300px; width: 100%; }}
        </style>
    </head>
    <body class="bg-gray-100 text-gray-800 p-8">
        <div class="max-w-7xl mx-auto space-y-8">
            
            <!-- Header with Job Info -->
            <header class="bg-white p-6 rounded-2xl shadow-lg">
                <div class="flex justify-between items-start">
                    <div>
                        <h1 class="text-4xl font-bold text-gray-900">Job Dashboard</h1>
                        <p class="mt-2 text-gray-600">Results for Job ID: <code class="bg-gray-100 px-2 py-1 rounded">{job_id}</code></p>
                        <p class="mt-1 text-sm text-gray-500">File: {job['filename']} | Completed: {job.get('completed_at', 'N/A')}</p>
                    </div>
                    <div class="text-right">
                        <a href="/dashboard/" class="bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition-colors">
                            ← Back to Main Dashboard
                        </a>
                    </div>
                </div>
            </header>

            <!-- Key Metrics -->
            <section class="grid grid-cols-1 md:grid-cols-4 gap-6">
                <div class="bg-white p-6 rounded-2xl shadow-lg text-center">
                    <div class="text-3xl font-bold text-indigo-600">{processing_stats.get('records_processed', 0):,}</div>
                    <div class="text-gray-600 mt-2">Records Processed</div>
                </div>
                <div class="bg-white p-6 rounded-2xl shadow-lg text-center">
                    <div class="text-3xl font-bold text-green-600">{quality_stats.get('success_rate', 0):.1f}%</div>
                    <div class="text-gray-600 mt-2">Data Quality</div>
                </div>
                <div class="bg-white p-6 rounded-2xl shadow-lg text-center">
                    <div class="text-3xl font-bold text-purple-600">{processing_stats.get('unique_products', 0)}</div>
                    <div class="text-gray-600 mt-2">Unique Products</div>
                </div>
                <div class="bg-white p-6 rounded-2xl shadow-lg text-center">
                    <div class="text-3xl font-bold text-orange-600">{processing_stats.get('regions', 0)}</div>
                    <div class="text-gray-600 mt-2">Regions</div>
                </div>
            </section>

            <!-- Data Files and Downloads -->
            <section class="bg-white p-6 rounded-2xl shadow-lg">
                <h2 class="text-2xl font-semibold mb-4">Generated Data Files</h2>
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
    """
    
    # Add download links for each file
    saved_files = results.get('saved_files', {})
    for file_type, file_path in saved_files.items():
        if file_type != 'summary':  # Skip JSON summary in download section
            file_name = file_type.replace('_', ' ').title()
            html_content += f"""
                    <div class="border border-gray-200 rounded-lg p-4 hover:border-indigo-300 transition-colors">
                        <h3 class="font-semibold text-gray-900">{file_name}</h3>
                        <p class="text-sm text-gray-600 mt-1">CSV format</p>
                        <a href="/download/{job_id}?file_type={file_type}" 
                           class="mt-3 inline-block bg-indigo-600 text-white px-3 py-1 rounded text-sm hover:bg-indigo-700 transition-colors">
                           Download
                        </a>
                    </div>
            """
    
    html_content += """
                </div>
            </section>

            <!-- Processing Details -->
            <section class="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div class="bg-white p-6 rounded-2xl shadow-lg">
                    <h2 class="text-2xl font-semibold mb-4">Processing Statistics</h2>
                    <div class="space-y-3">
    """
    
    # Add processing stats
    for key, value in processing_stats.items():
        if isinstance(value, (int, float)):
            display_key = key.replace('_', ' ').title()
            if isinstance(value, float):
                html_content += f'<div class="flex justify-between"><span class="text-gray-600">{display_key}:</span><span class="font-semibold">{value:.2f}</span></div>'
            else:
                html_content += f'<div class="flex justify-between"><span class="text-gray-600">{display_key}:</span><span class="font-semibold">{value:,}</span></div>'
    
    html_content += """
                    </div>
                </div>
                
                <div class="bg-white p-6 rounded-2xl shadow-lg">
                    <h2 class="text-2xl font-semibold mb-4">Data Quality</h2>
                    <div class="space-y-3">
    """
    
    # Add quality stats
    for key, value in quality_stats.items():
        if isinstance(value, (int, float)):
            display_key = key.replace('_', ' ').title()
            if 'rate' in key.lower():
                html_content += f'<div class="flex justify-between"><span class="text-gray-600">{display_key}:</span><span class="font-semibold">{value:.2f}%</span></div>'
            elif isinstance(value, float):
                html_content += f'<div class="flex justify-between"><span class="text-gray-600">{display_key}:</span><span class="font-semibold">{value:.2f}</span></div>'
            else:
                html_content += f'<div class="flex justify-between"><span class="text-gray-600">{display_key}:</span><span class="font-semibold">{value:,}</span></div>'
    
    html_content += """
                    </div>
                </div>
            </section>
            
            <!-- API Links -->
            <section class="bg-white p-6 rounded-2xl shadow-lg">
                <h2 class="text-2xl font-semibold mb-4">API Access</h2>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                    <div>
                        <h3 class="font-semibold text-gray-900">Job Status API</h3>
                        <code class="bg-gray-100 p-2 rounded block mt-2 break-all">GET /status/{job_id}</code>
                    </div>
                    <div>
                        <h3 class="font-semibold text-gray-900">Interactive Dashboard API</h3>
                        <code class="bg-gray-100 p-2 rounded block mt-2 break-all">GET /job-dashboard/{job_id}</code>
                    </div>
                </div>
            </section>

        </div>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

@app.post("/run-pipeline")
async def run_main_pipeline(
    background_tasks: BackgroundTasks,
    num_rows: int = Query(10000, description="Number of rows to generate", ge=100, le=1000000),
    chunk_size: int = Query(1000, description="Number of rows to process per chunk", ge=100, le=10000)
):
    """
    Run the main pipeline (equivalent to running main.py).
    
    Args:
        num_rows: Number of sample rows to generate
        chunk_size: Number of rows to process per chunk
        
    Returns:
        dict: Job ID and status information
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Initialize job status
        job_status[job_id] = {
            'job_id': job_id,
            'filename': f'generated_data_{num_rows}_rows.csv',
            'status': 'queued',
            'created_at': datetime.now().isoformat(),
            'input_file': 'data/raw/sales_data.csv',
            'output_dir': 'data/processed',
            'chunk_size': chunk_size,
            'num_rows': num_rows,
            'type': 'main_pipeline'
        }
        
        # Persist job status
        persist_job_status()
        
        # Start pipeline in background
        background_tasks.add_task(
            PipelineJobManager.run_main_pipeline,
            job_id,
            num_rows,
            chunk_size
        )
        
        logger.info(f"Started main pipeline job {job_id} with {num_rows} rows")
        
        return {
            "job_id": job_id,
            "type": "main_pipeline",
            "status": "queued",
            "message": "Main pipeline started successfully.",
            "parameters": {
                "num_rows": num_rows,
                "chunk_size": chunk_size
            },
            "estimated_processing_info": "Use /status/{job_id} to check progress"
        }
        
    except Exception as e:
        logger.error(f"Failed to start main pipeline: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")

@app.post("/run-large-scale-test")
async def run_large_scale_test(
    background_tasks: BackgroundTasks,
    num_rows: int = Query(100000, description="Number of rows for large scale test", ge=10000, le=10000000)
):
    """
    Run large scale test (equivalent to running run_large_scale_test.py).
    
    Args:
        num_rows: Number of rows for large scale testing
        
    Returns:
        dict: Job ID and status information
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Initialize job status
        job_status[job_id] = {
            'job_id': job_id,
            'filename': f'large_scale_test_{num_rows}_rows.csv',
            'status': 'queued',
            'created_at': datetime.now().isoformat(),
            'input_file': f'data/raw/large_sales_data_{num_rows}.csv',
            'output_dir': 'data/processed',
            'chunk_size': 1000,  # Fixed chunk size for large scale tests
            'num_rows': num_rows,
            'type': 'large_scale_test'
        }
        
        # Persist job status
        persist_job_status()
        
        # Start pipeline in background
        background_tasks.add_task(
            PipelineJobManager.run_large_scale_test,
            job_id,
            num_rows
        )
        
        logger.info(f"Started large scale test job {job_id} with {num_rows} rows")
        
        return {
            "job_id": job_id,
            "type": "large_scale_test",
            "status": "queued",
            "message": "Large scale test started successfully.",
            "parameters": {
                "num_rows": num_rows
            },
            "estimated_processing_info": "Use /status/{job_id} to check progress"
        }
        
    except Exception as e:
        logger.error(f"Failed to start large scale test: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start test: {str(e)}")

def start_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Start the FastAPI server."""
    logger.info(f"Starting Sales Data Pipeline API server on {host}:{port}")
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )

if __name__ == "__main__":
    start_server(reload=True)
