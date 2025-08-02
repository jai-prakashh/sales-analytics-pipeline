# ========================
# src/pipeline/ingestion.py
# ========================

"""
Data Ingestion Module

Handles memory-efficient reading of large CSV files using chunked processing.
"""

import csv
import logging

logger = logging.getLogger(__name__)

class CSVReader:
    """
    A memory-efficient CSV reader that reads a file in chunks.
    This is crucial for handling large files (100M+ rows) without
    overloading system memory.
    """
    
    def __init__(self, file_path):
        """
        Initialize the CSV reader.
        
        Args:
            file_path (str): Path to the CSV file to read
        """
        self.file_path = file_path
        self.header = []
        logger.info(f"Initialized CSVReader for file: {file_path}")

    def read_in_chunks(self, chunk_size):
        """
        A generator that yields a list of dictionaries for each chunk of data.

        Args:
            chunk_size (int): The number of rows to yield per chunk.
        
        Yields:
            list[dict]: A list of dictionaries representing a chunk of rows.
        """
        try:
            with open(self.file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.header = reader.fieldnames
                logger.info(f"CSV header: {self.header}")
                
                chunk = []
                row_count = 0
                
                for row in reader:
                    chunk.append(row)
                    row_count += 1
                    
                    if len(chunk) == chunk_size:
                        logger.debug(f"Yielding chunk with {len(chunk)} rows")
                        yield chunk
                        chunk = []
                
                # Yield any remaining rows in the last chunk
                if chunk:
                    logger.debug(f"Yielding final chunk with {len(chunk)} rows")
                    yield chunk
                
                logger.info(f"Total rows processed: {row_count}")
                
        except FileNotFoundError:
            logger.error(f"File '{self.file_path}' was not found")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
