#!/usr/bin/env python3
"""
Example script for downloading CEMS Fire Historical data using the extended downloader framework

This script demonstrates how to use the updated downloader_ERA5.py to download
CEMS Fire Historical data instead of ERA5 data.

Author: Hui-Min Wang
"""

import sys
import os

# Add the current directory to Python path to import downloader_ERA5
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from downloader_ERA5 import *

if __name__ == '__main__':
    ####################
    # CEMS Fire Historical Configuration
    ####################
    
    # Years to download
    years = range(2000, 2001)  # Start with a small range for testing
    
    # Variables available in CEMS Fire Historical dataset
    variables = ['fire_weather_index']
    
    # Dataset name
    dataset = "cems-fire-historical-v1"
    
    # No pressure levels for CEMS Fire
    pressure_levels = None
    
    # Short names for variables (required for skip_existing)
    short_names = {'fire_weather_index': 'fwi'}
    
    # Custom request parameters (optional - uses defaults from DATASET_CONFIGS if None)
    custom_request = None
    
    # Common settings
    api_keys_file = None  # Use default 'cdsapi_keys.json'
    workers_per_key = 2   # Number of workers per key
    skip_existing = True  # Whether to skip downloading existing files
    
    ####################
    # Program Execution
    ####################
    
    # Load API keys from JSON file
    cdsapi_keys = load_api_keys(api_keys_file)
    
    # Validate configuration
    if skip_existing and not short_names:
        logger.warning("skip_existing is True but no short_names provided. This may cause files to be re-downloaded.")
    
    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== CEMS Fire Historical Download Configuration ===")
    logger.info(f"Years: {years.start} to {years.stop-1}")
    variables = [variables] if isinstance(variables, str) else variables
    logger.info(f"Variables: {', '.join(variables)}")
    logger.info(f"Dataset: {dataset}")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Workers per key: {workers_per_key}")
    logger.info(f"Skip existing files: {skip_existing}")
    if short_names:
        logger.info(f"Using short names: {short_names}")
    logger.info("=================================")
    
    logger.info("Starting CEMS Fire Historical download process")
    
    # Track start time
    start_time = time.time()
    
    # Create a Manager for sharing resources across processes
    with Manager() as manager:
        # Create a shared task queue
        shared_task_queue = manager.Queue()
        
        # Fill the queue with all year-variable combinations
        for year, var in product(years, variables):
            # Get short_name if available
            var_short_name = short_names.get(var) if short_names else None
            # Create task with custom_request parameter
            shared_task_queue.put((year, var, None, dataset, None, var_short_name, skip_existing, custom_request))
        
        logger.info(f"Initialized shared task queue with {shared_task_queue.qsize()} tasks")
        
        # Start workers for each key
        all_threads = []
        for key in cdsapi_keys:
            key_threads = start_key_workers(key, shared_task_queue, workers_per_key)
            all_threads.extend(key_threads)
        
        # Wait for all threads to complete
        for t in all_threads:
            t.join()
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"Download process completed in {elapsed_time:.2f} seconds")
