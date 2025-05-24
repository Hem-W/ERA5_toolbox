"""
Download ERA5 data from CDS API

Author: Hui-Min Wang

This script downloads ERA5 data from CDS API for specified years.
"""


import traceback
import pathlib
from itertools import product
import cdsapi
from multiprocessing import Manager
import os
import time
import threading
import queue
import logging, datetime
import sys
from tqdm import tqdm
import urllib3
import json5
import netCDF4

# Script version
__version__ = "0.2.0.dev"

# Get current time for log file name
current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"era5_download_{current_time}.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ERA5_downloader")
logger.info(f"ERA5 Downloader Version: {__version__}")

# Deprecated VB_MAP - kept for backward compatibility but no longer used by default
# Will be removed in a future version
VB_MAP = {"2m_temperature": "t2m", "total_precipitation": "tp", "2m_dewpoint_temperature": "d2m", 
          "10m_u_component_of_wind": "u10", "10m_v_component_of_wind": "v10", 
          "100m_u_component_of_wind": "u100", "100m_v_component_of_wind": "v100",
          "surface_solar_radiation_downwards": "ssrd", "surface_thermal_radiation_downwards": "strd",
          "toa_incident_solar_radiation": "tisr",
          "potential_evaporation": "pev", 
          'convective_available_potential_energy': 'cape', 'convective_inhibition': 'ci',
          'mean_vertically_integrated_moisture_divergence': 'mvid',
          'volumetric_soil_water_layer_1': 'vswl1', 'volumetric_soil_water_layer_2': 'vswl2',
          "mean_sea_level_pressure": "msl", "surface_pressure": "sp",
          "geopotential": "z", "u_component_of_wind": "u", "v_component_of_wind": "v", 
          "specific_humidity": "q", "divergence": "div", "temperature": "t",
          }

def download_file_with_urllib3(url, target_path, chunk_size=1024*1024):
    """
    Download a file using urllib3 with retry and resume capabilities
    
    Args:
        url: URL to download
        target_path: Target file path
        chunk_size: Size of chunks to download (1MB default)
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    try:
        # Configure retry strategy
        retry = urllib3.Retry(
            total=10,                  # Maximum number of retries
            backoff_factor=0.5,        # Backoff factor for retries
            status_forcelist=[500, 502, 503, 504],  # Retry on these HTTP status codes
            allowed_methods=["HEAD", "GET"]        # Only retry for these methods
        )
        
        # Create connection pool with retry strategy
        http = urllib3.PoolManager(
            retries=retry,
            timeout=urllib3.Timeout(connect=30.0, read=1800.0),  # 30s connect, 30min read
            maxsize=10                 # Connection pool size
        )
        
        # Check for partial download to resume
        headers = {}
        file_size = 0
        mode = 'wb'
        
        if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            file_size = os.path.getsize(target_path)
            headers['Range'] = f'bytes={file_size}-'
            mode = 'ab'
            logger.info(f"urllib3: Resuming download from byte {file_size}")
        
        # Get content length with a HEAD request
        head_response = http.request('HEAD', url, headers=headers)
        total_size = int(head_response.headers.get('content-length', 0))
        
        if 'Range' in headers and head_response.status == 206:  # Partial content
            # For resumed download, add the existing file size to get total
            if 'content-range' in head_response.headers:
                content_range = head_response.headers.get('content-range', '')
                if content_range and '/' in content_range:
                    total_size = int(content_range.split('/')[-1])
        
        # Start download
        response = http.request(
            'GET', 
            url, 
            headers=headers, 
            preload_content=False  # Stream the response
        )
        
        # Initialize progress bar
        progress = file_size
        with tqdm(total=total_size, initial=file_size, unit='B', unit_scale=True, 
                  desc=f"Downloading {os.path.basename(target_path)}") as pbar:
            
            with open(target_path, mode) as f:
                for chunk in response.stream(chunk_size):
                    if chunk:
                        f.write(chunk)
                        progress += len(chunk)
                        pbar.update(len(chunk))
        
        response.release_conn()
        
        # Verify if download is complete
        if total_size > 0 and progress < total_size:
            logger.warning(f"urllib3: Download incomplete for {target_path}: got {progress} bytes out of {total_size}")
            return False
            
        logger.info(f"urllib3: Successfully downloaded {target_path}")
        return True
        
    except Exception as e:
        logger.error(f"urllib3: Error downloading {target_path}: {str(e)}")
        return False

def get_variable_code_from_netcdf(nc_file, api_variable):
    """
    Extract the actual variable code from a NetCDF file
    
    Args:
        nc_file: Path to the NetCDF file
        api_variable: The variable name used in the API request
        
    Returns:
        str: The variable code as stored in the NetCDF file
    """
    try:
        # Open the NetCDF file
        dataset = netCDF4.Dataset(nc_file, 'r')
        
        # Get the variable names in the file
        variables = list(dataset.variables.keys())
        
        # Close the dataset
        dataset.close()
        
        # Exclude common dimension and coordinate variables
        excluded = ['time', 'latitude', 'longitude', 'lat', 'lon', 'level']
        actual_vars = [v for v in variables if v not in excluded]
        
        if len(actual_vars) == 1:
            # If there's only one variable (excluding dimensions), use that
            return actual_vars[0]
        elif len(actual_vars) > 1:
            # If there are multiple variables, log them and use the first one
            logger.info(f"Multiple variables found in NetCDF file: {actual_vars}")
            logger.info(f"Using first variable: {actual_vars[0]}")
            return actual_vars[0]
        else:
            # Fallback to the original variable name if no clear variable is found
            logger.warning(f"No clear variable found in NetCDF file, falling back to original name")
            return api_variable
            
    except Exception as e:
        logger.error(f"Error extracting variable from NetCDF file: {str(e)}")
        logger.error(traceback.format_exc())
        # Fallback to the original variable name
        return api_variable

def download_with_client(client, year, variable, dataset="reanalysis-era5-single-levels", pressure_level=None, skip_existing=True, worker_id=None, short_name=None):
    """
    Download ERA5 data using an existing CDS client
    
    This function reuses an existing CDS client to avoid creating a new client for each download
    
    Args:
        client: Existing CDS client
        year: Year to download
        variable: Variable to download
        dataset: Dataset name
        pressure_level: Pressure level (for pressure level dataset)
        skip_existing: Whether to skip if file exists
        worker_id: Optional identifier for the worker running this download
        short_name: Optional short name for the variable to use in filename. If None, 
                   the full variable name will be used initially and then updated 
                   with the actual variable code from the file.
    """
    # Create context prefix for logs if worker_id is provided
    log_prefix = f"Worker {worker_id}: " if worker_id else ""
    
    request = {
        'product_type': ['reanalysis'],
        'variable': [variable],
        'year': [str(year) if isinstance(year, int) else year],
        'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
        'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
        'time': ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'],
        "data_format": "netcdf",
        "download_format": "unarchived"
    }
    
    # Determine what to use for the filename (short_name if provided, otherwise variable name)
    var_name_for_file = short_name if short_name is not None else variable
    
    if dataset == "reanalysis-era5-pressure-levels":
        if pressure_level is None:
            raise ValueError("pressure_level is required for reanalysis-era5-pressure-levels dataset")
        request["pressure_level"] = [pressure_level]
        target = f"era5.reanalysis.{var_name_for_file}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
    else:
        target = f"era5.reanalysis.{var_name_for_file}.1hr.0p25deg.global.{year}.nc"
    
    # Check for existing files if skip_existing is enabled
    if skip_existing:
        # For skipping existing files, short_name must be provided
        if short_name is None:
            logger.warning(f"{log_prefix}skip_existing is True but no short_name provided for {variable}. Cannot reliably check for existing files.")
        else:
            # Use the short_name directly for file pattern
            if dataset == "reanalysis-era5-pressure-levels":
                file_pattern = f"era5.reanalysis.{short_name}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
            else:
                file_pattern = f"era5.reanalysis.{short_name}.1hr.0p25deg.global.{year}.nc"
            
            # Check if file with exact short_name exists
            if os.path.exists(file_pattern):
                logger.info(f"{log_prefix}Skip existing file {file_pattern} for variable {variable}")
                return
    
    logger.info(f"{log_prefix}Requesting {target}")
    
    try:
        # Retrieve the data
        result = client.retrieve(dataset, request)

        # Store the URL for potential fallback use
        download_url = None
        try:
            download_url = result.location
        except (AttributeError, Exception):
            logger.warning(f"{log_prefix}Could not get direct download URL, only use standard download")
        
        # Try CDSAPI download first
        try:
            # Use regular download method (original behavior)
            result.download(target)
            logger.info(f"{log_prefix}Successfully downloaded {target} via cdsapi")
        except Exception as e:
            # Log error when standard download fails
            logger.error(f"{log_prefix}Download failed for {target}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Try direct download if we have the URL (for ANY error)
            if download_url:
                logger.info(f"{log_prefix}Attempting download with urllib3 for {target}: {download_url}")
                
                # Try up to 3 times with urllib3
                for attempt in range(1, 4):
                    if attempt > 1:
                        logger.info(f"{log_prefix}Retry attempt {attempt}/3 for {target} using urllib3")
                    
                    if download_file_with_urllib3(download_url, target):
                        logger.info(f"{log_prefix}Successfully downloaded {target} using urllib3")
                        break  # Exit the retry loop on success
                    
                    # Wait before retry (exponential backoff)
                    if attempt < 3:
                        wait_time = 60 * (2 ** (attempt - 1))
                        logger.info(f"{log_prefix}Waiting {wait_time} seconds before next attempt for {target} using urllib3")
                        time.sleep(wait_time)
                
                # If we've reached this point, all download attempts failed
                logger.error(f"{log_prefix}All download attempts failed for {target} using urllib3")
                return  # Exit the function since all download methods failed
            else:
                logger.error(f"{log_prefix}No direct download URL available for fallback")
                return  # Exit the function since no fallback method available
        
        # If we reach here, at least one download method succeeded
        # Only extract the variable code from the file if short_name was not provided
        if short_name is None and os.path.exists(target):
            # Now determine the actual variable code from the file
            actual_var_code = get_variable_code_from_netcdf(target, variable)
            
            # Rename the file with the correct variable code
            if dataset == "reanalysis-era5-pressure-levels":
                final_target = f"era5.reanalysis.{actual_var_code}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
            else:
                final_target = f"era5.reanalysis.{actual_var_code}.1hr.0p25deg.global.{year}.nc"
                
            # Only rename if the filenames are different
            if target != final_target:
                # Check if the final target already exists
                if os.path.exists(final_target):
                    logger.warning(f"{log_prefix}Final target {final_target} already exists, removing temporary file")
                    os.remove(target)
                else:
                    # Rename the file
                    os.rename(target, final_target)
                    logger.info(f"{log_prefix}Renamed {target} to {final_target}")
        
        return  # Exit function successfully after download and potential rename
            
    except Exception as e:
        logger.error(f"{log_prefix}Error downloading {year}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def key_worker(key, task_queue, worker_index=""):
    """Worker that processes multiple tasks with a single environment setup"""
    worker_id = f"{key[:4]}:{worker_index}"
    logger.info(f"Worker {worker_id} started")
    
    # Setup client once
    try:
        # Create client directly with API key instead of using environment variables
        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=key)
        
        # Process tasks from queue
        while True:
            try:
                task = task_queue.get(timeout=10)
                if task is None:  # Stop signal
                    logger.info(f"Worker {worker_id} received stop signal")
                    task_queue.task_done()
                    break
                
                # Unpack task - always expect short_name and skip_existing parameters
                year, variable, _, dataset, pressure_level, short_name, skip_existing = task
                
                # Process task with existing client
                try:
                    download_with_client(client, year, variable, dataset, pressure_level, skip_existing, worker_id=worker_id, short_name=short_name)
                    task_count += 1
                except Exception as e:
                    # Skip redundant error logging since download_with_client already logs errors
                    pass
                
                task_queue.task_done()
                
            except queue.Empty:
                # If queue is empty for a while, check if we should exit
                if task_queue.empty():
                    logger.info(f"Worker {worker_id} found empty queue, exiting")
                    break
                # Otherwise continue waiting for new tasks
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} encountered error: {str(e)}")
                # Continue processing other tasks
    finally:
        logger.info(f"Worker {worker_id} finished and cleaned up")

def start_key_workers(key, shared_task_queue, num_workers=2):
    """Start multiple worker threads for a single API key"""
    # Create multiple worker threads for this key
    threads = []
    for i in range(num_workers):
        # Pass worker index to distinguish between workers with the same key
        t = threading.Thread(target=key_worker, args=(key, shared_task_queue, i))
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Return the threads so we can join them later
    return threads

def load_api_keys(keys_file='cdsapi_keys.json'):
    """Load API keys from a JSON file using JSON5 parser (supports comments)
    
    The JSON file should have a format like:
    ```
    {
        // API keys for CDS
        "keys": [
            "your-api-key-1",  // Optional comment
            "your-api-key-2",  // Optional comment
        ]
    }
    ```
    
    Args:
        keys_file (str): Path to the JSON file containing API keys
        
    Returns:
        list: List of API keys
        
    Raises:
        RuntimeError: If no valid keys could be loaded
    """
    keys_file = "cdsapi_keys.json" if keys_file is None else keys_file
    try:
        if not os.path.exists(keys_file):
            logger.error(f"API keys file {keys_file} not found.")
            raise RuntimeError(f"API keys file {keys_file} not found. Please create this file with your API keys.")
            
        with open(keys_file, 'r') as f:
            try:
                data = json5.load(f)
            except Exception as e:
                logger.error(f"Error parsing {keys_file} with JSON5: {str(e)}")
                raise RuntimeError(f"Error parsing {keys_file}. Please ensure it contains valid JSON. JSON5 error: {str(e)}")
            
        if not isinstance(data, dict) or 'keys' not in data or not isinstance(data['keys'], list):
            logger.error(f"Invalid format in {keys_file}. Expected 'keys' list.")
            raise RuntimeError(f"Invalid format in {keys_file}. Expected a JSON object with a 'keys' list.")
        
        keys = data['keys']
        if not keys:
            logger.error(f"No keys found in {keys_file}")
            raise RuntimeError(f"No keys found in {keys_file}. Please add at least one API key.")
        
        logger.info(f"Successfully loaded {len(keys)} API keys from {keys_file}")
        return keys
    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        logger.error(f"Error loading API keys from {keys_file}: {str(e)}")
        raise RuntimeError(f"Error loading API keys from {keys_file}: {str(e)}")

if __name__ == '__main__':
    ####################
    # User Specification
    ####################
    years = range(2003, 2022)
    variables = ['convective_available_potential_energy', 'convective_inhibition', 
                 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'mean_vertically_integrated_moisture_divergence']
    dataset = "reanalysis-era5-single-levels"
    pressure_levels = None  # List of pressure levels (hPa)
    api_keys_file = None
    workers_per_key = 2  # Number of workers per key
    skip_existing = True  # Whether to skip downloading existing files (requires short_names if True)
    # Optional: Provide short names for variables.
    # If skip_existing=True, short_names MUST be provided for reliable operation.
    # Example: short_names = {'convective_available_potential_energy': 'cape'}
    short_names = {
        'convective_available_potential_energy': 'cape', 
        'convective_inhibition': 'cin',
        'volumetric_soil_water_layer_1': 'swvl1', 
        'volumetric_soil_water_layer_2': 'swvl2', 
        'mean_vertically_integrated_moisture_divergence': 'avg_vimdf'
    }
    
    ####################
    # Program
    ####################
    # Load API keys from JSON file
    cdsapi_keys = load_api_keys(api_keys_file)
    # Convert single variable string to list, or keep as list if already a list
    variables = [variables] if isinstance(variables, str) else variables
    # Validate configuration
    if skip_existing and not short_names:
        logger.warning("skip_existing is True but no short_names provided. This may cause files to be re-downloaded.")
    
    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    logger.info(f"Years: {years.start} to {years.stop-1}")
    logger.info(f"Variables: {', '.join(variables)}")
    logger.info(f"Dataset: {dataset}")
    # Convert single pressure level to list, or keep as list if already a list
    pressure_levels = [pressure_levels] if isinstance(pressure_levels, (int, str)) else pressure_levels
    if pressure_levels:
        logger.info(f"Pressure Levels: {', '.join(str(p) for p in pressure_levels)} hPa")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Workers per key: {workers_per_key}")
    logger.info(f"Skip existing files: {skip_existing}")
    if short_names:
        logger.info(f"Using short names: {short_names}")
    logger.info("=================================")
    
    logger.info("Starting ERA5 download process with dynamic task assignment")
    
    # Track start time
    start_time = time.time()
    
    # Create a Manager for sharing resources across processes
    with Manager() as manager:
        # Create a shared task queue
        shared_task_queue = manager.Queue()
        
        # Fill the queue with all year-variable-pressure_level combinations
        if dataset == "reanalysis-era5-pressure-levels" and pressure_levels:
            for year, var, level in product(years, variables, pressure_levels):
                # Get short_name if available
                var_short_name = short_names.get(var) if short_names else None
                # Create task without key - key will be added by the worker
                shared_task_queue.put((year, var, None, dataset, level, var_short_name, skip_existing))
        else:
            for year, var in product(years, variables):
                # Get short_name if available
                var_short_name = short_names.get(var) if short_names else None
                # Create task without key - key will be added by the worker
                shared_task_queue.put((year, var, None, dataset, None, var_short_name, skip_existing))
        
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
    logger.info(f"All download processes completed in {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}")