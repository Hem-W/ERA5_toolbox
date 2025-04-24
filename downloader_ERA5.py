# Download ERA5 data from CDS API
import traceback
import pathlib
import cdsapi
from multiprocessing import Pool, Manager
import os
import time
import threading
import queue
import logging, datetime
import sys
from tqdm import tqdm
import urllib3
import json5

# Script version
__version__ = "0.1.0"

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

VB_MAP = {"2m_temperature": "t2m", "total_precipitation": "tp", 
          "10m_u_component_of_wind": "u10", "10m_v_component_of_wind": "v10", 
          "100m_u_component_of_wind": "u100", "100m_v_component_of_wind": "v100",
          "surface_solar_radiation_downwards": "ssrd",
          "toa_incident_solar_radiation": "tisr",
          "potential_evaporation": "pev", 
          "mean_sea_level_pressure": "msl",
          "geopotential": "z", "u_component_of_wind": "u", "v_component_of_wind": "v"}

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
            logger.info(f"Resuming download from byte {file_size}")
        
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
            logger.warning(f"Download incomplete: got {progress} bytes out of {total_size}")
            return False
            
        logger.info(f"Successfully downloaded {target_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading with urllib3: {str(e)}")
        return False

def download_with_client(client, year, variable, dataset="reanalysis-era5-single-levels", pressure_level=None, skip_existing=True):
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
    """
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
    
    # Add pressure level for pressure-levels dataset
    if dataset == "reanalysis-era5-pressure-levels":
        if pressure_level is None:
            raise ValueError("pressure_level is required for reanalysis-era5-pressure-levels dataset")
        request["pressure_level"] = [pressure_level]
        target = f"era5.reanalysis.{VB_MAP.get(variable) or variable}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
    else:
        target = f"era5.reanalysis.{VB_MAP.get(variable) or variable}.1hr.0p25deg.global.{year}.nc"
    
    if skip_existing and pathlib.Path(target).exists():
        logger.info(f"Skip existing file {target}")
        return
    
    logger.info(f"Requesting {target}")
    
    try:
        # Retrieve the data
        result = client.retrieve(dataset, request)

        # Store the URL for potential fallback use
        download_url = None
        try:
            download_url = result.location
        except (AttributeError, Exception):
            logger.warning(f"Could not get direct download URL, only use standard download")
        
        try:
            # Use the regular download method (original behavior)
            result.download(target)
            logger.info(f"Successfully downloaded {target} via cdsapi")
            return
        except Exception as e:
            logger.error(f"Standard download failed: {str(e)}")
            
            # Check if the error is network-related
            if ("urllib3.exceptions.ProtocolError" in str(e) or 
                "IncompleteRead" in str(e) or 
                "ChunkedEncodingError" in str(e) or
                "ConnectionError" in str(e) or
                "timeout" in str(e).lower()):
                
                # Try direct download if we have the URL
                if download_url:
                    logger.info(f"Attempting fallback download with urllib3: {download_url}")
                    
                    # Try up to 3 times with urllib3
                    for attempt in range(1, 4):
                        if attempt > 1:
                            logger.info(f"Retry attempt {attempt}/3 for {target}")
                        
                        success = download_file_with_urllib3(download_url, target)
                        
                        if success:
                            logger.info(f"Successfully downloaded {target} using urllib3 as fallback")
                            return  # Exit function successfully
                        
                        # Wait before retry (exponential backoff)
                        if attempt < 3:
                            wait_time = 60 * (2 ** (attempt - 1))
                            logger.info(f"Waiting {wait_time} seconds before next attempt")
                            time.sleep(wait_time)
                    
                    logger.error(f"All download attempts failed for {target}")
                else:
                    logger.error("No direct download URL available for fallback")
                
                # If we reach here, the download failed
                if os.path.exists(target):
                    try:
                        os.remove(target)
                        logger.info(f"Deleted small/broken file {target} due to failed downloads")
                    except Exception as del_err:
                        logger.error(f"Failed to delete broken file {target}: {str(del_err)}")
            
            # Re-raise the original exception
            raise
            
    except Exception as e:
        logger.error(f"Error downloading {year}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def key_worker(key, task_queue):
    """Worker that processes multiple tasks with a single environment setup"""
    key_display = key[:8] + '...' if key else 'default'
    logger.info(f"Worker for key {key_display} started")
    
    try:
        # Create client directly with API key instead of using environment variables
        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=key)
        
        # Process tasks from queue
        while True:
            try:
                task = task_queue.get(timeout=10)
                if task is None:  # Stop signal
                    logger.info(f"Worker for key {key_display} received stop signal")
                    task_queue.task_done()
                    break
                    
                # Process task with existing client
                year, variable = task[0], task[1]
                dataset = task[3] if len(task) > 3 else "reanalysis-era5-single-levels"
                pressure_level = task[4] if len(task) > 4 else None
                
                logger.info(f"Worker for key {key_display} processing year {year}")
                
                try:
                    # Process download without creating new environment each time
                    download_with_client(client, year, variable, dataset, pressure_level)
                    logger.info(f"Worker for key {key_display} completed year {year}")
                except Exception as e:
                    logger.error(f"Worker for key {key_display} error processing year {year}: {str(e)}")
                
                task_queue.task_done()
                
            except queue.Empty:
                # If queue is empty for a while, check if we should exit
                if task_queue.empty():
                    logger.info(f"Worker for key {key_display} found empty queue, exiting")
                    break
                # Otherwise continue waiting for new tasks
                continue
            except Exception as e:
                logger.error(f"Worker for key {key_display} encountered error: {str(e)}")
                # Continue processing other tasks
    finally:
        logger.info(f"Worker for key {key_display} finished and cleaned up")

def start_key_workers(key, shared_task_queue, num_workers=2):
    """Start multiple worker threads for a single API key"""
    # key_display = key[:8] + '...' if key else 'default'
    # logger.info(f"Starting {num_workers} workers for key {key_display}")
    
    # Create multiple worker threads for this key
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=key_worker, args=(key, shared_task_queue))
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
    years = range(1940, 2025)
    var = "u_component_of_wind"
    dataset = "reanalysis-era5-pressure-levels"
    pressure_level = "500"
    
    # Load API keys from JSON file
    cdsapi_keys = load_api_keys()

    # Number of workers per key
    workers_per_key = 2
    
    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    logger.info(f"Years: {years.start} to {years.stop-1}")
    logger.info(f"Variable: {var}")
    logger.info(f"Dataset: {dataset}")
    if pressure_level:
        logger.info(f"Pressure Level: {pressure_level} hPa")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Workers per key: {workers_per_key}")
    logger.info("=================================")
    
    logger.info("Starting ERA5 download process with dynamic task assignment")
    
    # Track start time
    start_time = time.time()
    
    # Create a Manager for sharing resources across processes
    with Manager() as manager:
        # Create a shared task queue
        shared_task_queue = manager.Queue()
        
        # Fill the queue with all year tasks (without keys)
        for year in years:
            # Create task without key - key will be added by the worker
            shared_task_queue.put((year, var, None, dataset, pressure_level))
        
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