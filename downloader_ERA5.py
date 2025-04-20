# Download ERA5 data from CDS API
import traceback
import pathlib
import cdsapi
from multiprocessing import Pool
import os
import tempfile
import time
import threading
import queue
import logging, datetime
import sys
from tqdm import tqdm
import urllib3
import json5

# Script version
__version__ = "0.0.2.dev"

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
          "geopotential": "z"}

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

def download_ERA5(year, variable, dataset="reanalysis-era5-single-levels", pressure_level=None, skip_existing=True, api_key=None):
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
    
    # Display only a portion of the key for security in logs
    key_display = api_key[:8] + '...' if api_key else "default"
    logger.info(f"Requesting {target} with key {key_display}")
    
    # Create a temporary .cdsapirc file with our credentials
    # Using the correct format: "url: URL" and "key: KEY"
    rc_content = f"""url: https://cds.climate.copernicus.eu/api
key: {api_key}
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cdsapirc', delete=False) as tmp:
        tmp.write(rc_content)
        tmp_path = tmp.name
        
    try:
        # Set the environment variable to point to our temporary config
        old_env = os.environ.get('CDSAPI_RC')
        os.environ['CDSAPI_RC'] = tmp_path
        
        # Now create the client which will use our temporary config
        client = cdsapi.Client()
        result = client.retrieve(dataset, request)

        # Store the URL for potential fallback use
        download_url = None
        try:
            download_url = result.location
            # logger.info(f"Got direct download URL: {download_url}")
        except (AttributeError, Exception):
            # If we can't get the URL, continue with regular download
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
                if os.path.exists(target): # and os.path.getsize(target) < 10*1024*1024:
                    try:
                        os.remove(target)
                        logger.info(f"Deleted small/broken file {target} due to failed downloads")
                    except Exception as del_err:
                        logger.error(f"Failed to delete broken file {target}: {str(del_err)}")
            
            # Re-raise the original exception
            raise
            
    finally:
        # Clean up
        if old_env:
            os.environ['CDSAPI_RC'] = old_env
        else:
            os.environ.pop('CDSAPI_RC', None)
        try:
            os.unlink(tmp_path)
        except:
            pass  # Ignore errors during cleanup

def process_year(args):
    year, variable, key = args[0], args[1], args[2]
    dataset = args[3] if len(args) > 3 else "reanalysis-era5-single-levels"
    pressure_level = args[4] if len(args) > 4 else None
    try:
        download_ERA5(year, variable, dataset=dataset, pressure_level=pressure_level, api_key=key)
    except Exception as e:
        key_display = key[:8] + '...' if key else 'default'
        logger.error(f"Error downloading {year} with key {key_display}: {str(e)}")
        logger.error(traceback.format_exc())

def worker_thread(task_queue):
    """Worker thread that processes years from a queue"""
    while True:
        try:
            task = task_queue.get(block=False)
            if task is None:  # Sentinel to stop the thread
                task_queue.task_done()
                break
                
            try:
                process_year(task)
            finally:
                task_queue.task_done()
                
        except queue.Empty:
            break

def process_with_concurrent_workers(key_years_tuple):
    """Process years for a key using two concurrent worker threads
    
    This ensures that each key has two active submissions at all times,
    maximizing throughput by allowing one request to be processed while
    another is being downloaded.
    """
    key, years_data = key_years_tuple
    key_display = key[:8] + '...' if key else 'default'
    logger.info(f"Starting worker process for key {key_display} with {len(years_data)} tasks")
    
    # Create two queues
    task_queue1 = queue.Queue()
    task_queue2 = queue.Queue()
    
    # Split tasks between queues (evenly distribute)
    for i, task in enumerate(years_data):
        if i % 2 == 0:
            task_queue1.put(task)
        else:
            task_queue2.put(task)
    
    # Create and start two worker threads
    threads = []
    if not task_queue1.empty():
        t1 = threading.Thread(target=worker_thread, args=(task_queue1,))
        t1.start()
        threads.append(t1)
    
    if not task_queue2.empty():
        t2 = threading.Thread(target=worker_thread, args=(task_queue2,))
        t2.start()
        threads.append(t2)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    logger.info(f"Completed all tasks for key {key_display}")

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
    var = "geopotential"
    dataset = "reanalysis-era5-pressure-levels"
    pressure_level = "500"
    
    # Load API keys from JSON file
    cdsapi_keys = load_api_keys()

    # Number of concurrent processes - one process per key
    num_processes = len(cdsapi_keys)
    
    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    logger.info(f"Years: {years.start} to {years.stop-1}")
    logger.info(f"Variable: {var}")
    logger.info(f"Dataset: {dataset}")
    if pressure_level:
        logger.info(f"Pressure Level: {pressure_level} hPa")
    logger.info(f"API Keys loaded: {len(cdsapi_keys)}")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Number of processes: {num_processes}")
    logger.info("=================================")
    
    logger.info("Starting ERA5 download process")
    
    # Distribute years across keys
    args_list = []
    for i, year in enumerate(years):
        key_index = i % len(cdsapi_keys)
        key = cdsapi_keys[key_index]
        args_list.append((year, var, key, dataset, pressure_level))
    
    # Group tasks by key
    key_grouped_tasks = {}
    for arg in args_list:
        year, var, key = arg[0], arg[1], arg[2]
        if key not in key_grouped_tasks:
            key_grouped_tasks[key] = []
        key_grouped_tasks[key].append(arg)
    
    # Create a list of (key, years_data) tuples for multiprocessing
    key_tasks = [(key, years_data) for key, years_data in key_grouped_tasks.items()]
    
    # Run downloads in parallel - one process per key
    # Each process will launch two threads to handle concurrent downloads
    logger.info("Starting download pool")
    start_time = time.time()
    with Pool(processes=num_processes) as pool:
        pool.map(process_with_concurrent_workers, key_tasks)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Download process completed in {elapsed_time:.2f} seconds")