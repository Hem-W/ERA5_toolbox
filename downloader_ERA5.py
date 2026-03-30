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
import xarray as xr  # Added for robust NetCDF variable extraction

# Script version
__version__ = "0.3.1.dev"

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
logger = logging.getLogger("ERA5_toolbox.downloader_ERA5")
logger.info(f"ERA5 Downloader Version: {__version__}")

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
    Extract the actual variable code from a NetCDF file using xarray.
    Args:
        nc_file: Path to the NetCDF file
        api_variable: The variable name used in the API request
    Returns:
        str: The variable code as stored in the NetCDF file
    """
    # Common auxiliary variables to exclude
    excluded = {'number', 'expver'}
    try:
        # Open the NetCDF file with xarray (xarray automatically filters coordinates)
        ds = xr.open_dataset(nc_file)

        # Get data variables (non-coordinate variables)
        data_vars = list(ds.data_vars)

        # Filter out auxiliary variables
        candidates = [v for v in data_vars if v not in excluded]

        # Return the appropriate variable
        if api_variable in candidates:
            var_code = api_variable
        elif len(candidates) == 1:
            var_code = candidates[0]
        elif len(candidates) > 1:
            logger.info(f"Multiple variables found in NetCDF file: {candidates}")
            logger.info(f"Using first variable: {candidates[0]}")
            var_code = candidates[0]
        else:
            logger.warning("No clear variable found, falling back to original name")
            var_code = api_variable

        # Always close the dataset before returning
        ds.close()
        return var_code

    except Exception as e:
        logger.error(f"Error reading NetCDF file: {e}")
        return api_variable


def submit_request(client, year, variable, dataset="reanalysis-era5-single-levels", pressure_level=None, skip_existing=True, worker_id=None, short_name=None):
    """
    Submit a request to the CDS API and wait for the server to process it.

    Each call must use a freshly created client: cdsapi.Client stores last_state as
    an instance variable, so concurrent retrieve() calls on the same client corrupt
    each other's state. The returned result object is fully self-contained; the client
    is discarded after this function returns.

    Args:
        client: A freshly created CDS client dedicated to this request
        year: Year to download
        variable: Variable to download
        dataset: Dataset name
        pressure_level: Pressure level in hPa (required for pressure-level dataset)
        skip_existing: Whether to skip if the target file already exists
        worker_id: Optional identifier for log prefixes
        short_name: Optional short name for the variable used in the filename.
                    If None, the variable name is used initially and the file is
                    renamed after download based on the actual variable code.

    Returns:
        tuple: (result, target_path, year, variable, dataset, pressure_level, short_name)
               or None if the task was skipped or the request failed
    """
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

    var_name_for_file = short_name if short_name is not None else variable

    if dataset == "reanalysis-era5-pressure-levels":
        if pressure_level is None:
            raise ValueError("pressure_level is required for reanalysis-era5-pressure-levels dataset")
        request["pressure_level"] = [pressure_level]
        target = f"era5.reanalysis.{var_name_for_file}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
    else:
        target = f"era5.reanalysis.{var_name_for_file}.1hr.0p25deg.global.{year}.nc"

    # Check for existing files before submitting to avoid wasting a server slot
    if skip_existing:
        if short_name is None:
            logger.warning(f"{log_prefix}skip_existing is True but no short_name provided for {variable}. Cannot reliably check for existing files.")
        else:
            if dataset == "reanalysis-era5-pressure-levels":
                file_pattern = f"era5.reanalysis.{short_name}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
            else:
                file_pattern = f"era5.reanalysis.{short_name}.1hr.0p25deg.global.{year}.nc"

            if os.path.exists(file_pattern):
                logger.info(f"{log_prefix}Skip existing file {file_pattern} for variable {variable}")
                return None

    logger.info(f"{log_prefix}Requesting {target}")

    try:
        result = client.retrieve(dataset, request)
        logger.info(f"{log_prefix}Request completed, result ready for {target}")
        return (result, target, year, variable, dataset, pressure_level, short_name)
    except Exception as e:
        logger.error(f"{log_prefix}Request failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def perform_download(result, target, year, variable, dataset, pressure_level, short_name, worker_id=None):
    """
    Download a prepared CDS result to disk.

    The result object returned by retrieve() is self-contained — no client is needed.
    Falls back to urllib3 via result.location if cdsapi's own download fails.

    Args:
        result: The result object returned by client.retrieve()
        target: Target file path
        year: Year (used for rename logic when short_name is None)
        variable: Original API variable name (used for rename logic)
        dataset: Dataset name (used for rename logic)
        pressure_level: Pressure level (used for rename logic)
        short_name: Short name used in the filename (None triggers auto-rename)
        worker_id: Optional identifier for log prefixes

    Returns:
        bool: True if download succeeded, False otherwise
    """
    log_prefix = f"Worker {worker_id}: " if worker_id else ""

    try:
        try:
            # Download method 1: cdsapi built-in
            result.download(target)
            logger.info(f"{log_prefix}Successfully downloaded {target} via cdsapi")
        except Exception as e:
            logger.error(f"{log_prefix}Standard download failed for {target}: {str(e)}")
            logger.error(traceback.format_exc())

            # Download method 2: urllib3 via result.location
            download_url = None
            try:
                download_url = result.location
            except (AttributeError, Exception):
                logger.warning(f"{log_prefix}Could not get direct download URL, cannot use urllib3 fallback")

            if download_url:
                for attempt in range(1, 4):
                    if attempt > 1:
                        logger.info(f"{log_prefix}Retry attempt {attempt}/3 for {target} using urllib3")

                    if download_file_with_urllib3(download_url, target):
                        logger.info(f"{log_prefix}Successfully downloaded {target} using urllib3")
                        break

                    if attempt < 3:
                        wait_time = 60 * (2 ** (attempt - 1))
                        logger.info(f"{log_prefix}Waiting {wait_time} seconds before next attempt for {target} using urllib3")
                        time.sleep(wait_time)
                else:
                    logger.error(f"{log_prefix}All download attempts failed for {target} using urllib3")
                    raise RuntimeError(f"All urllib3 download attempts failed for {target}")
            else:
                raise

        # Rename the file when short_name was not provided (auto-detect from NetCDF)
        if short_name is None and os.path.exists(target):
            actual_var_code = get_variable_code_from_netcdf(target, variable)

            if dataset == "reanalysis-era5-pressure-levels":
                final_target = f"era5.reanalysis.{actual_var_code}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
            else:
                final_target = f"era5.reanalysis.{actual_var_code}.1hr.0p25deg.global.{year}.nc"

            if target != final_target:
                if os.path.exists(final_target):
                    logger.warning(f"{log_prefix}Final target {final_target} already exists, removing redundant file {target}")
                    os.remove(target)
                else:
                    os.rename(target, final_target)
                    logger.info(f"{log_prefix}Renamed {target} to {final_target}")

        return True

    except Exception as e:
        logger.error(f"{log_prefix}Download failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        if os.path.exists(target):
            logger.info(f"{log_prefix}Removing broken file {target}")
            os.remove(target)
        return False


def key_request_thread(key, task_queue, results_queue, worker_id):
    """
    Request thread for one API key.

    Submits one retrieve() at a time per thread. Multiple instances of this
    function can run concurrently for the same key (each with its own fresh
    cdsapi.Client) to keep several requests queued on the server side.

    Does NOT send sentinels — start_key_workers() manages that via a supervisor
    thread that waits for all request threads to finish before signalling the
    download pool.

    Each task gets a fresh cdsapi.Client because Client.last_state is an instance
    variable — reusing a client across tasks would cause state corruption.
    """
    logger.info(f"Request thread {worker_id} started")
    while True:
        try:
            task = task_queue.get(timeout=10)
        except queue.Empty:
            if task_queue.empty():
                break
            continue

        if task is None:  # explicit stop signal
            task_queue.task_done()
            break

        year, variable, _, dataset, pressure_level, short_name, skip_existing = task
        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=key)
        outcome = submit_request(client, year, variable, dataset,
                                 pressure_level, skip_existing, worker_id, short_name)
        if outcome is not None:
            results_queue.put(outcome)
        task_queue.task_done()

    logger.info(f"Request thread {worker_id} finished")


def key_download_thread(results_queue, worker_id):
    """
    Download thread that consumes ready results from the request thread.

    Uses a blocking get() with no timeout so the thread stays alive for the full
    run duration, even when the CDS server takes hours to process a request.
    Exits only when it receives the None sentinel sent by the request thread after
    all tasks are exhausted.
    """
    logger.info(f"Download thread {worker_id} started")
    while True:
        item = results_queue.get()  # blocks indefinitely — no timeout
        if item is None:
            break
        result, target, year, variable, dataset, pressure_level, short_name = item
        perform_download(result, target, year, variable, dataset,
                         pressure_level, short_name, worker_id)
    logger.info(f"Download thread {worker_id} finished")


def start_key_workers(key, shared_task_queue, download_workers=1, concurrent_requests=1):
    """
    Start the request pipeline and download pool for a single API key.

    Architecture per key:
    - concurrent_requests request threads: each submits one retrieve() at a time
      (each with its own fresh cdsapi.Client), pushing results to a per-key queue
      as soon as the server is done. Multiple threads keep several requests queued
      on the CDS server simultaneously.
    - download_workers download threads: each blocks on the results queue and
      downloads concurrently with ongoing request submissions.
    - 1 supervisor thread: waits for all request threads to finish, then sends
      the None sentinels that shut down the download pool.

    Args:
        key: CDS API key
        shared_task_queue: Shared task queue across all keys
        download_workers: Number of parallel download threads per key (default 1)
        concurrent_requests: Number of concurrent request threads per key (default 1)

    Returns:
        list: All spawned threads (request threads + download threads + supervisor)
    """
    results_queue = queue.Queue()
    threads = []

    # Start download pool first so it is ready before the first result arrives
    for i in range(download_workers):
        t = threading.Thread(target=key_download_thread,
                             args=(results_queue, f"{key[:4]}:dl{i}"))
        t.daemon = True
        t.start()
        threads.append(t)

    # Start concurrent request threads
    req_threads = []
    for i in range(concurrent_requests):
        req = threading.Thread(target=key_request_thread,
                               args=(key, shared_task_queue, results_queue,
                                     f"{key[:4]}:req{i}"))
        req.daemon = True
        req.start()
        req_threads.append(req)
        threads.append(req)

    # Supervisor: sends sentinels to the download pool once all request threads finish
    def _sentinel_supervisor():
        for t in req_threads:
            t.join()
        for _ in range(download_workers):
            results_queue.put(None)
        logger.info(f"Key {key[:4]}: all request threads done, download pool signalled")

    sup = threading.Thread(target=_sentinel_supervisor, daemon=True)
    sup.start()
    threads.append(sup)

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
    years = range(1980, 1992)
    variables = ['surface_pressure']
    dataset = "reanalysis-era5-single-levels"
    pressure_levels = None  # List of pressure levels (hPa)
    api_keys_file = None  # Use default 'cdsapi_keys.json'
    # Number of concurrent request threads per key.
    # Each thread submits one retrieve() at a time with its own fresh cdsapi.Client,
    # so N threads keep N requests queued on the CDS server simultaneously.
    # Note: CDS typically runs 1 request at a time per key but allows several queued.
    concurrent_requests = 4
    # Number of parallel download threads per key.
    download_workers = 1
    skip_existing = True  # Whether to skip downloading existing files (requires short_names if True)
    # Optional: Provide short names for variables.
    # If skip_existing=True, short_names MUST be provided for reliable operation.
    # Example: short_names = {'convective_available_potential_energy': 'cape'}
    short_names = {'surface_pressure': 'sp'}

    ####################
    # Program
    ####################
    # Load API keys from JSON file
    cdsapi_keys = load_api_keys(api_keys_file)
    # Validate configuration
    if skip_existing and not short_names:
        logger.warning("skip_existing is True but no short_names provided. This may cause files to be re-downloaded.")

    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    logger.info(f"Years: {years.start} to {years.stop-1}")
    # Convert single variable string to list, or keep as list if already a list
    variables = [variables] if isinstance(variables, str) else variables
    logger.info(f"Variables: {', '.join(variables)}")
    logger.info(f"Dataset: {dataset}")
    # Convert single pressure level to list, or keep as list if already a list
    pressure_levels = [pressure_levels] if isinstance(pressure_levels, (int, str)) else pressure_levels
    if pressure_levels:
        logger.info(f"Pressure Levels: {', '.join(str(p) for p in pressure_levels)} hPa")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Concurrent requests per key: {concurrent_requests}")
    logger.info(f"Download workers per key: {download_workers}")
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
                var_short_name = short_names.get(var) if short_names else None
                shared_task_queue.put((year, var, None, dataset, level, var_short_name, skip_existing))
        else:
            for year, var in product(years, variables):
                var_short_name = short_names.get(var) if short_names else None
                shared_task_queue.put((year, var, None, dataset, None, var_short_name, skip_existing))

        logger.info(f"Initialized shared task queue with {shared_task_queue.qsize()} tasks")

        # Start pipeline (request thread + download pool) for each key
        all_threads = []
        for key in cdsapi_keys:
            key_threads = start_key_workers(key, shared_task_queue, download_workers, concurrent_requests)
            all_threads.extend(key_threads)

        # Wait for all threads to complete
        for t in all_threads:
            t.join()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"All download processes completed in {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}")
