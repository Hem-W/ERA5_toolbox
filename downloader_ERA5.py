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
import yaml
import argparse
import xarray as xr  # Added for robust NetCDF variable extraction

# Script version
__version__ = "0.4.2"

logger = logging.getLogger("ERA5_toolbox.downloader_ERA5")


class ReportCollector:
    """
    Thread-safe collector for per-task outcomes across the download pipeline.

    Each entry is a dict with keys:
        year, variable, dataset, pressure_level,
        status:  'success' | 'skipped' | 'failed'
        stage:   None                      (for success/skipped)
                 'request'                 (retrieve() raised)
                 'default_download'        (cdsapi download failed, no fallback URL)
                 'fallback_download'       (all urllib3 attempts failed)
        via:     'cdsapi' | 'urllib3'      (only set on success, else None)
        error:   str | None                (error message on failure)
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.entries = []

    def add(self, year, variable, dataset, pressure_level,
            status, stage=None, via=None, error=None):
        entry = {
            'year': year,
            'variable': variable,
            'dataset': dataset,
            'pressure_level': pressure_level,
            'status': status,
            'stage': stage,
            'via': via,
            'error': error,
        }
        with self._lock:
            self.entries.append(entry)

    def _task_label(self, e):
        pl = f" @{e['pressure_level']}hPa" if e['pressure_level'] else ""
        return f"{e['variable']}{pl} {e['year']}"

    def print_summary(self, log=None):
        """Print a human-readable summary via `log` (defaults to module logger)."""
        out = log or logger
        with self._lock:
            entries = list(self.entries)

        successes = [e for e in entries if e['status'] == 'success']
        skipped   = [e for e in entries if e['status'] == 'skipped']
        failures  = [e for e in entries if e['status'] == 'failed']

        out.info("=" * 60)
        out.info("=== ERA5 Download Summary Report ===")
        out.info(f"Total tasks processed: {len(entries)}")
        out.info(f"  Successful downloads: {len(successes)}")
        if successes:
            via_cdsapi  = sum(1 for e in successes if e['via'] == 'cdsapi')
            via_urllib3 = sum(1 for e in successes if e['via'] == 'urllib3')
            out.info(f"    via cdsapi:  {via_cdsapi}")
            out.info(f"    via urllib3 fallback: {via_urllib3}")
        out.info(f"  Skipped (already existed): {len(skipped)}")
        out.info(f"  Failed: {len(failures)}")

        if failures:
            # Group by stage for quick scanning
            by_stage = {}
            for e in failures:
                by_stage.setdefault(e['stage'] or 'unknown', []).append(e)
            out.info("--- Failures by stage ---")
            for stage, items in by_stage.items():
                out.info(f"  failed at {stage}: {len(items)}")

            out.info("--- Failed requests (detail) ---")
            for e in failures:
                msg = (e['error'] or '').strip().splitlines()
                short = msg[0] if msg else ''
                out.info(f"  [{e['stage']}] {self._task_label(e)} :: {short}")
        out.info("=" * 60)

def validate_key(key, url):
    """
    Validate a CDS API key before starting the task loop.

    cdsapi routes keys to one of two client implementations depending on format:
    - Key WITH colon (UID:APIKEY) → cdsapi.Client, uses HTTP Basic Auth,
      POST to {url}/resources/{dataset}
    - Key WITHOUT colon (UUID token) → ecmwf.datastores.LegacyClient, uses
      PRIVATE-TOKEN header, POST to {url}/retrieve/v1/processes/{dataset}/execution

    Step 1 (local): cdsapi.Client.__init__ validates key format and auth setup.
    Step 2 (network): calls the appropriate authenticated endpoint for each client type.

    Returns:
        (True, None) if the key is valid.
        (False, error_message) if the key is invalid, with the original error message.
    """
    # Step 1: local format check — no network call
    try:
        client = cdsapi.Client(url=url, key=key, quiet=True)
    except AssertionError as e:
        return False, f"Malformed key format: {e}"
    except Exception as e:
        return False, f"Client init error: {e}"

    # Step 2: network check, branching on client type.
    try:
        if hasattr(client, 'client') and hasattr(client.client, 'check_authentication'):
            # LegacyClient (key without colon): uses PRIVATE-TOKEN header internally.
            # check_authentication() calls POST {url}/profiles/v1/account/verification/pat
            # and raises requests.HTTPError for invalid tokens.
            client.client.check_authentication()
        else:
            # Old-style Client (key with colon): uses HTTP Basic Auth via session.auth.
            # POST to /resources/ with an empty body — server validates credentials before
            # the request body, so a bad key returns 401/403 and a good key returns 400/422.
            resp = client.session.post(
                f"{url}/resources/reanalysis-era5-single-levels",
                json={},
                verify=client.verify,
                timeout=client.timeout,
            )
            if resp.status_code in (401, 403):
                return False, f"HTTP {resp.status_code} {resp.reason}: {resp.text.strip()}"
        return True, None
    except Exception as e:
        return False, str(e)


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


def submit_request(client, year, variable, dataset="reanalysis-era5-single-levels", pressure_level=None, skip_existing=True, worker_id=None, short_name=None, report=None):
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
                if report is not None:
                    report.add(year, variable, dataset, pressure_level,
                               status='skipped')
                return None

    logger.info(f"{log_prefix}Requesting {target}")

    try:
        result = client.retrieve(dataset, request)
        logger.info(f"{log_prefix}Request completed, result ready for {target}")
        return (result, target, year, variable, dataset, pressure_level, short_name)
    except Exception as e:
        logger.error(f"{log_prefix}Request failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        if report is not None:
            report.add(year, variable, dataset, pressure_level,
                       status='failed', stage='request', error=str(e))
        return None


def perform_download(result, target, year, variable, dataset, pressure_level, short_name, worker_id=None, report=None):
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

    # Tracks which stage/path we ended up on, for the summary report.
    download_via = None        # 'cdsapi' | 'urllib3' on success
    failure_stage = None       # 'default_download' | 'fallback_download' on failure

    try:
        try:
            # Download method 1: cdsapi built-in
            result.download(target)
            logger.info(f"{log_prefix}Successfully downloaded {target} via cdsapi")
            download_via = 'cdsapi'
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
                        download_via = 'urllib3'
                        break

                    if attempt < 3:
                        wait_time = 60 * (2 ** (attempt - 1))
                        logger.info(f"{log_prefix}Waiting {wait_time} seconds before next attempt for {target} using urllib3")
                        time.sleep(wait_time)
                else:
                    logger.error(f"{log_prefix}All download attempts failed for {target} using urllib3")
                    failure_stage = 'fallback_download'
                    raise RuntimeError(f"All urllib3 download attempts failed for {target}")
            else:
                failure_stage = 'default_download'
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

        if report is not None:
            report.add(year, variable, dataset, pressure_level,
                       status='success', via=download_via)
        return True

    except Exception as e:
        logger.error(f"{log_prefix}Download failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        if os.path.exists(target):
            logger.info(f"{log_prefix}Removing broken file {target}")
            os.remove(target)
        if report is not None:
            report.add(year, variable, dataset, pressure_level,
                       status='failed',
                       stage=failure_stage or 'default_download',
                       error=str(e))
        return False


def key_request_thread(key, task_queue, results_queue, worker_id, report=None):
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
                                 pressure_level, skip_existing, worker_id, short_name,
                                 report=report)
        if outcome is not None:
            results_queue.put(outcome)
        task_queue.task_done()

    logger.info(f"Request thread {worker_id} finished")


def key_download_thread(results_queue, worker_id, report=None):
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
                         pressure_level, short_name, worker_id,
                         report=report)
    logger.info(f"Download thread {worker_id} finished")


def start_key_workers(key, shared_task_queue, download_workers=1, concurrent_requests=1, report=None):
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
                             args=(results_queue, f"{key[:4]}:dl{i}", report))
        t.daemon = True
        t.start()
        threads.append(t)

    # Start concurrent request threads
    req_threads = []
    for i in range(concurrent_requests):
        req = threading.Thread(target=key_request_thread,
                               args=(key, shared_task_queue, results_queue,
                                     f"{key[:4]}:req{i}", report))
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

def filter_valid_keys(keys, url="https://cds.climate.copernicus.eu/api"):
    """
    Validate each key and return only those that pass the pre-flight check.

    Logs an error for every invalid key (with the original server message) and
    raises RuntimeError if no valid keys remain.

    Args:
        keys: List of CDS API key strings.
        url: CDS API base URL.

    Returns:
        list: Keys that passed validation.

    Raises:
        RuntimeError: If all keys are invalid.
    """
    logger.info(f"Validating {len(keys)} API key(s)...")
    valid = []
    for key in keys:
        ok, err = validate_key(key, url)
        if ok:
            valid.append(key)
            logger.info(f"Key {key[:4]}...: valid")
        else:
            logger.error(f"Key {key[:4]}...: invalid — {err}")

    if not valid:
        raise RuntimeError("No valid API keys found. Please check your keys and licence agreements.")

    if len(valid) < len(keys):
        logger.warning(
            f"{len(keys) - len(valid)} key(s) removed; "
            f"proceeding with {len(valid)} valid key(s)"
        )
    return valid


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

def load_config_from_yaml(yaml_path):
    """
    Load download configuration from a YAML file.

    Args:
        yaml_path: Path to the YAML configuration file.

    Returns:
        dict: Validated configuration with keys:
            years, variables, dataset, pressure_levels, api_keys_file,
            concurrent_requests, download_workers, skip_existing, short_names.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        ValueError: If required fields are missing or have invalid types.
    """
    yaml_path = pathlib.Path(yaml_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

    with open(yaml_path, 'r') as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected a YAML mapping at the top level, got {type(raw).__name__}")

    # --- years (required) ---
    if 'years' not in raw:
        raise ValueError("'years' is required in the YAML configuration")
    years_raw = raw['years']
    if isinstance(years_raw, dict):
        if 'start' not in years_raw or 'stop' not in years_raw:
            raise ValueError("'years' mapping must contain 'start' and 'stop' keys")
        start = int(years_raw['start'])
        stop = int(years_raw['stop'])
        if start > stop:
            raise ValueError(f"'years.start' ({start}) must be <= 'years.stop' ({stop})")
        years = range(start, stop + 1)  # inclusive on both ends
    elif isinstance(years_raw, list):
        years = [int(y) for y in years_raw]
    else:
        raise ValueError(f"'years' must be a list or a mapping with start/stop, got {type(years_raw).__name__}")

    # --- variables (required) ---
    if 'variables' not in raw:
        raise ValueError("'variables' is required in the YAML configuration")
    variables = raw['variables']
    if isinstance(variables, str):
        variables = [variables]
    if not isinstance(variables, list) or not all(isinstance(v, str) for v in variables):
        raise ValueError("'variables' must be a string or list of strings")

    # --- optional fields with defaults ---
    dataset = raw.get('dataset', 'reanalysis-era5-single-levels')
    pressure_levels = raw.get('pressure_levels', None)
    api_keys_file = raw.get('api_keys_file', None)
    concurrent_requests = int(raw.get('concurrent_requests', 4))
    download_workers = int(raw.get('download_workers', 1))
    skip_existing = bool(raw.get('skip_existing', True))
    short_names = raw.get('short_names', None)

    if short_names is not None and not isinstance(short_names, dict):
        raise ValueError("'short_names' must be a mapping of variable name to short name")

    return {
        'years': years,
        'variables': variables,
        'dataset': dataset,
        'pressure_levels': pressure_levels,
        'api_keys_file': api_keys_file,
        'concurrent_requests': concurrent_requests,
        'download_workers': download_workers,
        'skip_existing': skip_existing,
        'short_names': short_names,
    }


if __name__ == '__main__':
    ####################
    # CLI Interface
    ####################
    parser = argparse.ArgumentParser(
        description="Download ERA5 data from CDS API",
        epilog="See template_request.yaml for an example YAML configuration file.",
    )
    parser.add_argument(
        '-f', '--file',
        type=str,
        default=None,
        help="Path to a YAML configuration file. If omitted, hardcoded defaults below are used.",
    )
    args = parser.parse_args()

    if args.file is not None:
        # Load configuration from YAML file
        try:
            config = load_config_from_yaml(args.file)
        except (FileNotFoundError, ValueError) as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            sys.exit(1)
        config_source = args.file
        years             = config['years']
        variables         = config['variables']
        dataset           = config['dataset']
        pressure_levels   = config['pressure_levels']
        api_keys_file     = config['api_keys_file']
        concurrent_requests = config['concurrent_requests']
        download_workers  = config['download_workers']
        skip_existing     = config['skip_existing']
        short_names       = config['short_names']
    else:
        ####################
        # Hardcoded Defaults (fallback when no --file is provided)
        ####################
        config_source = "hardcoded defaults"
        years = range(2018, 2020)
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
    # Configure logging (here so the Manager child process does not re-run this)
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"era5_download_{current_time}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger.info(f"ERA5 Downloader Version: {__version__}")
    logger.info(f"Configuration source: {config_source}")

    # Load API keys from JSON file and validate each one
    try:
        cdsapi_keys = filter_valid_keys(load_api_keys(api_keys_file))
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)
    # Validate configuration
    if skip_existing and not short_names:
        logger.warning("skip_existing is True but no short_names provided. This may cause files to be re-downloaded.")

    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    if isinstance(years, range):
        logger.info(f"Years: {years.start} to {years.stop - 1} ({len(years)} years)")
    else:
        logger.info(f"Years: {years} ({len(years)} years)")
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

    # Collect per-task outcomes from every worker thread for the final summary
    report = ReportCollector()

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
            key_threads = start_key_workers(key, shared_task_queue, download_workers, concurrent_requests, report=report)
            all_threads.extend(key_threads)

        # Wait for all threads to complete
        for t in all_threads:
            t.join()

    # Calculate elapsed time
    _td = datetime.timedelta(seconds=int(time.time() - start_time))
    logger.info(f"All dataset requests and downloads completed in {_td.days:02d}d{_td.seconds//3600:02d}h{_td.seconds%3600//60:02d}m")

    # Final summary report
    report.print_summary()
