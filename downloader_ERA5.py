"""
Download ERA5 data from CDS API

Author: Hui-Min Wang

This script downloads ERA5 data from CDS API for specified years.
"""


import traceback
import pathlib
from itertools import product
import cdsapi
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
import xarray as xr
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

# Script version
__version__ = "0.4.5.dev"

logger = logging.getLogger("ERA5_toolbox.downloader_ERA5")

# Full month/day/hour grids used by every CDS retrieve() — year-granular
# downloads always request all months, days, and hours.
_ALL_MONTHS = [f'{m:02d}' for m in range(1, 13)]
_ALL_DAYS = [f'{d:02d}' for d in range(1, 32)]
_ALL_HOURS = [f'{h:02d}:00' for h in range(24)]


@dataclass(frozen=True)
class RequestTask:
    """One CDS retrieve() to issue: payload fields plus path-pattern context."""
    year: int
    variable: str
    dataset: str
    pressure_level: Optional[object]   # int | str | None at runtime
    short_name: Optional[str]
    skip_existing: bool
    path_pattern: str


@dataclass
class DownloadTask:
    """Server-prepared CDS result paired with its originating ``RequestTask``."""
    result: object   # cdsapi Result (no public type to import)
    target: str
    request: RequestTask


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
            by_stage = defaultdict(list)
            for e in failures:
                by_stage[e['stage'] or 'unknown'].append(e)
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

        # Initialize byte counter for progress tracking
        downloaded_bytes = file_size
        with tqdm(total=total_size, initial=file_size, unit='B', unit_scale=True,
                  desc=f"Downloading {os.path.basename(target_path)}") as pbar:

            with open(target_path, mode) as f:
                for chunk in response.stream(chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        pbar.update(len(chunk))

        response.release_conn()

        # Verify if download is complete
        if total_size > 0 and downloaded_bytes < total_size:
            logger.warning(f"urllib3: Download incomplete for {target_path}: got {downloaded_bytes} bytes out of {total_size}")
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
        with xr.open_dataset(nc_file) as ds:
            candidates = [v for v in ds.data_vars if v not in excluded]

        # Return the appropriate variable
        if api_variable in candidates:
            return api_variable
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            logger.info(f"Multiple variables found in NetCDF file: {candidates}")
            logger.info(f"Using first variable: {candidates[0]}")
            return candidates[0]
        logger.warning("No clear variable found, falling back to original name")
        return api_variable

    except Exception as e:
        logger.error(f"Error reading NetCDF file: {e}")
        return api_variable


def default_folder_pattern():
    """Default folder pattern: one ``hour/{short_name}`` subdir per variable."""
    return "hour/{short_name}"


def default_name_pattern(dataset):
    """Default file-name pattern; pressure-level files include ``{pressure_level}hpa`` to avoid collisions."""
    if dataset == "reanalysis-era5-pressure-levels":
        return "era5.reanalysis.{short_name}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc"
    return "era5.reanalysis.{short_name}.1hr.0p25deg.global.{year}.nc"


def build_target_path(path_pattern, *, short_name, variable, year, dataset,
                      pressure_level=None):
    """Compose the full output path by substituting placeholders.

    Supported placeholders (all optional in the pattern):
        {short_name}     — variable short name; falls back to ``variable``
                           when ``short_name`` is None.
        {variable}       — API variable name (e.g. ``surface_pressure``).
        {year}           — year being downloaded.
        {pressure_level} — pressure level in hPa; empty string when absent.
        {dataset}        — CDS dataset name.

    Raises:
        ValueError: if ``path_pattern`` is empty or references an unknown placeholder.
    """
    if not path_pattern:
        raise ValueError("path_pattern must be a non-empty string")

    ctx = {
        'short_name': short_name if short_name is not None else variable,
        'variable': variable,
        'year': year,
        'dataset': dataset,
        'pressure_level': '' if pressure_level is None else pressure_level,
    }
    try:
        return path_pattern.format(**ctx)
    except KeyError as e:
        raise ValueError(
            f"Unknown placeholder {e} in path_pattern; "
            f"supported placeholders: {sorted(ctx.keys())}"
        ) from e


def submit_request(client, request_task, worker_id=None, report=None):
    """
    Submit a request to the CDS API and wait for the server to process it.

    Args:
        client: A *freshly created* CDS client dedicated to this request
                (see ``key_request_thread`` for why clients are not reused).
        request_task: ``RequestTask`` describing what to download.
        worker_id: Optional identifier for log prefixes.
        report: Optional ``ReportCollector`` to record outcome.

    Returns:
        DownloadTask ready for download, or None if the task was skipped
        or the request failed.
    """
    log_prefix = f"Worker {worker_id}: " if worker_id else ""
    rt = request_task

    request = {
        'product_type': ['reanalysis'],
        'variable': [rt.variable],
        'year': [str(rt.year)],
        'month': _ALL_MONTHS,
        'day': _ALL_DAYS,
        'time': _ALL_HOURS,
        "data_format": "netcdf",
        "download_format": "unarchived"
    }

    if rt.dataset == "reanalysis-era5-pressure-levels":
        if rt.pressure_level is None:
            raise ValueError("pressure_level is required for reanalysis-era5-pressure-levels dataset")
        request["pressure_level"] = [rt.pressure_level]

    target = build_target_path(
        rt.path_pattern,
        short_name=rt.short_name, variable=rt.variable,
        year=rt.year, dataset=rt.dataset, pressure_level=rt.pressure_level,
    )

    # Check for existing files before submitting to avoid wasting a server slot
    if rt.skip_existing:
        if rt.short_name is None:
            logger.warning(f"{log_prefix}skip_existing is True but no short_name provided for {rt.variable}. Cannot reliably check for existing files.")
        else:
            if os.path.exists(target):
                logger.info(f"{log_prefix}Skip existing file {target} for variable {rt.variable}")
                if report is not None:
                    report.add(rt.year, rt.variable, rt.dataset, rt.pressure_level,
                               status='skipped')
                return None

    logger.info(f"{log_prefix}Requesting {target}")

    try:
        result = client.retrieve(rt.dataset, request)
        logger.info(f"{log_prefix}Request completed, result ready for {target}")
        return DownloadTask(result=result, target=target, request=rt)
    except Exception as e:
        logger.error(f"{log_prefix}Request failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        if report is not None:
            report.add(rt.year, rt.variable, rt.dataset, rt.pressure_level,
                       status='failed', stage='request', error=str(e))
        return None


def perform_download(download_task, worker_id=None, report=None):
    """
    Download a prepared CDS result to disk.

    The result object inside ``download_task`` is self-contained — no client is
    needed. Falls back to urllib3 via ``result.location`` if cdsapi's own
    download fails.

    Args:
        download_task: ``DownloadTask`` returned by ``submit_request``.
        worker_id: Optional identifier for log prefixes.
        report: Optional ``ReportCollector`` to record outcome.

    Returns:
        bool: True if download succeeded, False otherwise.
    """
    log_prefix = f"Worker {worker_id}: " if worker_id else ""
    rt = download_task.request
    result = download_task.result
    target = download_task.target

    # Ensure the output directory exists before the downloader writes into it.
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

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
            except AttributeError:
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
        if rt.short_name is None and os.path.exists(target):
            actual_var_code = get_variable_code_from_netcdf(target, rt.variable)

            final_target = build_target_path(
                rt.path_pattern,
                short_name=actual_var_code, variable=rt.variable,
                year=rt.year, dataset=rt.dataset, pressure_level=rt.pressure_level,
            )

            if target != final_target:
                if os.path.exists(final_target):
                    logger.warning(f"{log_prefix}Final target {final_target} already exists, removing redundant file {target}")
                    os.remove(target)
                else:
                    final_dir = os.path.dirname(final_target)
                    if final_dir:
                        os.makedirs(final_dir, exist_ok=True)
                    os.rename(target, final_target)
                    logger.info(f"{log_prefix}Renamed {target} to {final_target}")

        if report is not None:
            report.add(rt.year, rt.variable, rt.dataset, rt.pressure_level,
                       status='success', via=download_via)
        return True

    except Exception as e:
        logger.error(f"{log_prefix}Download failed for {target}: {str(e)}")
        logger.error(traceback.format_exc())
        if os.path.exists(target):
            logger.info(f"{log_prefix}Removing broken file {target}")
            os.remove(target)
        if report is not None:
            report.add(rt.year, rt.variable, rt.dataset, rt.pressure_level,
                       status='failed',
                       stage=failure_stage or 'default_download',
                       error=str(e))
        return False


def key_request_thread(key, task_queue, results_queue, worker_id, report=None):
    """
    Request thread for one API key. Submits one retrieve() at a time.

    Multiple instances run concurrently for the same key to keep several
    requests queued on the CDS server. A fresh ``cdsapi.Client`` is created
    per task because ``Client.last_state`` is an instance attribute — reusing
    a client across concurrent retrieve() calls corrupts its state.

    Shutdown: the thread exits on a ``None`` sentinel from ``task_queue``.
    The caller must enqueue one sentinel per request thread after all real
    tasks. Sentinels for the download pool are handled separately by
    ``start_key_workers``' supervisor.
    """
    logger.info(f"Request thread {worker_id} started")
    while True:
        task = task_queue.get()
        if task is None:  # stop signal
            break

        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=key)
        outcome = submit_request(client, task, worker_id=worker_id, report=report)
        if outcome is not None:
            results_queue.put(outcome)

    logger.info(f"Request thread {worker_id} finished")


def key_download_thread(results_queue, worker_id, report=None):
    """
    Consume ready ``DownloadTask`` items from ``results_queue`` and download them.

    Blocks on ``results_queue.get()`` so the thread stays alive through long
    server waits, and exits on a ``None`` sentinel sent by
    ``start_key_workers``' supervisor after all request threads finish.
    """
    logger.info(f"Download thread {worker_id} started")
    while True:
        item = results_queue.get()
        if item is None:
            break
        perform_download(item, worker_id=worker_id, report=report)
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
    keys_file = keys_file or "cdsapi_keys.json"

    if not os.path.exists(keys_file):
        logger.error(f"API keys file {keys_file} not found.")
        raise RuntimeError(f"API keys file {keys_file} not found. Please create this file with your API keys.")

    try:
        with open(keys_file, 'r') as f:
            data = json5.load(f)
    except Exception as e:
        logger.error(f"Error reading {keys_file}: {str(e)}")
        raise RuntimeError(f"Error reading {keys_file}. Please ensure it is readable and contains valid JSON. Underlying error: {str(e)}") from e

    if not isinstance(data, dict) or 'keys' not in data or not isinstance(data['keys'], list):
        logger.error(f"Invalid format in {keys_file}. Expected 'keys' list.")
        raise RuntimeError(f"Invalid format in {keys_file}. Expected a JSON object with a 'keys' list.")

    keys = data['keys']
    if not keys:
        logger.error(f"No keys found in {keys_file}")
        raise RuntimeError(f"No keys found in {keys_file}. Please add at least one API key.")

    logger.info(f"Successfully loaded {len(keys)} API keys from {keys_file}")
    return keys

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
    if isinstance(pressure_levels, (int, str)):
        pressure_levels = [pressure_levels]
    elif pressure_levels is not None and not isinstance(pressure_levels, list):
        raise ValueError("'pressure_levels' must be an int, str, or list of int/str")
    api_keys_file = raw.get('api_keys_file', None)
    concurrent_requests = int(raw.get('concurrent_requests', 4))
    download_workers = int(raw.get('download_workers', 1))
    skip_existing = bool(raw.get('skip_existing', True))
    short_names = raw.get('short_names', None)

    if short_names is not None and not isinstance(short_names, dict):
        raise ValueError("'short_names' must be a mapping of variable name to short name")

    folder_pattern = raw.get('folder_pattern', None)
    name_pattern = raw.get('name_pattern', None)
    if folder_pattern is not None and not isinstance(folder_pattern, str):
        raise ValueError("'folder_pattern' must be a string")
    if name_pattern is not None and not isinstance(name_pattern, str):
        raise ValueError("'name_pattern' must be a string")

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
        'folder_pattern': folder_pattern,
        'name_pattern': name_pattern,
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
        folder_pattern    = config['folder_pattern']
        name_pattern      = config['name_pattern']
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
        # Output path patterns (None -> use dataset-aware defaults).
        # Supported placeholders: {short_name}, {variable}, {year},
        # {pressure_level}, {dataset}.
        folder_pattern = None
        name_pattern = None

    ####################
    # Program
    ####################
    # Configure logging
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

    # Resolve dataset-aware defaults and combine folder_pattern + name_pattern
    # into a single path_pattern here at the top level, so the request and
    # download helpers only ever deal with one fully-formed template.
    if folder_pattern is None:
        folder_pattern = default_folder_pattern()
    if name_pattern is None:
        name_pattern = default_name_pattern(dataset)
    path_pattern = os.path.join(folder_pattern, name_pattern) if folder_pattern else name_pattern

    # Log initial configuration
    key_prefixes = [key[:4] for key in cdsapi_keys]
    logger.info("=== ERA5 Download Configuration ===")
    if isinstance(years, range):
        logger.info(f"Years: {years.start} to {years.stop - 1} ({len(years)} years)")
    else:
        logger.info(f"Years: {years} ({len(years)} years)")
    logger.info(f"Variables: {', '.join(variables)}")
    logger.info(f"Dataset: {dataset}")
    if pressure_levels:
        logger.info(f"Pressure Levels: {', '.join(str(p) for p in pressure_levels)} hPa")
    logger.info(f"API Keys (first 4 digits): {', '.join(key_prefixes)}")
    logger.info(f"Concurrent requests per key: {concurrent_requests}")
    logger.info(f"Download workers per key: {download_workers}")
    logger.info(f"Skip existing files: {skip_existing}")
    if short_names:
        logger.info(f"Using short names: {short_names}")
    logger.info(f"Path pattern: {path_pattern}")
    logger.info("=================================")

    logger.info("Starting ERA5 download process with dynamic task assignment")

    # Track start time
    start_time = time.time()

    # Collect per-task outcomes from every worker thread for the final summary
    report = ReportCollector()

    shared_task_queue = queue.Queue()

    # Fill the queue with all year-variable-pressure_level combinations
    # (levels collapses to [None] for datasets without pressure levels).
    levels = pressure_levels or [None]
    for year, var, level in product(years, variables, levels):
        var_short_name = short_names.get(var) if short_names else None
        shared_task_queue.put(RequestTask(
            year=year, variable=var, dataset=dataset,
            pressure_level=level, short_name=var_short_name,
            skip_existing=skip_existing, path_pattern=path_pattern,
        ))

    logger.info(f"Initialized shared task queue with {shared_task_queue.qsize()} tasks")

    # One stop sentinel per request thread so each one exits cleanly.
    total_request_threads = len(cdsapi_keys) * concurrent_requests
    for _ in range(total_request_threads):
        shared_task_queue.put(None)

    # Start pipeline (request thread + download pool) for each key
    all_threads = []
    for key in cdsapi_keys:
        key_threads = start_key_workers(key, shared_task_queue, download_workers, concurrent_requests, report=report)
        all_threads.extend(key_threads)

    # Wait for all threads to complete
    for t in all_threads:
        t.join()

    # Calculate elapsed time
    elapsed = int(time.time() - start_time)
    days, rem = divmod(elapsed, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    logger.info(f"All dataset requests and downloads completed in {days:02d}d{hours:02d}h{minutes:02d}m{seconds:02d}s")

    # Final summary report
    report.print_summary()
