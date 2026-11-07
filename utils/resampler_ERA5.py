"""
Process ERA5 hourly data to daily statistics

Author: Hui-Min Wang
TODO: Experimental! Not included in the package.

This script converts ERA5 hourly data to daily statistics (sum, mean, max, or min).
For accumulated variables like precipitation (tp), a time shift can be applied
to correctly align the data with the daily periods.

Features:
    + dask distributed: per-worker memory limit = --memory-limit if given, else the larger of
      (a) what the final whole-year gather needs (planned from one year of daily output) and
      (b) 90% of available RAM / --workers. A limit is a cap, not a reservation, so limits may add up
      to more than RAM (e.g. --workers 32 --threads 1 --memory-limit 100GB): only one worker holds the
      whole year at a time. 

Usage:
    python resampler_ERA5.py 2020 --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1
    python resampler_ERA5.py $(seq 2020 2025) --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1
    python resampler_ERA5.py 2020 --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1 --log-level DEBUG --log-dir /path/to/log
    python resampler_ERA5.py 2020 --dataset era5-land --variable swvl1 --input-dir ERA5-land/hour/swvl1 --output-dir ERA5-land/day/swvl1 --method mean
    python resampler_ERA5.py 2020 --dataset era5-land --variable tp --input-dir ERA5-land/hour/tp --output-dir ERA5-land/day/tp --method sum
    python resampler_ERA5.py 2020 --dataset era5-land-00utc --variable tp --input-dir ERA5-land/hour/tp --output-dir ERA5-land/day/tp --method sum

Input-Output file layout (--dataset):
    + era5      : one file per year,  era5.reanalysis.{var}.1hr.0p25deg.global.{YYYY}.nc
    + era5-land : one file per month, era5-land.reanalysis.{var}.1hr.0p1deg.global.{YYYYMM}.nc
    + era5-land-00utc : one file per year of 00 UTC steps only (accumulated variables, --method sum),
                  era5-land.reanalysis.{var}.00utc.0p1deg.global.{YYYY}.nc; year Y also reads Y+1 for Dec 31
    Output is always one file per year, e.g. era5-land.reanalysis.{var}.day{method}.0p1deg.global.{YYYY}.nc
    
Advanced Usage:
    + multiple variables
        for var in swvl1 swvl2; do python resampler_ERA5.py $(seq 2003 2021) --variable $var --input-dir hour/${var} --output-dir day/${var} --workers 10 --method mean; done
    + include pressure levels
        for var in d q t; do for pres in 500hpa 1000hpa; do combined="${var}.${pres}"; python resampler_ERA5.py $(seq 2003 2021) --variable $combined --input-dir hour/${var} --output-dir day/${var} --workers 20 --method mean --chunk-size 150; done; done
"""

import xarray as xr
import pandas as pd
import earthkit.transforms.aggregate
import dask.distributed
import dask.utils
import argparse
import os
import logging
import gc
import calendar
import math
import psutil
from datetime import datetime, timedelta


# File naming per dataset; "monthly" datasets store one input file per month, "freq" is the
# time-step tag of the input file name. era5-land-00utc holds only the 00 UTC steps (downloader
# `times: ['00:00']`), one file per year, for ERA5-Land accumulated variables
DATASET_SPECS = {
    "era5": {"prefix": "era5", "resolution": "0p25deg", "monthly": False, "freq": "1hr"},
    "era5-land": {"prefix": "era5-land", "resolution": "0p1deg", "monthly": True, "freq": "1hr"},
    "era5-land-00utc": {"prefix": "era5-land", "resolution": "0p1deg", "monthly": False, "freq": "00utc"},
}


def is_land(dataset):
    """True for any ERA5-Land input layout (hourly monthly files or 00 UTC yearly files)"""
    return DATASET_SPECS[dataset]["prefix"] == "era5-land"


def input_file_path(input_dir, variable, year, month=None, dataset="era5"):
    """Path of an input file (month is required for monthly datasets)"""
    spec = DATASET_SPECS[dataset]
    stamp = f"{year}{month:02d}" if spec["monthly"] else f"{year}"
    return os.path.join(input_dir, f"{spec['prefix']}.reanalysis.{variable}.{spec['freq']}.{spec['resolution']}.global.{stamp}.nc")


def output_file_path(output_dir, variable, year, method, dataset="era5"):
    """Path of the yearly daily-statistics output file"""
    spec = DATASET_SPECS[dataset]
    return os.path.join(output_dir, f"{spec['prefix']}.reanalysis.{variable}.day{method}.{spec['resolution']}.global.{year}.nc")


# Worker memory planning: the final gather/concat holds the whole year's daily output in one task,
# about twice its size at peak (measured: 17.8 GiB for a 9.5 GB year); dask pauses a worker at 80%
# and kills it at 95% of its limit, so the peak is kept at ~70% of the limit
TASK_PEAK_FACTOR = 2.0
WORKER_PEAK_FRACTION = 0.7
RAM_USABLE_FRACTION = 0.9


def plan_memory(year, variable, input_dir, dataset="era5", n_workers=1, memory_limit=None):
    """
    Per-worker dask memory limit for one year.
    
    The final gather holds one year of daily output (days x grid x itemsize) in a single worker at
    about TASK_PEAK_FACTOR x its size, so the minimum limit is that peak / WORKER_PEAK_FRACTION.
    A limit is a cap, not a reservation: only one worker holds the whole year at a time, so the
    job's actual peak is about two such peaks (that worker + the main process holding the result).
    
    memory_limit (bytes or e.g. "100GB") is used as given for every worker, regardless of n_workers;
    a warning is logged if it is below the minimum. Otherwise the limit is
    max(minimum, RAM_USABLE_FRACTION x available RAM / n_workers), so other tasks (e.g. large input
    chunks of yearly ERA5 files) keep the headroom of dask's default.
    
    Raises MemoryError if the job's expected peak cannot fit in available RAM. Returns the limit in
    bytes, or memory_limit/"auto" if the input file is missing (process_year then reports it).
    """
    logger = logging.getLogger("ERA5_resampler")
    user_limit = dask.utils.parse_bytes(memory_limit) if isinstance(memory_limit, str) else memory_limit
    month = 1 if DATASET_SPECS[dataset]["monthly"] else None
    sample_file = input_file_path(input_dir, variable, year, month, dataset)
    if not os.path.exists(sample_file):
        logger.warning(f"Cannot plan worker memory, input file not found: {sample_file}")
        return user_limit or "auto"
    
    with xr.open_dataset(sample_file) as ds:
        da = ds[list(ds.data_vars)[0]]
        cells = math.prod(n for dim, n in da.sizes.items() if dim != "valid_time")
        itemsize = da.dtype.itemsize
    n_days = 366 if calendar.isleap(year) else 365
    year_bytes = n_days * cells * itemsize
    task_peak = TASK_PEAK_FACTOR * year_bytes
    min_limit = int(task_peak / WORKER_PEAK_FRACTION)
    job_peak = 2 * task_peak
    
    available = psutil.virtual_memory().available
    if job_peak > available * RAM_USABLE_FRACTION:
        raise MemoryError(f"Not enough RAM for year {year}: the job peaks at about {job_peak / 1e9:.1f} GB "
                          f"(one worker + main process holding {task_peak / 1e9:.1f} GB each), "
                          f"but only {available / 1e9:.1f} GB is available")
    
    if user_limit is not None:
        limit = int(user_limit)
        source = "--memory-limit"
        if limit < min_limit:
            logger.warning(f"--memory-limit {limit / 1e9:.1f} GB is below the {min_limit / 1e9:.1f} GB the "
                           f"final whole-year gather needs; the gathering worker will likely be killed")
    else:
        share = int(available * RAM_USABLE_FRACTION / n_workers)
        limit = max(min_limit, share)
        source = "gather minimum" if limit == min_limit else "RAM share"
    logger.info(f"Memory plan for {year}: one year of daily output = {year_bytes / 1e9:.1f} GB, "
                f"gather needs >= {min_limit / 1e9:.1f} GB per worker; worker limit {limit / 1e9:.1f} GB "
                f"x {n_workers} workers ({source}); expected job peak {job_peak / 1e9:.1f} GB "
                f"of {available / 1e9:.0f} GB available")
    return limit


def setup_logging(log_level=logging.INFO, log_dir=None):
    """
    Set up logging with both file and console output
    
    Parameters:
    -----------
    log_level : int
        Logging level (default: INFO)
    log_dir : str, optional
        Directory to store log files. If None, logs to current directory.
    
    Returns:
    --------
    logger : logging.Logger
        Configured logger
    """
    # Create log directory if specified and doesn't exist
    if (log_dir):
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"era5_resampler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    else:
        log_file = f"era5_resampler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("ERA5_toolbox.resampler_ERA5")


def process_year(year, variable="tp", input_dir='./', output_dir='./day', 
                 chunk_size=180, client=None, method="sum", time_shift_hours=None,
                 dataset="era5"):
    """
    Process a single year of hourly ERA5 data to daily statistics
    
    Parameters:
    -----------
    year : int
        Year to process
    variable : str
        ERA5 variable name (e.g., "tp" for total precipitation)
    input_dir : str
        Directory containing the input files
    output_dir : str
        Directory to save output files
    chunk_size : int
        Chunk size for latitude/longitude dimensions
    client : dask.distributed.Client, optional
        Dask client for parallel processing
    method : str
        Aggregation method: "sum", "mean", "max", or "min" (default: "sum").
        ERA5-Land accumulated variables (GRIB_stepType == 'accum') only accept "sum",
        computed from the 00 UTC values, see process_year_land_accum.
    time_shift_hours : int or None
        Time shift in hours (default: None). Use 'none' for no time shift.
        Negative for forward shift, positive for backward shift.
    dataset : str
        "era5" (yearly input files) or "era5-land" (monthly input files)
    """
    logger = logging.getLogger("ERA5_resampler")
    monthly = DATASET_SPECS[dataset]["monthly"]
    
    if monthly:
        current_files = [input_file_path(input_dir, variable, year, m, dataset) for m in range(1, 13)]
    else:
        current_files = [input_file_path(input_dir, variable, year, dataset=dataset)]
    
    # We need the current year file(s) regardless of time shift
    missing_files = [f for f in current_files if not os.path.exists(f)]
    if missing_files:
        logger.error(f"Current year file(s) not found: {missing_files}")
        raise FileNotFoundError(f"Current year file(s) not found: {missing_files}")
    current_file = current_files[0]
    
    # TODO: get data_var from current_file for later use; decouple "variable" in file name from data_var
    
    # Get the data variable name from the current file (needed regardless of time_shift_hours)
    with xr.open_dataset(current_file) as temp_ds:
        data_var = list(temp_ds.data_vars)[0]
        step_type = temp_ds[data_var].attrs.get('GRIB_stepType')
    
    # ERA5-Land hourly accumulations are cumulative from 00 UTC (not hourly increments),
    # so their daily "sum" is taken from the 00 UTC values instead of summing hourly values
    is_land_accum = is_land(dataset) and step_type == "accum"
    check_method(method, dataset, data_var, step_type)
    if is_land_accum:
        return process_year_land_accum(year, variable, data_var, current_files, input_dir, output_dir,
                                       chunk_size, time_shift_hours, dataset)
    
    # Determine time_shift_hours based on data type if not explicitly set
    if time_shift_hours is None:
        try:
            # Open the NetCDF file and read the attribute
            with xr.open_dataset(current_file) as temp_ds:
                # Check if the GRIB_stepType attribute exists
                if 'GRIB_stepType' in temp_ds[data_var].attrs:
                    step_type = temp_ds[data_var].attrs['GRIB_stepType']
                    logger.info(f"Found GRIB_stepType attribute: {step_type}")
                    
                    if step_type in ["accum", "avg", "mean"]:
                        time_shift_hours = -1
                        logger.info(f"Setting time_shift_hours = -1 for accumulated variable")
                    elif step_type == "instant":
                        time_shift_hours = 0
                        logger.info(f"Setting time_shift_hours = 0 for instantaneous variable")
                    else:
                        logger.warning(f"Unknown GRIB_stepType: {step_type}, defaulting to time_shift_hours = 0")
                        time_shift_hours = 0
                else:
                    logger.warning(f"No GRIB_stepType attribute found, defaulting to time_shift_hours = 0")
                    time_shift_hours = 0
        except Exception as e:
            logger.warning(f"Could not determine time shift from file metadata: {str(e)}. Using time_shift_hours = 0")
            time_shift_hours = 0
    
    logger.info(f"Processing year {year} for variable {variable} with time shift of {time_shift_hours} hours...")
    
    # Determine which files to open based on time_shift_hours
    files_to_open = list(current_files)
    
    # For negative time shift, we need the next year's data
    if time_shift_hours < 0:
        if monthly:
            next_year_file = input_file_path(input_dir, variable, year + 1, 1, dataset)
        else:
            next_year_file = input_file_path(input_dir, variable, year + 1, dataset=dataset)
        if os.path.exists(next_year_file):
            files_to_open.append(next_year_file)
            logger.info(f"Using next year file for negative time shift: {next_year_file}")
        else:
            logger.warning(f"Next year file not found: {next_year_file}")
            logger.warning(f"Processing with just the current year file (December 31 may have incomplete data)")
    
    # For positive time shift, we need the previous year's data
    elif time_shift_hours > 0:
        if monthly:
            prev_year_file = input_file_path(input_dir, variable, year - 1, 12, dataset)
        else:
            prev_year_file = input_file_path(input_dir, variable, year - 1, dataset=dataset)
        if os.path.exists(prev_year_file):
            files_to_open.insert(0, prev_year_file)
            logger.info(f"Using previous year file for positive time shift: {prev_year_file}")
        else:
            logger.warning(f"Previous year file not found: {prev_year_file}")
            logger.warning(f"Processing with just the current year file (January 1 may have incomplete data)")
    
    # We need to extend our selection window by the absolute time shift to ensure enough data
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)
    
    if time_shift_hours < 0:
        # For negative shift, extend end date
        extended_end_date = end_date + timedelta(hours=abs(time_shift_hours))
        time_slice = slice(start_date.isoformat(), extended_end_date.isoformat())
        logger.info(f"Time selection: {start_date.isoformat()} to {extended_end_date.isoformat()}")
    elif time_shift_hours > 0:
        # For positive shift, extend start date backward
        extended_start_date = start_date - timedelta(hours=time_shift_hours)
        time_slice = slice(extended_start_date.isoformat(), end_date.isoformat())
        logger.info(f"Time selection: {extended_start_date.isoformat()} to {end_date.isoformat()}")
    else:
        # No shift
        time_slice = slice(start_date.isoformat(), end_date.isoformat())
        logger.info(f"Time selection: {start_date.isoformat()} to {end_date.isoformat()}")
    
    # Open dataset with chunking for parallel processing
    logger.info(f"Opening input files: {files_to_open}")
    # Chunk spatially at open time so no task ever reads a whole (monthly/yearly) global field
    ds = xr.open_mfdataset(files_to_open, 
                          chunks={"valid_time": -1, "latitude": chunk_size, "longitude": chunk_size}).sel(
                              valid_time=time_slice
                          )
    
    # Examine "method" based on ds[data_var].attrs['GRIB_stepType']
    # Expect `method="sum"` for accumulated variables
    if ds[data_var].attrs['GRIB_stepType'] == 'accum':
        if method != 'sum':
            logger.warning(f"Step type of {data_var} is 'accum', but method is '{method}'. Please examine the method setting.")
    
    try:
        # Calculate daily statistics with the specified time shift
        logger.info(f"Calculating daily {method} with time shift of {time_shift_hours} hours...")
        ds_daily = earthkit.transforms.aggregate.temporal.daily_reduce(
            ds, 
            how=method, 
            time_dim="valid_time",
            time_shift={"hours": time_shift_hours},
            remove_partial_periods=True
        )
        
        # Compute the result
        # logger.info("Computing results...")
        ds_daily = ds_daily.compute()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the output - include time shift in filename
        output_file = output_file_path(output_dir, variable, year, method, dataset)
        logger.info(f"Saving output to {output_file}")
        encoding = {
            list(ds_daily.data_vars.keys())[0]: {
                # 'chunksizes': (74, 145, 288),  # Time, lat, lon chunks
                'zlib': True, 'complevel': 1,
                # 'shuffle': True,
            }
        }
        ds_daily.to_netcdf(output_file, encoding=encoding)
        
        logger.info(f"Finished processing year {year}")
    finally:
        # Ensure dataset is closed to release resources
        logger.info("Closing dataset")
        ds.close()
        
        # Force garbage collection to free memory
        logger.info("Running garbage collection")
        gc.collect()


def check_method(method, dataset, data_var, step_type):
    """Reject method/variable combinations that silently give wrong daily values"""
    if DATASET_SPECS[dataset]["freq"] == "00utc" and step_type != "accum":
        raise ValueError(
            f"{data_var} is not an accumulated variable (GRIB_stepType={step_type!r}): {dataset} files hold "
            f"only 00 UTC snapshots, which give no daily {method}. Download hourly data for it instead.")
    is_land_accum = is_land(dataset) and step_type == "accum"
    if is_land_accum and method != "sum":
        raise ValueError(
            f"{data_var} is an ERA5-Land accumulated variable (GRIB_stepType='accum'): its hourly values are "
            f"accumulated from 00 UTC (01 UTC holds 00-01, 23 UTC holds 00-23, 00 UTC of the next day holds the "
            f"whole previous day), so a daily '{method}' of them is not meaningful. Use --method sum, which "
            f"takes the 00 UTC value of the following day as the daily total.")


def process_year_land_accum(year, variable, data_var, current_files, input_dir, output_dir,
                            chunk_size, time_shift_hours, dataset="era5-land"):
    """
    Daily totals of an ERA5-Land accumulated variable for one year.
    
    ERA5-Land accumulations restart at 00 UTC, and the value at D+1 00 UTC covers the whole of day D.
    So the daily total of day D is the 00 UTC value at D+1, relabelled to D 00:00 (the same labelling
    as daily_reduce outputs). Year Y therefore needs 00 UTC of Y-01-02 ... Y+1-01-01, i.e. January of
    Y+1; the Y-01-01 00 UTC value belongs to Y-1 and is excluded. No summing, no daily_reduce.
    """
    logger = logging.getLogger("ERA5_resampler")
    
    # A time shift has no meaning here: the 00 UTC value is the daily total by definition
    if time_shift_hours:
        raise ValueError(f"--time-shift-hours {time_shift_hours} is not applicable to ERA5-Land accumulated variables "
                         f"(the daily total is the 00 UTC value of the following day); omit it or pass 0/none")
    logger.info(f"ERA5-Land accumulated variable, method 'sum': daily total of day D = {data_var} at D+1 00 UTC; time shift not used")
    
    files_to_open = list(current_files)
    next_year_file = input_file_path(input_dir, variable, year + 1, 1, dataset)
    if os.path.exists(next_year_file):
        files_to_open.append(next_year_file)
        logger.info(f"Using next year file for the December 31 total: {next_year_file}")
    else:
        logger.warning(f"Next year file not found: {next_year_file}")
        logger.warning(f"Processing with just the current year files (December 31 will be missing)")
    
    first = datetime(year, 1, 2)   # 00 UTC of Jan 2 -> total of Jan 1
    last = datetime(year + 1, 1, 1)  # 00 UTC of next Jan 1 -> total of Dec 31
    
    # Select the 00 UTC steps on the lazy backend arrays *before* chunking, so dask only ever
    # reads those timesteps (not whole hourly months)
    parts, opened = [], []
    try:
        for f in files_to_open:
            ds_m = xr.open_dataset(f, cache=False)  # lazy backend array; no in-memory caching
            opened.append(ds_m)
            t = ds_m.indexes["valid_time"]
            keep = (t.hour == 0) & (t.minute == 0) & (t >= first) & (t <= last)
            if keep.any():
                parts.append(ds_m[[data_var]].isel(valid_time=keep.nonzero()[0]).chunk(
                    {"valid_time": -1, "latitude": chunk_size, "longitude": chunk_size}))
        ds = xr.concat(parts, dim="valid_time", data_vars="minimal", coords="minimal",
                       compat="override", combine_attrs="override")
        
        # Label each total with the day it belongs to (D+1 00:00 -> D 00:00)
        ds = ds.drop_vars("expver", errors="ignore")
        ds = ds.assign_coords(valid_time=ds["valid_time"] - pd.Timedelta(days=1))
        ds["valid_time"].attrs.update({
            "time_shift": "-1 days +00:00:00",
            "comment": "Label D 00:00 = daily total of day D (00-24 UTC), taken from the 00 UTC value of day D+1",
        })
        ds[data_var].attrs["cell_methods"] = "valid_time: sum (daily total)"
        ds[data_var].attrs["comment"] = ("Daily total for the labeled day D, derived from the ERA5-Land "
                                         "00 UTC value of the following day (accumulation from 00 UTC)")
        
        n_expected = (datetime(year + 1, 1, 1) - datetime(year, 1, 1)).days
        logger.info(f"Selected {ds.sizes['valid_time']} daily totals (expected {n_expected})")
        ds_daily = ds.compute()
        
        os.makedirs(output_dir, exist_ok=True)
        output_file = output_file_path(output_dir, variable, year, "sum", dataset)
        logger.info(f"Saving output to {output_file}")
        ds_daily.to_netcdf(output_file, encoding={data_var: {'zlib': True, 'complevel': 1}})
        logger.info(f"Finished processing year {year}")
    finally:
        for d in opened:
            d.close()
        gc.collect()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process ERA5 hourly data to daily statistics')
    parser.add_argument('years', type=int, nargs='+', help='Years to process')
    parser.add_argument('--dataset', type=str, choices=list(DATASET_SPECS), default='era5',
                      help='Input dataset: era5 (yearly files), era5-land (monthly files) or era5-land-00utc (yearly 00 UTC files of accumulated variables)')
    parser.add_argument('--variable', type=str, default='tp', 
                      help='ERA5 variable name (e.g., tp for precipitation)')
    parser.add_argument('--input-dir', type=str, default='./', 
                      help='Directory containing input files')
    parser.add_argument('--output-dir', type=str, default='./day', 
                      help='Directory to save output files')
    parser.add_argument('--workers', type=int, default=8, 
                      help='Number of dask workers (prefer many workers with --threads 1)')
    parser.add_argument('--memory-limit', type=str, default=None,
                      help='Memory limit per dask worker, e.g. 100GB (default: max of the whole-year '
                           'gather need and 90%% of available RAM / workers). It is a cap, not a '
                           'reservation, so workers x limit may exceed RAM')
    parser.add_argument('--threads', type=int, default=2, 
                      help='Threads per dask worker')
    parser.add_argument('--chunk-size', type=int, default=180, 
                      help='Chunk size for spatial dimensions')
    parser.add_argument('--method', type=str, choices=['sum', 'mean', 'max', 'min'],
                      default='sum', help='Aggregation method for daily statistics')
    parser.add_argument(
        '--time-shift-hours', type=lambda x: None if x.lower() == 'none' else int(x), default=None,
        help="Time shift in hours (default: None). Use 'none' for no time shift.\n"
             "Negative for forward shift, positive for backward shift.\n"
             "-1 is normally used for accumulated variables like precipitation."
    )
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                      default='INFO', help='Logging level')
    parser.add_argument('--log-dir', type=str, default=None,
                      help='Directory to store log files')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = getattr(logging, args.log_level)
    logger = setup_logging(log_level=log_level, log_dir=args.log_dir)
    
    # Process each year independently so one failure doesn't stop the rest
    for year in args.years:
        try:
            # Per-worker limit: any worker must be able to hold the whole year of daily output
            n_workers = args.workers
            memory_limit = plan_memory(year, args.variable, args.input_dir, args.dataset,
                                       n_workers, args.memory_limit)
            # Setup dask client for each year to avoid resource accumulation/leakage
            logger.info(f"Initializing dask client for year {year} with {n_workers} workers and {args.threads} threads per worker")
            # Use a context manager to ensure the client is properly closed after each year
            with dask.distributed.Client(n_workers=n_workers, threads_per_worker=args.threads,
                                         memory_limit=memory_limit) as client:
                process_year(
                    year,
                    variable=args.variable,
                    input_dir=args.input_dir,
                    output_dir=args.output_dir,
                    chunk_size=args.chunk_size,
                    client=client,
                    method=args.method,
                    time_shift_hours=args.time_shift_hours,
                    dataset=args.dataset
                )
        except Exception:
            logger.exception(f"Error processing year {year}")

    return args


if __name__ == "__main__":
    # Get a consistent logger instance
    logger = logging.getLogger("ERA5_toolbox.resampler_ERA5")
    start_time = datetime.now()
    
    try:
        main()
    except Exception as e:
        # Log any uncaught exceptions at the highest level
        logger.critical(f"Fatal error in main program: {str(e)}", exc_info=True)
    finally:
        # Always log total processing time, even if there was an error
        end_time = datetime.now()
        logger.info(f"Total processing time: {end_time - start_time}")