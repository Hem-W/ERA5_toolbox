"""
Process ERA5 hourly data to daily statistics

Author: Hui-Min Wang
TODO: Experimental! Not included in the package.

This script converts ERA5 hourly data to daily statistics (sum, mean, max, or min).
For accumulated variables like precipitation (tp), a time shift can be applied
to correctly align the data with the daily periods.

Features:
    + dask distributed: memory limit auto determined by RAM/number of workers

Usage:
    python resampler_ERA5.py 2020 --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1
    python resampler_ERA5.py $(seq 2020 2025) --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1
    python resampler_ERA5.py 2020 --variable tp --input-dir /path/to/input --output-dir /path/to/output --chunk-size 180 --workers 4 --threads 2 --method sum --time-shift-hours -1 --log-level DEBUG --log-dir /path/to/log
    
Advanced Usage:
    + multiple variables
        for var in swvl1 swvl2; do python resampler_ERA5.py $(seq 2003 2021) --variable $var --input-dir hour/${var} --output-dir day/${var} --workers 10 --method mean; done
    + include pressure levels
        for var in d q t; do for pres in 500hpa 1000hpa; do combined="${var}.${pres}"; python resampler_ERA5.py $(seq 2003 2021) --variable $combined --input-dir hour/${var} --output-dir day/${var} --workers 20 --method mean --chunk-size 150; done; done
"""

import xarray as xr
import earthkit.transforms.aggregate
import dask.distributed
import argparse
import os
import logging
import gc
from datetime import datetime, timedelta


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
                 chunk_size=180, client=None, method="sum", time_shift_hours=None):
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
        Aggregation method: "sum", "mean", "max", or "min" (default: "sum")
    time_shift_hours : int or None
        Time shift in hours (default: None). Use 'none' for no time shift.
        Negative for forward shift, positive for backward shift.
    """
    logger = logging.getLogger("ERA5_resampler")
    
    current_file = os.path.join(input_dir, f"era5.reanalysis.{variable}.1hr.0p25deg.global.{year}.nc")
    
    # We need the current year file regardless of time shift
    if not os.path.exists(current_file):
        logger.error(f"Current year file not found: {current_file}")
        raise FileNotFoundError(f"Current year file not found: {current_file}")
    
    # TODO: get data_var from current_file for later use; decouple "variable" in file name from data_var
    
    # Determine time_shift_hours based on data type if not explicitly set
    if time_shift_hours is None:
        try:
            # Open the NetCDF file and read the attribute
            with xr.open_dataset(current_file) as temp_ds:
                # Get the first (and typically only) data variable name
                data_var = list(temp_ds.data_vars)[0]
                
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
    files_to_open = []
    files_to_open.append(current_file)
    
    # For negative time shift, we need the next year's data
    if time_shift_hours < 0:
        next_year_file = os.path.join(input_dir, f"era5.reanalysis.{variable}.1hr.0p25deg.global.{year+1}.nc")
        if os.path.exists(next_year_file):
            files_to_open.append(next_year_file)
            logger.info(f"Using next year file for negative time shift: {next_year_file}")
        else:
            logger.warning(f"Next year file not found: {next_year_file}")
            logger.warning(f"Processing with just the current year file (December 31 may have incomplete data)")
    
    # For positive time shift, we need the previous year's data
    elif time_shift_hours > 0:
        prev_year_file = os.path.join(input_dir, f"era5.reanalysis.{variable}.1hr.0p25deg.global.{year-1}.nc")
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
    ds = xr.open_mfdataset(files_to_open, 
                          chunks={"valid_time": -1}).sel(
                              valid_time=time_slice
                          ).chunk({"latitude": chunk_size, "longitude": chunk_size})
    
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
        output_file = os.path.join(output_dir, f"era5.reanalysis.{variable}.day{method}.0p25deg.global.{year}.nc")
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


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process ERA5 hourly data to daily statistics')
    parser.add_argument('years', type=int, nargs='+', help='Years to process')
    parser.add_argument('--variable', type=str, default='tp', 
                      help='ERA5 variable name (e.g., tp for precipitation)')
    parser.add_argument('--input-dir', type=str, default='./', 
                      help='Directory containing input files')
    parser.add_argument('--output-dir', type=str, default='./day', 
                      help='Directory to save output files')
    parser.add_argument('--workers', type=int, default=4, 
                      help='Number of dask workers')
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
    
    # Setup dask client once
    logger.info(f"Initializing dask client with {args.workers} workers and {args.threads} threads per worker")
    client = dask.distributed.Client(n_workers=args.workers, threads_per_worker=args.threads)
    
    try:
        # Process each year
        for year in args.years:
            process_year(
                year,
                variable=args.variable,
                input_dir=args.input_dir,
                output_dir=args.output_dir,
                chunk_size=args.chunk_size,
                client=client,
                method=args.method,
                time_shift_hours=args.time_shift_hours
            )
        logger.info("All years processed successfully")
    except Exception as e:
        logger.exception(f"Error during processing: {str(e)}")
        # Don't re-raise the exception to allow cleanup to happen
    finally:
        # Ensure client is closed even if an error occurs
        logger.info("Closing dask client")
        client.close()
    
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