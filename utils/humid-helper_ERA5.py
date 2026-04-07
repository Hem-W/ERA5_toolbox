"""
ERA5 Relative Humidity Helper

Author: Hui-Min Wang
TODO: Experimental! Not included in the package.

This script calculates relative humidity from ERA5 temperature (t2m) and 
dew point temperature (d2m) data for specified years.

Usage:
    python humid-helper_ERA5.py --years 2020 --t2m_dir /path/to/t2m --d2m_dir /path/to/d2m --out_dir /path/to/out
    python humid-helper_ERA5.py --years $(seq 2020 2025) --t2m_dir /path/to/t2m --d2m_dir /path/to/d2m --out_dir /path/to/out
    python humid-helper_ERA5.py --years 2020 --t2m_dir /path/to/t2m --d2m_dir /path/to/d2m --out_dir /path/to/out --parallel
"""

import os
import argparse
import xarray as xr
import earthkit.meteo.thermo.array.thermo as thermo
from dask.distributed import Client


def calculate_relative_humidity(t2m_file, d2m_file, output_file, chunk_size=50):
    """
    Calculate relative humidity from temperature and dew point temperature.
    
    Parameters:
    -----------
    t2m_file : str
        Path to temperature data NetCDF file
    d2m_file : str
        Path to dew point temperature data NetCDF file
    output_file : str
        Path to save the resulting humidity data
    chunk_size : int, optional
        Size of chunks for dask processing
    """
    print(f"Processing: {t2m_file} and {d2m_file}")
    print(f"Output will be saved to: {output_file}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Load and chunk the datasets
    ds_t2m = xr.open_dataset(t2m_file).chunk({"latitude": chunk_size, "longitude": chunk_size})
    ds_d2m = xr.open_dataset(d2m_file).chunk({"latitude": chunk_size, "longitude": chunk_size})
    
    # Calculate relative humidity using earthkit
    print("Calculating relative humidity...")
    da_r2m = xr.apply_ufunc(
        thermo.relative_humidity_from_dewpoint,
        ds_t2m.t2m,
        ds_d2m.d2m,
        dask="parallelized"
    )
    
    # Compute the result (this may take some time for large datasets)
    da_r2m = da_r2m.compute()
    
    # Set metadata attributes
    da_r2m = da_r2m.assign_attrs({
        "units": "%",
        "cell_methods": "",
        "history": "earthkit.meteo.thermo.array.thermo.relative_humidity_from_dewpoint(t2m, d2m)",
        "standard_name": "relative_humidity",
        "long_name": "Relative humidity (calculated from t2m and d2m)",
        "description": "Computed from temperature, and dew point temperature."
    })
    
    # Rename to r2m
    da_r2m = da_r2m.rename("r2m")
    
    # Save to NetCDF file with compression
    print(f"Saving result to {output_file}")
    da_r2m.to_netcdf(
        output_file,
        encoding={"r2m": {"zlib": True, "complevel": 1}}
    )
    
    print("Done!")
    return output_file


def process_year(year, t2m_dir, d2m_dir, out_dir):
    """
    Process a single year of ERA5 data.
    
    Parameters:
    -----------
    year : int
        Year to process
    base_dir : str, optional
        Base directory for ERA5 data, defaults to current directory
    
    Returns:
    --------
    str
        Path to the output file
    """
    
    # Define file paths
    t2m_file = os.path.join(t2m_dir, f"era5.reanalysis.t2m.1hr.0p25deg.global.{year}.nc")
    d2m_file = os.path.join(d2m_dir, f"era5.reanalysis.d2m.1hr.0p25deg.global.{year}.nc")
    r2m_dir = out_dir
    os.makedirs(r2m_dir, exist_ok=True)
    output_file = os.path.join(r2m_dir, f"era5.reanalysis.r2m.1hr.0p25deg.global.{year}.nc")
    
    # Check if input files exist
    if not os.path.exists(t2m_file):
        raise FileNotFoundError(f"Temperature file not found: {t2m_file}")
    if not os.path.exists(d2m_file):
        raise FileNotFoundError(f"Dew point file not found: {d2m_file}")
    
    # Calculate relative humidity
    return calculate_relative_humidity(t2m_file, d2m_file, output_file)


def main():
    parser = argparse.ArgumentParser(description="Calculate relative humidity from ERA5 temperature and dew point data")
    parser.add_argument('--years', type=int, nargs='+', help='List of years to process')
    parser.add_argument('--t2m_dir', type=str, default=None,
                       help='Base directory for ERA5 t2m data (default: current directory)')
    parser.add_argument('--d2m_dir', type=str, default=None,
                       help='Base directory for ERA5 d2m data (default: current directory)')
    parser.add_argument('--out_dir', type=str, default=None,
                       help='Base directory for ERA5 r2m data (default: current directory)')
    parser.add_argument('--parallel', action='store_true',
                       help='Use dask parallel processing')
    
    args = parser.parse_args()
    
    # Setup dask client for parallel processing
    if args.parallel:
        client = Client(processes=False)
        print(f"Dask dashboard available at: {client.dashboard_link}")
    
    # Determine years to process
    if args.years:
        years_to_process = args.years
    
    print(f"Processing years: {years_to_process}")
    
    # Process each year
    output_files = []
    for year in years_to_process:
        try:
            output_file = process_year(year, args.t2m_dir, args.d2m_dir, args.out_dir)
            output_files.append(output_file)
        except Exception as e:
            print(f"Error processing year {year}: {e}")
    
    print("\nSummary:")
    print(f"Successfully processed {len(output_files)} out of {len(years_to_process)} years.")
    for file in output_files:
        print(f" - {file}")


if __name__ == "__main__":
    main()
