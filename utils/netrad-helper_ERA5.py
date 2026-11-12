"""
ERA5 Surface Net Radiation Helper

Author: Hui-Min Wang
TODO: Experimental! Not included in the package.

This script calculates daily surface net radiation (Rn) from ERA5 daily-summed
surface net short-wave (ssr) and surface net long-wave (str) radiation:

    Rn = ssr + str        [J m**-2, daily sum]

Both inputs are downward-positive (str is usually negative), so the sum is the
net downward radiative energy received by the surface each day.
Divide by 86400 for W m**-2, or by 1e6 for MJ m**-2 day**-1 (FAO-56).

Usage:
    python netrad-helper_ERA5.py --years 2020 --ssr_dir day/ssr --str_dir day/str --out_dir day_derived/rn
    python netrad-helper_ERA5.py --years $(seq 1980 2024) --ssr_dir day/ssr --str_dir day/str --out_dir day_derived/rn --workers 4
"""

import os
import sys
import argparse
from datetime import datetime
import xarray as xr
from concurrent.futures import ProcessPoolExecutor, as_completed


OUT_VAR = "rn"


def calculate_net_radiation(ssr_file, str_file, output_file):
    """
    Calculate daily surface net radiation from daily ssr and str.

    Parameters:
    -----------
    ssr_file : str
        Path to daily-summed surface net short-wave radiation NetCDF file
    str_file : str
        Path to daily-summed surface net long-wave radiation NetCDF file
    output_file : str
        Path to save the resulting net radiation data
    """
    print(f"Processing: {ssr_file} and {str_file}")

    with xr.open_dataset(ssr_file) as ds_ssr, xr.open_dataset(str_file) as ds_str:
        da_ssr = ds_ssr["ssr"]
        da_str = ds_str["str"]

        # Guard against silently misaligned inputs (xarray would otherwise inner-join)
        for dim in da_ssr.dims:
            if not da_ssr[dim].equals(da_str[dim]):
                raise ValueError(f"Coordinate '{dim}' differs between {ssr_file} and {str_file}")

        da_rn = (da_ssr + da_str).astype("float32").rename(OUT_VAR)
        # Carry over the daily time-shift note from the resampler
        da_rn["valid_time"].attrs = da_ssr["valid_time"].attrs

        da_rn.attrs = {
            "units": da_ssr.attrs.get("units", "J m**-2"),
            "standard_name": "surface_net_downward_radiative_flux",
            "long_name": "Surface net radiation (calculated from ssr and str)",
            "cell_methods": "valid_time: sum (daily)",
            "history": "rn = ssr + str (daily sums of ERA5 surface_net_solar_radiation and surface_net_thermal_radiation)",
            "description": "Net downward radiation at the surface (Rn). Divide by 86400 for W m**-2, or by 1e6 for MJ m**-2 day**-1.",
        }

        # CF-style global history: newest entry first, followed by the inputs' own history
        ssr_path = os.path.abspath(ssr_file)
        str_path = os.path.abspath(str_file)
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        history = "\n".join(filter(None, [
            f"{timestamp} {' '.join(sys.argv)}: {OUT_VAR} = ssr + str; ssr from {ssr_path}; str from {str_path}",
            f"[ssr] {ds_ssr.attrs.get('history', '')}",
            f"[str] {ds_str.attrs.get('history', '')}",
        ]))

        ds_out = da_rn.to_dataset()
        ds_out.attrs = {
            "Conventions": ds_ssr.attrs.get("Conventions", "CF-1.7"),
            "institution": ds_ssr.attrs.get("institution", ""),
            "source": f"{os.path.basename(ssr_file)}, {os.path.basename(str_file)}",
            "history": history,
        }

        # Write to a temporary file first so an interrupted run never leaves a partial output
        tmp_file = output_file + ".tmp"
        print(f"Saving result to {output_file}")
        ds_out.to_netcdf(
            tmp_file,
            encoding={OUT_VAR: {"zlib": True, "complevel": 1, "shuffle": True,
                                "chunksizes": (74, 145, 288)}},
        )
    os.replace(tmp_file, output_file)

    print(f"Done: {output_file}")
    return output_file


def process_year(year, ssr_dir, str_dir, out_dir, overwrite=False):
    """
    Process a single year of ERA5 daily data.

    Returns:
    --------
    str
        Path to the output file
    """
    ssr_file = os.path.join(ssr_dir, f"era5.reanalysis.ssr.daysum.0p25deg.global.{year}.nc")
    str_file = os.path.join(str_dir, f"era5.reanalysis.str.daysum.0p25deg.global.{year}.nc")
    os.makedirs(out_dir, exist_ok=True)
    output_file = os.path.join(out_dir, f"era5.reanalysis.{OUT_VAR}.daysum.0p25deg.global.{year}.nc")

    if os.path.exists(output_file) and not overwrite:
        print(f"Skipping existing file: {output_file}")
        return output_file

    if not os.path.exists(ssr_file):
        raise FileNotFoundError(f"ssr file not found: {ssr_file}")
    if not os.path.exists(str_file):
        raise FileNotFoundError(f"str file not found: {str_file}")

    return calculate_net_radiation(ssr_file, str_file, output_file)


def main():
    parser = argparse.ArgumentParser(description="Calculate daily surface net radiation (rn = ssr + str) from ERA5 daily data")
    parser.add_argument('--years', type=int, nargs='+', required=True, help='List of years to process')
    parser.add_argument('--ssr_dir', type=str, default='day/ssr',
                        help='Directory for ERA5 daily ssr data (default: day/ssr)')
    parser.add_argument('--str_dir', type=str, default='day/str',
                        help='Directory for ERA5 daily str data (default: day/str)')
    parser.add_argument('--out_dir', type=str, default=f'day_derived/{OUT_VAR}',
                        help=f'Directory for output rn data (default: day_derived/{OUT_VAR})')
    parser.add_argument('--workers', type=int, default=1,
                        help='Number of years processed in parallel (~3 GB RAM each, default: 1)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')

    args = parser.parse_args()
    print(f"Processing years: {args.years}")

    failed = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_year, year, args.ssr_dir, args.str_dir, args.out_dir, args.overwrite): year
            for year in args.years
        }
        for future in as_completed(futures):
            year = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                failed.append(year)

    if failed:
        print(f"Failed years: {sorted(failed)}")
    else:
        print("All years processed successfully")


if __name__ == "__main__":
    main()
