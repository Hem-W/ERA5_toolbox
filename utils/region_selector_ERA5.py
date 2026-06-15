"""
ERA5 Region Selector (xarray-based `cdo sellonlatbox` replacement)

Author: Hui-Min Wang
TODO: Experimental! Not included in the package.

Extract a longitude/latitude box from (global) ERA5 NetCDF files using xarray.
This is a fast alternative to `cdo sellonlatbox,lon1,lon2,lat1,lat2` which can be
very slow when subsetting a small region out of a large global file.

The longitude box uses the same convention as CDO:
    + Bounds may be given in either 0..360 or -180..180; membership is computed
      with modular (mod 360) arithmetic, so the requested box is matched
      regardless of the file's longitude convention.
    + To select a box crossing the prime meridian / date line, pass lon1 > lon2
      (e.g. --box 350 10 -15 22), exactly like CDO.
    + Output longitude values keep the file's original convention, and are
      reordered to be geographically contiguous across the seam.
Latitude ordering of the input file (e.g. decreasing 90..-90 for ERA5) is preserved.

Usage:
    # equivalent to:
    #   cdo -O sellonlatbox,86,140,-15,22 in.nc out.nc
    python region_selector_ERA5.py in.nc out.nc --box 86 140 -15 22

    # explicit bounds, custom compression and time chunking
    python region_selector_ERA5.py in.nc out.nc \
        --lon-min 86 --lon-max 140 --lat-min -15 --lat-max 22 \
        --chunk-time 240 --complevel 1

Advanced Usage:
    # batch over years (mirrors the global -> MC example)
    for y in $(seq 2015 2020); do \
        python region_selector_ERA5.py \
            /home/food/dataset/ERA5/hour/t/era5.reanalysis.t.500hpa.1hr.0p25deg.global.$y.nc \
            /home/food/niuzx/Work/WSRP/ERA5_hourly/t/era5.reanalysis.t.500hpa.1hr.0p25deg.MC.$y.nc \
            --box 86 140 -15 22; \
    done
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import dask
import numpy as np
import xarray as xr


def setup_logging(log_level=logging.INFO, log_dir=None):
    """Set up logging with both file and console output."""
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(
            log_dir, f"era5_region_selector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
    else:
        log_file = f"era5_region_selector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger("ERA5_toolbox.region_selector_ERA5")


def _detect_coord(ds, candidates):
    """Return the first coordinate name present in the dataset, else None."""
    for name in candidates:
        if name in ds.coords or name in ds.dims:
            return name
    return None


def _lon_indices(lon_values, lon1, lon2):
    """
    Compute the longitude indices inside the CDO-style box [lon1, lon2].

    Membership is evaluated with mod-360 arithmetic so the file's longitude
    convention (0..360 or -180..180) does not matter. Pass lon1 > lon2 to
    select a box that wraps across the seam (prime meridian / date line).
    The returned indices are ordered by increasing modular distance from lon1,
    yielding a geographically contiguous slice across the seam.
    """
    lon_values = np.asarray(lon_values, dtype="float64")
    span = float(lon2) - float(lon1)

    if span >= 360.0:
        # Whole globe (e.g. 0..360 or -180..180); keep original order.
        return np.arange(lon_values.size)

    # Python's % always returns a non-negative result for a positive modulus,
    # which is exactly the wrap-around behaviour we want.
    dist = (lon_values - float(lon1)) % 360.0
    width = span % 360.0
    in_box = dist <= width + 1e-9  # tolerance for float-grid edges

    idx = np.where(in_box)[0]
    # Order across the seam: ascending modular distance from lon1.
    idx = idx[np.argsort(dist[idx], kind="stable")]
    return idx


def _lat_indices(lat_values, lat1, lat2):
    """Return latitude indices in [min, max], preserving the file's order."""
    lat_values = np.asarray(lat_values, dtype="float64")
    lo, hi = min(float(lat1), float(lat2)), max(float(lat1), float(lat2))
    in_box = (lat_values >= lo - 1e-9) & (lat_values <= hi + 1e-9)
    return np.where(in_box)[0]


def select_region(
    input_file,
    output_file,
    lon1,
    lon2,
    lat1,
    lat2,
    chunk_time=None,
    complevel=1,
    overwrite=True,
):
    """
    Extract a lon/lat box from a NetCDF file and write it to ``output_file``.

    Parameters
    ----------
    input_file : str
        Path to the source NetCDF file.
    output_file : str
        Path to the destination NetCDF file.
    lon1, lon2 : float
        Western and eastern longitude bounds (CDO order). lon1 > lon2 wraps.
    lat1, lat2 : float
        Latitude bounds (any order).
    chunk_time : int or None
        Dask chunk size along the time dimension. ``None`` (default) aligns
        with the file's native on-disk time chunk size, which avoids
        repeatedly decompressing the same compressed chunk and is essential
        for speed. Override only if you understand the storage layout.
    complevel : int
        zlib compression level for the output (0 disables compression).
    overwrite : bool
        Overwrite the output file if it exists (mirrors ``cdo -O``).
    """
    logger = logging.getLogger("ERA5_toolbox.region_selector_ERA5")

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if os.path.exists(output_file) and not overwrite:
        raise FileExistsError(
            f"Output file already exists (use overwrite): {output_file}"
        )

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Peek at coordinate names / time dimension without decoding everything.
    with xr.open_dataset(input_file) as probe:
        lon_name = _detect_coord(probe, ["longitude", "lon"])
        lat_name = _detect_coord(probe, ["latitude", "lat"])
        if lon_name is None or lat_name is None:
            raise KeyError(
                f"Could not find longitude/latitude coordinates. "
                f"Available coords: {list(probe.coords)}"
            )
        time_name = _detect_coord(probe, ["valid_time", "time"])
        lon_values = probe[lon_name].values
        lat_values = probe[lat_name].values
        n_lon_full, n_lat_full = lon_values.size, lat_values.size

        # Determine the file's native on-disk chunk size along the time dim so
        # dask chunks can be aligned to it (avoids redundant decompression).
        native_time_chunk = None
        if time_name is not None:
            main_var = max(
                probe.data_vars,
                key=lambda v: probe[v].ndim if time_name in probe[v].dims else -1,
                default=None,
            )
            if main_var is not None and time_name in probe[main_var].dims:
                chunksizes = probe[main_var].encoding.get("chunksizes")
                if chunksizes:
                    t_axis = probe[main_var].dims.index(time_name)
                    native_time_chunk = int(chunksizes[t_axis])

    lon_idx = _lon_indices(lon_values, lon1, lon2)
    lat_idx = _lat_indices(lat_values, lat1, lat2)

    if lon_idx.size == 0 or lat_idx.size == 0:
        raise ValueError(
            f"Empty selection for box lon=[{lon1}, {lon2}], lat=[{lat1}, {lat2}]. "
            f"File lon range: [{lon_values.min()}, {lon_values.max()}], "
            f"lat range: [{lat_values.min()}, {lat_values.max()}]."
        )

    logger.info(f"Input : {input_file}")
    logger.info(f"Output: {output_file}")
    logger.info(
        f"Box   : lon=[{lon1}, {lon2}], lat=[{lat1}, {lat2}] "
        f"({lon_name}, {lat_name})"
    )
    logger.info(
        f"Subset: {lon_idx.size}/{n_lon_full} lon x {lat_idx.size}/{n_lat_full} lat points"
    )

    if time_name is None:
        chunks = {}
    else:
        # Prefer the explicit override, else the native on-disk chunk, else "auto".
        t_chunk = chunk_time if chunk_time is not None else native_time_chunk
        chunks = {time_name: t_chunk} if t_chunk else {}
        logger.info(
            f"Time chunking: {t_chunk if t_chunk else 'auto'} "
            f"(native on-disk: {native_time_chunk})"
        )
    ds = xr.open_dataset(input_file, chunks=chunks)
    try:
        ds_sel = ds.isel({lon_name: lon_idx, lat_name: lat_idx})

        # Build encoding: keep compression, drop stale on-disk chunk sizes that
        # may exceed the (smaller) subset dimensions.
        encoding = {}
        for var in ds_sel.data_vars:
            enc = {}
            if complevel and complevel > 0:
                enc["zlib"] = True
                enc["complevel"] = int(complevel)
            encoding[var] = enc

        # Append provenance to the global history attribute (CDO-like).
        stamp = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
        op = f"region_selector_ERA5 sellonlatbox,{lon1},{lon2},{lat1},{lat2}"
        prev_hist = ds_sel.attrs.get("history", "")
        ds_sel.attrs["history"] = (f"{stamp}: {op}\n{prev_hist}").strip()

        logger.info("Writing subset (streaming via dask)...")
        # HDF5/netCDF4 is not thread-safe. With dask's default multi-threaded
        # scheduler, many worker threads contend for the single HDF5 global
        # lock while concurrently reading the source and writing the output,
        # which can deadlock (more cores -> higher chance). Force the
        # single-threaded synchronous scheduler: it removes the contention
        # entirely with no real speed penalty for this I/O-bound subsetting.
        with dask.config.set(scheduler="synchronous"):
            ds_sel.to_netcdf(output_file, encoding=encoding)
        logger.info("Done.")
    finally:
        ds.close()

    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Extract a lon/lat box from ERA5 NetCDF files (xarray-based "
        "fast replacement for `cdo sellonlatbox`).",
    )
    parser.add_argument("input", type=str, help="Input NetCDF file")
    parser.add_argument("output", type=str, help="Output NetCDF file")

    box = parser.add_argument_group("region (CDO order: lon1 lon2 lat1 lat2)")
    box.add_argument(
        "--box",
        type=float,
        nargs=4,
        metavar=("LON1", "LON2", "LAT1", "LAT2"),
        default=None,
        help="Bounding box in CDO order. lon1 > lon2 wraps across the seam.",
    )
    box.add_argument("--lon-min", type=float, default=None, help="Western longitude (lon1)")
    box.add_argument("--lon-max", type=float, default=None, help="Eastern longitude (lon2)")
    box.add_argument("--lat-min", type=float, default=None, help="Southern latitude (lat1)")
    box.add_argument("--lat-max", type=float, default=None, help="Northern latitude (lat2)")

    parser.add_argument(
        "--chunk-time", type=int, default=None,
        help="Dask chunk size along the time dimension. Default aligns with "
             "the file's native on-disk chunk size (recommended for speed).",
    )
    parser.add_argument(
        "--complevel", type=int, default=1,
        help="zlib compression level for output, 0 disables (default: 1)",
    )
    parser.add_argument(
        "--no-overwrite", action="store_true",
        help="Do not overwrite an existing output file",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    parser.add_argument("--log-dir", type=str, default=None, help="Directory for log files")

    args = parser.parse_args()

    if args.box is not None:
        lon1, lon2, lat1, lat2 = args.box
    else:
        missing = [
            name for name, val in [
                ("--lon-min", args.lon_min), ("--lon-max", args.lon_max),
                ("--lat-min", args.lat_min), ("--lat-max", args.lat_max),
            ] if val is None
        ]
        if missing:
            parser.error(
                "Provide --box LON1 LON2 LAT1 LAT2, or all of "
                "--lon-min/--lon-max/--lat-min/--lat-max. Missing: "
                + ", ".join(missing)
            )
        lon1, lon2, lat1, lat2 = args.lon_min, args.lon_max, args.lat_min, args.lat_max

    logger = setup_logging(getattr(logging, args.log_level), args.log_dir)

    start_time = datetime.now()
    try:
        select_region(
            args.input,
            args.output,
            lon1, lon2, lat1, lat2,
            chunk_time=args.chunk_time,
            complevel=args.complevel,
            overwrite=not args.no_overwrite,
        )
    except Exception as e:
        logger.critical(f"Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info(f"Total processing time: {datetime.now() - start_time}")


if __name__ == "__main__":
    main()
