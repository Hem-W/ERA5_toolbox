#!/usr/bin/env python3
"""
Add missing history attribute to NetCDF files

This script adds the missing 'history' attribute to existing NetCDF files.
It will add: 'earthkit.meteo.thermo.array.thermo.relative_humidity_from_dewpoint(t2m, d2m)'
to the r2m variable's attributes.
"""

import os
import sys
import netCDF4 as nc
import glob

def add_history_attribute(filename):
    """Add history attribute to a NetCDF file"""
    print(f'Processing {filename}...')
    
    try:
        # Open the file in append mode
        with nc.Dataset(filename, 'r+') as ds:
            # Check if 'r2m' variable exists
            if 'r2m' in ds.variables:
                # Add history attribute to the r2m variable
                ds.variables['r2m'].history = 'earthkit.meteo.thermo.array.thermo.relative_humidity_from_dewpoint(t2m, d2m)'
                print(f'✅ Added history attribute to {filename}')
            else:
                print(f'⚠️ Warning: No r2m variable found in {filename}')
    except Exception as e:
        print(f'❌ Error processing {filename}: {e}')
        return False
    
    return True

def main():
    if len(sys.argv) > 1:
        # Process specific directory or file
        target = sys.argv[1]
        if os.path.isdir(target):
            # If directory, process all .nc files
            files = glob.glob(os.path.join(target, '*.nc'))
        else:
            # If file, process just that file
            files = [target]
    else:
        # Default directory
        target_dir = '/home/petrichor/dataset/ERA5/derived/r2m'
        files = glob.glob(os.path.join(target_dir, '*.nc'))
    
    if not files:
        print(f'No NetCDF files found in {target_dir}')
        return
    
    print(f'Found {len(files)} NetCDF files to process')
    
    success_count = 0
    for file in files:
        success = add_history_attribute(file)
        if success:
            success_count += 1
    
    print(f'Processed {success_count} of {len(files)} files successfully')

if __name__ == '__main__':
    main()
