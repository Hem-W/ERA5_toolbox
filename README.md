# ERA5 Data Downloader

A tool for downloading ERA5 climate data from the Copernicus Climate Data Store (CDS) using multiple API keys concurrently to improve download speeds.

## Installation

1. Clone or download this repository:
   ```bash
   git clone https://github.com/Hem-W/ERA5_toolbox.git
   cd ERA5_toolbox
   ```

### Installation Method 1: Using Conda (Recommended)

2. Create a conda environment using the provided environment.yml file:
   ```bash
   conda env create -f environment.yml
   conda activate era5_toolbox
   ```

### Installation Method 2: Manual Installation

2. Install the required dependencies manually:
   ```bash
   pip install cdsapi json5 tqdm urllib3 netcdf4 xarray
   ```

## Configuration

3. Configure your API keys by creating or modifying the `cdsapi_keys.json` file:
   ```json5
   {
       "keys": [
           "your-first-api-key",
           "your-second-api-key",
           "your-third-api-key"
       ]
   }
   ```
   
   You can obtain CDS API keys by registering at https://cds.climate.copernicus.eu/

4. Make sure the `cdsapi_keys.json` file is in the same directory as the script, or specify a different location in the `api_keys_file` parameter.

## Usage

To download ERA5 data, modify the user specifications in the main section of `downloader_ERA5.py` and run:

```
python -u downloader_ERA5.py
```

### Configuration Parameters

Edit these parameters in the main section of the script:

```python
# User Specification
years = range(2019, 2025)
variables = ["10m_u_component_of_wind", "2m_temperature"]
dataset = "reanalysis-era5-single-levels"
pressure_levels = None  # List of pressure levels (hPa)
api_keys_file = None  # Use default 'cdsapi_keys.json'
workers_per_key = 2  # Number of concurrent downloads per API key
skip_existing = True  # Whether to skip downloading existing files

# Optional: Provide short names for variables (recommended when skip_existing=True)
short_names = {
    '10m_u_component_of_wind': 'u10', 
    '2m_temperature': 't2m'
}
```

### Single-Level Data Example

To download single-level ERA5 data:

```python
years = range(1940, 2025)
variables = ["toa_incident_solar_radiation", "2m_temperature", "total_precipitation"]
dataset = "reanalysis-era5-single-levels"
# Define short names for better file naming and skipping existing files
short_names = {
    "toa_incident_solar_radiation": "tisr", 
    "2m_temperature": "t2m", 
    "total_precipitation": "tp"
}
```

### Pressure-Level Data Example

To download pressure-level ERA5 data:

```python
years = range(1940, 2025)
variables = ["geopotential", "u_component_of_wind", "v_component_of_wind"]
dataset = "reanalysis-era5-pressure-levels"
pressure_levels = ["500", "700"]  # Pressure levels in hPa
short_names = {
    "geopotential": "z", 
    "u_component_of_wind": "u", 
    "v_component_of_wind": "v"
}
```

## Features

- Dynamic task assignment system that automatically balances workload among multiple API keys
  - Each API key can run multiple worker threads simultaneously
  - Dynamically assigns API keys to tasks based on progress
- Robust download mechanism:
  - Automatic fallback download if the CDS API method fails
  - Exponential backoff retry strategy for failed downloads
  - Resume capability for interrupted downloads
- Smart file handling:
  - Optional automatic variable code extraction from NetCDF files
  - Skip existing files when provided with short names
  - File naming based on actual variable codes
- Supports both ERA5 single-level and pressure-level datasets

## Output Files

Output files are named with a standardized convention:
- Single-level: `era5.reanalysis.[variable_shortname].1hr.0p25deg.global.[year].nc`
- Pressure-level: `era5.reanalysis.[variable_shortname].[pressure_level]hpa.1hr.0p25deg.global.[year].nc`

The variable short name can be:
1. Provided by the user via the `short_names` dictionary (recommended)
2. Automatically extracted from the downloaded NetCDF file (when `short_name` is not provided)

## Security

The script loads API keys from a separate JSON file, which:
- Keeps sensitive credentials out of source code
- Makes it easier to maintain and update keys