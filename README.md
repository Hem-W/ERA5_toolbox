# ERA5 Data Downloader

A tool for downloading ERA5 climate data from the Copernicus Climate Data Store (CDS) using multiple API keys concurrently to improve download speeds.

## Features

- Dynamic task assignment system that automatically balances workload among multiple API keys
  - Pre-flight API key validation with automatic removal of invalid keys
  - Configurable concurrent request threads and parallel download threads per key
  - Keeps the CDS server queue occupied while downloads proceed concurrently
- Robust download mechanism:
  - Automatic fallback download if the CDS API method fails
  - Exponential backoff retry strategy for failed downloads
  - Resumable for interrupted downloads
- Automatic file name handling:
  - Optional automatic variable short name extraction from NetCDF files
  - Skip existing files when provided with short names
- Supports both ERA5 single-level and pressure-level datasets

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

### Option 1: YAML Configuration File (Recommended)

Create a YAML configuration file based on `template_request.yaml` and run:

```bash
python -u downloader_ERA5.py --file my_config.yaml
```

Or run in the background:

```bash
nohup python -u downloader_ERA5.py --file my_config.yaml &
```

See `template_request.yaml` for a commented example with all available parameters.

### Option 2: Hardcoded in Script

If no `--file` is provided, the script uses the hardcoded configuration in the main section of `downloader_ERA5.py`:

```bash
python -u downloader_ERA5.py
```

### Configuration Parameters

The following parameters can be set via the YAML file or by editing the hardcoded configuration in the script:

```python
# User Specification
years = range(2019, 2025)
variables = ["10m_u_component_of_wind", "2m_temperature"]
dataset = "reanalysis-era5-single-levels"
pressure_levels = None  # List of pressure levels (hPa)
api_keys_file = None  # Use default 'cdsapi_keys.json'
concurrent_requests = 4  # Number of concurrent request threads per key
download_workers = 1  # Number of parallel download threads per key
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

## Output File Naming Pattern

Output files are named with a prescribed pattern:
- Single-level: `era5.reanalysis.[variable_shortname].1hr.0p25deg.global.[year].nc`
- Pressure-level: `era5.reanalysis.[variable_shortname].[pressure_level]hpa.1hr.0p25deg.global.[year].nc`

The variable short name can be:
1. Provided by the user via the `short_names` dictionary (recommended)
2. Automatically extracted from the downloaded NetCDF file (when `short_name` is not provided)

## API Key Security

The script loads API keys from a separate JSON file, which:
- Keeps sensitive credentials out of source code
- Makes it easier to maintain and update keys

## Experimental Helper Utilities

- Resampling ERA5 data: `utils/resampler_ERA5.py`
- Calculate relative humidity from temperature and specific humidity: `utils/humid-helper_ERA5.py`