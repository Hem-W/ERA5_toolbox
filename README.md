# ERA5 Data Downloader

A tool for downloading ERA5 climate data from the Copernicus Climate Data Store (CDS) using multiple API keys concurrently to improve download speeds.

## Setup

1. Install the required dependencies:
   ```
   pip install cdsapi json5 tqdm
   ```

2. Configure your API keys:
   Create or modify the `cdsapi_keys.json` file with your CDS API keys:
   ```json
   {
       "keys": [
           "your-first-api-key",
           "your-second-api-key",
           "your-third-api-key"
       ]
   }
   ```
   
   You can obtain CDS API keys by registering at https://cds.climate.copernicus.eu/

3. Make sure the `cdsapi_keys.json` file is in the same directory as the script, or specify a different location in the `load_api_keys()` function call.

## Usage

To download ERA5 data, simply run the script:

```
python -u downloader_ERA5.py
```

### Single-Level Data

To download single-level ERA5 data, modify the main section:

```python
if __name__ == '__main__':
    years = range(1940, 2025)
    var = "toa_incident_solar_radiation"  # or any other single-level variable
    dataset = "reanalysis-era5-single-levels"
    pressure_level = None
    # ...
```

### Pressure-Level Data

To download pressure-level ERA5 data, configure the parameters like this:

```python
if __name__ == '__main__':
    years = range(1940, 2025)
    var = "geopotential"  # or any other pressure-level variable
    dataset = "reanalysis-era5-pressure-levels"
    pressure_level = "500"  # Pressure level in hPa
    # ...
```

## Features

- Dynamic task assignment system that automatically balances workload between API keys
  - Faster keys will process more tasks, maximizing overall throughput
- Each API key maintains two concurrent downloads for optimal performance
- Reuses CDS client for each worker, reducing overhead and improving efficiency
- Skips already downloaded files
- Supports both ERA5 single-level and pressure-level datasets
- Resilient downloading with automatic retries and resuming of partial downloads
- Enhanced progress tracking with detailed logging

## Security

The script loads API keys from a separate JSON file, which:
- Keeps sensitive credentials out of source code
- Makes it easier to maintain and update keys