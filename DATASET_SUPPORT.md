# Multi-Dataset Support in ERA5 Toolbox

The ERA5 Toolbox has been extended to support multiple datasets from the Copernicus Climate Data Store (CDS), not just ERA5 data. This document explains how to use the framework with different datasets.

## Supported Datasets

### 1. ERA5 Reanalysis - Single Levels
- **Dataset ID**: `reanalysis-era5-single-levels`
- **Description**: ERA5 atmospheric reanalysis data at surface and single levels
- **Example variables**: `2m_temperature`, `10m_u_component_of_wind`, `total_precipitation`
- **Filename format**: `era5.reanalysis.{variable}.1hr.0p25deg.global.{year}.nc`

### 2. ERA5 Reanalysis - Pressure Levels
- **Dataset ID**: `reanalysis-era5-pressure-levels`
- **Description**: ERA5 atmospheric reanalysis data at pressure levels
- **Example variables**: `u_component_of_wind`, `v_component_of_wind`, `temperature`
- **Requires**: Pressure level specification (e.g., 1000, 850, 500 hPa)
- **Filename format**: `era5.reanalysis.{variable}.{pressure_level}hpa.1hr.0p25deg.global.{year}.nc`

### 3. CEMS Fire Historical
- **Dataset ID**: `cems-fire-historical-v1`
- **Description**: CEMS Fire Historical dataset with fire weather indices
- **Example variables**: `fire_weather_index`
- **Filename format**: `cems.fire.{variable}.{year}.nc`

## Configuration Examples

### ERA5 Single Levels Example
```python
years = range(2000, 2002)
variables = ['2m_temperature', '10m_u_component_of_wind']
dataset = "reanalysis-era5-single-levels"
pressure_levels = None
short_names = {'2m_temperature': 't2m', '10m_u_component_of_wind': 'u10'}
custom_request = None
```

### ERA5 Pressure Levels Example
```python
years = range(1998, 2003)
variables = ['u_component_of_wind', 'v_component_of_wind']
dataset = "reanalysis-era5-pressure-levels"
pressure_levels = [1000, 850, 500]  # hPa
short_names = {'u_component_of_wind': 'u', 'v_component_of_wind': 'v'}
custom_request = None
```

### CEMS Fire Historical Example
```python
years = range(2000, 2001)
variables = ['fire_weather_index']
dataset = "cems-fire-historical-v1"
pressure_levels = None
short_names = {'fire_weather_index': 'fwi'}
custom_request = None
```

## Key Features

### 1. Dataset Configuration System
The framework uses a `DATASET_CONFIGS` dictionary that defines:
- Request templates for each dataset
- Filename templates
- Special requirements (e.g., pressure levels)

### 2. Custom Request Parameters
You can override default request parameters using the `custom_request` parameter:
```python
custom_request = {
    "grid": "1.0/1.0",  # Override grid resolution
    "area": [90, -180, -90, 180],  # Specify geographic area
}
```

### 3. Backward Compatibility
All existing ERA5 functionality is preserved. Old scripts will continue to work without modification.

### 4. Flexible Filename Generation
Filenames are automatically generated based on dataset-specific templates, ensuring consistent naming conventions.

## Usage Instructions

### Method 1: Modify the main script
Edit the configuration section in `downloader_ERA5.py`:
```python
# Choose your dataset configuration
years = range(2000, 2001)
variables = ['fire_weather_index']
dataset = "cems-fire-historical-v1"
pressure_levels = None
short_names = {'fire_weather_index': 'fwi'}
```

### Method 2: Use the example script
Run the provided example for CEMS Fire Historical data:
```bash
python example_cems_fire.py
```

### Method 3: Create your own script
Import the framework and use it programmatically:
```python
from downloader_ERA5 import download_with_client, load_api_keys
import cdsapi

# Load API keys
keys = load_api_keys()
client = cdsapi.Client(key=keys[0])

# Download data
download_with_client(
    client=client,
    year=2000,
    variable='fire_weather_index',
    dataset='cems-fire-historical-v1',
    short_name='fwi'
)
```

## Adding New Datasets

To add support for a new dataset, add an entry to the `DATASET_CONFIGS` dictionary:

```python
DATASET_CONFIGS["new-dataset-name"] = {
    "request_template": {
        "product_type": "reanalysis",
        # ... other required parameters
        "data_format": "netcdf"
    },
    "filename_template": "new.dataset.{variable}.{year}.nc",
    # Optional: "requires_pressure_level": True
}
```

## Important Notes

1. **API Keys**: Ensure your CDS API keys have access to the datasets you want to download
2. **Short Names**: Always provide short names when using `skip_existing=True`
3. **File Naming**: Different datasets use different filename conventions
4. **Request Parameters**: Each dataset may have different required and optional parameters

## Error Handling

The framework will:
- Validate dataset names against supported datasets
- Check for required parameters (e.g., pressure levels)
- Provide clear error messages for configuration issues
- Log all download attempts and results

## Performance

All performance optimizations from the original ERA5 toolbox are preserved:
- Multi-threaded downloading with multiple API keys
- Automatic retry mechanisms
- Resume capabilities for interrupted downloads
- Skip existing files functionality
