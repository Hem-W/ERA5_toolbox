# ERA5 Data Downloader

A tool for downloading ERA5 climate data from the Copernicus Climate Data Store (CDS) using multiple API keys concurrently to improve download speeds.

## Setup

1. Install the required dependencies:
   ```
   pip install cdsapi
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
   
   **Important:** JSON doesn't support comments. Make sure your JSON file contains only valid JSON syntax without any comments.
   
   You can obtain CDS API keys by registering at https://cds.climate.copernicus.eu/

3. Make sure the `cdsapi_keys.json` file is in the same directory as the script, or specify a different location in the `load_api_keys()` function call.

## Usage

To download ERA5 data, simply run the script:

```
python download_ERA5_multiAPI.py
```

By default, the script downloads the `100m_v_component_of_wind` variable for years 1940-2024. You can modify these parameters in the script.

## Features

- Uses multiple API keys in parallel to speed up downloads
- Distributes download tasks efficiently across keys
- Skips already downloaded files
- Implements proper error handling and logging
- Each API key runs two concurrent downloads for optimal performance

## Security

The script loads API keys from a separate JSON file, which:
- Keeps sensitive credentials out of source code
- Makes it easier to maintain and update keys
- Prevents accidental exposure of keys in version control systems

Do not commit your `cdsapi_keys.json` file containing real API keys to version control. Add it to your `.gitignore` file. 