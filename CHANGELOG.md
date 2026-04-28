# Changelog

## [0.4.5.dev] - 20260428
### Added
- `RequestTask` and `DownloadTask` dataclasses now carry queue items and results, replacing long positional tuples

### Changed
- Request threads shut down via explicit `None` sentinels instead of a racy `timeout` + `queue.empty()` probe
- Shared task queue switched from `multiprocessing.Manager().Queue()` to a plain `queue.Queue` (all workers are threads in the parent process)
- Final elapsed-time log now includes seconds (`DdHHhMMmSSs` instead of `DdHHhMMm`)

### Fixed
- File handle is now properly released on every exit path in `get_variable_code_from_netcdf`

### Removed
- Unused `dataset` parameter from `default_folder_pattern()`
- Dead `task is None` sentinel branch in the old `key_request_thread` loop

## [0.4.3] - 20260424
### Added
- Configurable output path layout and file name pattern via `folder_pattern` and `name_pattern` YAML options, supporting `{short_name}`, `{variable}`, `{year}`, `{pressure_level}`, and `{dataset}` placeholders

## [0.4.2] - 20260421
### Added
- Per-task summary report (`ReportCollector`) printed at the end

## [0.4.1] - 20260419
### Fixed
- Final elapsed-time log now formats as `DdHHhMMm` instead of a `HH:MM:SS` string that wrapped past 24 hours

## [0.4.0.dev] - 20260406
### Added
- YAML configuration file support with a `--file` / `-f` CLI flag and an `argparse` interface
- `template_request.yaml` with commented defaults for every option

## [0.3.2] - 20260406
### Added
- Pre-flight CDS API key validation (`validate_key` / `filter_valid_keys`)

### Fixed
- Logging is now configured inside the `__main__` guard so `multiprocessing.Manager` child processes no longer reinitialise it in macOS

## [0.3.1.dev] - 20260330
### Changed
- `concurrent_requests` per API key for multiple request threads
- Supervisor thread that waits for all request threads to finish before sending the sentinels that shut down the download pool

## [0.3.0.dev] - 20260326
### Added
- Extracted `submit_request` and `perform_download` as independent callables; the request thread returns a self-contained `result` object that the download thread consumes

## [0.2.2.dev] - 20250603
### Changed
- Removed the deprecated hardcoded `VB_MAP` lookup table (short names now come from the user's `short_names` mapping or are auto-detected from the NetCDF file)
- Renamed the logger from `ERA5_downloader` to `ERA5_toolbox.downloader_ERA5` to match the package hierarchy

## [0.2.1] - 20250528
### Fixed
- Fixed the logic of using urllib3 to download failed tasks from cdsapi

## [0.2.0] - 20250524
### Changed
- Improved variable code extraction using xarray instead of netCDF4, handling auxiliary variables like 'number' and 'expver'
- Get variable short name recognition for automatically naming files when short_names is not provided (`short_names=None`)
- Only skip existing files (`skip_existing=True`) when short_names is provided

### Added
- Added xarray as a dependency for better NetCDF file handling

## [0.1.5.dev] - 20250521
### Added
- Support multiple pressure levels for ERA5 pressure levels dataset

## [0.1.4] - 20250430
### Added
- Support for downloading multiple variables in a single request

## [0.1.3] - 20250428
### Changed
- Enhanced error logging and simplified worker format in logging

## [0.1.2.dev] - 20250425
### Changed
- JSON5 support for API keys configuration allowing comments in JSON
- Improved handling of incomplete downloads when error occurs

## [0.1.1.dev] - 20250424
### Changed
- More precise logging when falling back to urllib3
- Improved retry logic with exponential backoff

## [0.1.0] - 20250422
### Added
- Dynamic task assignment that efficiently distributes tasks across API keys
- Enhanced logging with detailed worker status information

### Changed
- Reused CDS client for each worker to reduce overhead
- Direct input of API keys to workers instead of using .cdsapirc configuration

## [0.0.2.dev] - 20250421
### Added
- Support for ERA5 pressure levels dataset
- Added pressure_level parameter for specifying atmospheric levels
- Adaptive file naming for pressure level data

## [0.0.1] - 20250419
### Added
- Initial version with basic ERA5 download functionality
- Support for multiple API keys and parallel downloads
- Fallback download mechanism with urllib3