# Changelog

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