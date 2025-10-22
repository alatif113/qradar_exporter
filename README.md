# QRadar Exporter Scripts README

## Overview
These Python scripts allow you to export event logs from a QRadar SIEM instance using the Ariel API, split by configurable time intervals, and send them to a local TCP port. The solution is multi-threaded and supports logging, error handling, and retry mechanisms.

### Files
- **constants.py**: Defines all runtime configuration, constants, logging settings, QRadar API endpoints, and CLI argument parsing.
- **qradar_exporter.py**: Main worker script that reads a CSV file of device ranges, submits searches to QRadar, polls for completion, fetches results, and sends them to a TCP port.

## Requirements
- Python 3.8+
- Packages: `requests`
- Network access to the QRadar API endpoint
- TCP listener on specified ports to receive exported events

## Configuration
Most user-configurable fields are in `constants.py` or provided via CLI arguments.

### CLI Arguments
```bash
python qradar_exporter.py <input_csv> [--worker-count N] [--log-level LEVEL]
```
- `input_csv`: Path to CSV input file containing device/time ranges
- `--worker-count`: Number of concurrent threads (default 4)
- `--log-level`: Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL; default INFO)

### Constants (constants.py)
- **BASE_DIR**: Directory where scripts reside
- **LOG_DIR**: Directory for log files
- **EXPORTS_DIR**: Directory for exported events
- **LOG_FORMAT**: Log message format
- **ERROR_LOG_FILE_PATH**: Path for error logs
- **MAX_LOG_FILE_SIZE**: Max log file size in bytes before rotation
- **BACKUP_COUNT**: Number of rotated log backups to keep
- **BASE_URL**: QRadar server URL
- **SEARCH_ENDPOINT / STATUS_ENDPOINT / RESULTS_ENDPOINT**: QRadar API endpoints
- **QUERY**: Template for Ariel query; can be modified to change fields or conditions
- **SEC_TOKEN**: QRadar API security token
- **HEADERS**: HTTP headers for requests
- **CHUNK_SIZE**: Number of events fetched per batch
- **WORKER_COUNT**: Number of concurrent threads
- **LOG_LEVEL**: Runtime logging level

## CSV Input Format
The CSV file should have the following columns:
- `id`: Device type or identifier for the search
- `name`: Unique name for logging and payload tagging
- `start_time`: Interval start (format `YYYY-MM-DD HH:MM:SS`)
- `end_time`: Interval end (format `YYYY-MM-DD HH:MM:SS`)
- `interval_minutes`: Size of each interval in minutes
- `port`: Local TCP port to send events
- `prepend_name`: Flag (`true`/`false`) to prepend device name to payload

## Usage Example
```bash
python qradar_exporter.py devices.csv --worker-count 6 --log-level DEBUG
```

## Logging
- Rotating file logs stored in `LOG_DIR` per device range
- Errors also logged in `errors.log`
- Stream handler logs INFO+ messages to stdout

## Multi-threading
- Uses `ThreadPoolExecutor` to process multiple devices in parallel
- `JobProgressTracker` estimates remaining time
- `TimeIntervalGenerator` splits ranges into intervals processed in reverse chronological order

## Error Handling
- HTTP exceptions are caught and logged
- Invalid CSV rows are skipped and logged
- Search failures or cancellations are logged and do not stop other threads

## TCP Export
- Event payloads are sent to the TCP listener on the specified port
- Optional device name prepended if `prepend_name` is true
- Uses blocking `socket.sendall` for each batch

## Notes
- Ensure QRadar SEC_TOKEN is set before running
- Adjust `CHUNK_SIZE` and `WORKER_COUNT` for performance tuning
- Directories (`LOG_DIR`, `EXPORTS_DIR`) are auto-resolved relative to script location
