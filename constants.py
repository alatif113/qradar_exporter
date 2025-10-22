# === constants.py (fully annotated with constant descriptions) ===

from pathlib import Path
import argparse

# These CLI arguments materially change how qradar_exporter.py behaves
parser = argparse.ArgumentParser(description="Configuration")
parser.add_argument("input_csv", type=str, help="Path to the input CSV file.")  # <-- REQUIRED: specify CSV input; user changes to point to different datasets
parser.add_argument("--worker-count", type=int, default=4, help="Number of worker threads to use (default: 4).")  # <-- user can increase/decrease concurrency
parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level (default: INFO).")  # <-- user can control verbosity at runtime
args = parser.parse_args()

# === DERIVED PATHS ===
BASE_DIR = Path(__file__).resolve().parent  # Base folder where constants.py resides
LOG_DIR = BASE_DIR / "log"                   # Folder for log files, user can change path if desired
EXPORTS_DIR = BASE_DIR / "exports"           # Folder where exported events are stored, changeable by user if needed
INPUT_CSV = args.input_csv                     # Input CSV path provided by user via CLI

# === Logging Constants ===
LOG_FORMAT = "%(asctime)s [%(threadName)s] %(name)s %(levelname)s: %(message)s"  # Format for all logs, user can adjust formatting
ERROR_LOG_FILE_PATH = LOG_DIR / "errors.log"  # File to store error logs, user can change filename or directory

# === Log Rotation Settings ===
MAX_LOG_FILE_SIZE = 5 * 1024 * 1024  # Maximum size (bytes) for each log file before rotation; user can increase/decrease
BACKUP_COUNT = 10                    # Number of rotated backups to keep; user can adjust to save more/less history

# === HTTP/Network Constants ===
BASE_URL = "https://siem.mgroupnet.com"  # Base URL of the QRadar instance; user must modify if connecting to another system
SEARCH_ENDPOINT = "/api/ariel/searches"  # QRadar API endpoint for submitting searches
STATUS_ENDPOINT = "/api/ariel/searches/{search_id}"  # Endpoint to check search status; usually does not need user change
RESULTS_ENDPOINT = "/api/ariel/searches/{search_id}/results"  # Endpoint to fetch search results
QUERY = "SELECT UTF8(payload) as payload from events where devicetype = {id} START '{start_time}' STOP '{stop_time}'"  # Query template; advanced users can modify to change data selection
SEC_TOKEN = ""  # Security token for QRadar; user must set their own token
HEADERS = {
    'SEC': SEC_TOKEN,                       
    'Content-Type': 'application/json',    
    'Accept': 'application/json'            
}
CHUNK_SIZE = 50000  # Number of events sent per batch;

# === Worker Constants ===
WORKER_COUNT = args.worker_count  # Number of concurrent worker threads, changeable via CLI
LOG_LEVEL = args.log_level        # Logging verbosity level, changeable via CLI
