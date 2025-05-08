from pathlib import Path
import argparse

# === Setup ArgumentParser for Command-Line Args ===
parser = argparse.ArgumentParser(description="Configuration")
parser.add_argument("input_csv", type=str, help="Path to the input CSV file.")
parser.add_argument("--worker-count", type=int, default=4, help="Number of worker threads to use (default: 4).")
parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level (default: INFO).")
args = parser.parse_args()

# === Directory Paths ===
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "log"
EXPORTS_DIR = BASE_DIR / "exports"

# === Logging Constants ===
LOG_FORMAT = "%(asctime)s [%(threadName)s] %(name)s %(levelname)s: %(message)s"
LOG_FILE_PATH = LOG_DIR / "logfile.log"
ERROR_LOG_FILE_PATH = LOG_DIR / "errors.log"

# === Log Rotation Settings ===
MAX_LOG_FILE_SIZE = 5 * 1024 * 1024  # 5MB
BACKUP_COUNT = 10

# === HTTP/Network Constants ===
BASE_URL = "https://siem.mgroupnet.com"
SEARCH_ENDPOINT = "/api/ariel/searches"
STATUS_ENDPOINT = "/api/ariel/searches/{search_id}"
RESULTS_ENDPOINT = "/api/ariel/searches/{search_id}/results"
#QUERY = "SELECT UTF8(payload) as payload from events where devicetype = {id} START '{start_time}' STOP '{stop_time}'"
QUERY = "SELECT UTF8(payload) as payload from events WHERE logsourceid = {id} START '{start_time}' STOP '{stop_time}'"
SEC_TOKEN = ""
HEADERS = {
    'SEC': SEC_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# === Worker Constants ===
WORKER_COUNT = args.worker_count
LOG_LEVEL = args.log_level