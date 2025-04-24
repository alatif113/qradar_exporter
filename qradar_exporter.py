import csv
import threading
import time
import requests
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict

# === Constants ===
SEC_TOKEN = ""
BASE_URL = "https://siem.mgroupnet.com"
SEARCH_ENDPOINT = "/api/ariel/searches"
STATUS_ENDPOINT = "/api/ariel/searches/{search_id}"
RESULTS_ENDPOINT = "/api/ariel/searches/{search_id}/results"
QUERY = "SELECT UTF8(payload) as payload from events where devicetype = {devicetype_id} START '{start_time}' STOP '{stop_time}'"
HEADERS = {
        'SEC': SEC_TOKEN,
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }

# === Setup log directory ===
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)

# === Setup exports directory ===
EXPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

# === Shared Error Logger ===
error_handler = RotatingFileHandler(os.path.join(LOG_DIR, "errors.log"), maxBytes=5 * 1024 * 1024, backupCount=10)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(threadName)s] %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
error_logger = logging.getLogger("error")
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# === Dynamic Loggers by Range Name ===
loggers: Dict[str, logging.Logger] = {}

def get_range_logger(devicetype_name: str) -> logging.Logger:
    if devicetype_name not in loggers:
        logger = logging.getLogger(devicetype_name)

        file_handler = RotatingFileHandler(os.path.join(LOG_DIR, f"{devicetype_name}.log"), maxBytes=5 * 1024 * 1024, backupCount=10)  
        file_handler.setLevel(logging.INFO)      
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        logger.addHandler(file_handler)
        logger.addHandler(error_handler)  # Shared error log
        logger.setLevel(logging.INFO)
        logger.propagate = False
        loggers[devicetype_name] = logger

    return loggers[devicetype_name]

# === Job Progress Tracker ===
class JobProgressTracker:
    def __init__(self, total_jobs):
        self.total_jobs = total_jobs
        self.current_job = 0
        self.lock = threading.Lock()

    def next_job_number(self):
        with self.lock:
            self.current_job += 1
            return self.current_job

# === Time Interval Generator ===
class TimeIntervalGenerator:
    def __init__(self, devicetype_id, devicetype_name, start_time, end_time, interval_minutes):
        self.devicetype_id = devicetype_id
        self.devicetype_name = devicetype_name
        self.current = start_time
        self.end = end_time
        self.interval = timedelta(minutes=interval_minutes)
        self.lock = threading.Lock()
        self.total_jobs = int(((end_time - start_time).total_seconds()) // (interval_minutes * 60))
        self.progress_tracker = JobProgressTracker(self.total_jobs)

    def next_interval(self) -> Optional[Tuple[datetime, datetime, int, str, str, int]]:
        with self.lock:
            if self.current >= self.end:
                return None
            start = self.current
            end = min(start + self.interval, self.end)
            self.current = end
            job_number = self.progress_tracker.next_job_number()
            return (start, end, job_number, self.devicetype_id, self.devicetype_name, self.total_jobs)
        
# === Submit Search ===
def submit_search(devicetype_id, start_time, stop_time):
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")
    url = BASE_URL + SEARCH_ENDPOINT
    query = QUERY.format(devicetype_id=devicetype_id, start_time=start_time_str, stop_time=stop_time_str)
    params = {"query_expression": query}

    response = requests.post(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json().get("search_id")

def get_search_status(search_id):
    url = BASE_URL + STATUS_ENDPOINT.format(search_id=search_id)

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    res_json = response.json()
    return res_json.get("status"), res_json.get("progress"), res_json.get("record_count"), res_json.get("data_total_size"), res_json.get("query_string")

def get_search_results(search_id):
    url = BASE_URL + RESULTS_ENDPOINT.format(search_id=search_id)

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("events")

# === Worker Function ===
def func(start_time: datetime, end_time: datetime, job_number: int, devicetype_id: str, devicetype_name: str, total_jobs: int):
    logger = get_range_logger(devicetype_name)
    logger.info(f"Starting job {job_number} of {total_jobs} for '{devicetype_name}' on interval {start_time} to {end_time}")

    try:
        search_id = submit_search(devicetype_id, start_time, end_time)

        if not search_id:
            logger.error(f"Job {job_number}: No search ID returned")
            return

        logger.info(f"Job {job_number}: Search submitted with ID {search_id}")

        while True:
            status, progress, record_count, size, query = get_search_status(search_id)

            if status == "COMPLETED":
                logger.info(f"Job {job_number}: Search completed with {record_count} events and {size} bytes")
                events = get_search_results(search_id)

                if not events:
                    logger.error(f"Job {job_number}: No events returned")
                    return

                filename = f"{devicetype_name}_{search_id}.log".replace(":", "-")
                filepath = os.path.join(EXPORTS_DIR, filename)
                with open(filepath, "w") as f:
                    for event in events:
                        f.write(event["payload"] + "\n")
                logger.info(f"Job {job_number}: Results written to {filename}")
                break
            elif status == "ERROR":
                logger.error(f"Job {job_number}: Search failed with query {query}")
                break
            elif status == "CANCELED":
                logger.error(f"Job {job_number}: Search canceled with query {query}")
                break
            else:
                logger.info(f"Job {job_number}: Search in status {status} with progress {progress}, retrying in 10s")
                time.sleep(10)

    except requests.RequestException as e:
        logger.exception(f"Job {job_number}: Error during execution — Reason: {e}")

# === Worker Thread Loop ===
def worker(generators: List[TimeIntervalGenerator]):
    while True:
        for generator in generators:
            interval = generator.next_interval()
            if interval:
                func(*interval)
                break
        else:
            return  # No more intervals in any generator

# === CSV Reader ===
def load_ranges_from_csv(csv_path: str):
    expected_format = "%Y-%m-%d %H:%M:%S"
    ranges = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                devicetype_id = row['id']
                name = row['name']
                start = datetime.strptime(row['startTime'], expected_format)
                end = datetime.strptime(row['endTime'], expected_format)
                interval = int(row['timeInterval'])

                if end <= start:
                    raise ValueError(f"End time must be after start time in range: {name}")

                ranges.append((devicetype_id, name, start, end, interval))
            except (ValueError, KeyError) as e:
                logger.error(f"Skipping invalid row in CSV: {row} — Reason: {e}")
    return ranges

# === Entry Point ===
def run_workers_from_csv(csv_file_path: str, worker_count: int):
    ranges = load_ranges_from_csv(csv_file_path)
    generators = [TimeIntervalGenerator(devicetype_id, name, start, end, interval) for devicetype_id, name, start, end, interval in ranges]

    threads = []
    for i in range(worker_count):
        t = threading.Thread(target=worker, args=(generators,), name=f"Worker-{i+1}")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

# === Main ===
if __name__ == "__main__":
    run_workers_from_csv("input.csv", worker_count=4)
