import csv
import threading
import time
import requests
import logging
import socket
from pathlib import Path
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from queue import Queue
from logging import StreamHandler
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor
from constants import (
    INPUT_CSV, LOG_FORMAT, LOG_FILE_PATH, ERROR_LOG_FILE_PATH,MAX_LOG_FILE_SIZE, BACKUP_COUNT, WORKER_COUNT, LOG_LEVEL, HEADERS, BASE_URL, SEARCH_ENDPOINT, STATUS_ENDPOINT, RESULTS_ENDPOINT, QUERY
)

requests.packages.urllib3.disable_warnings()

# === Shared Error Logger ===
error_handler = RotatingFileHandler(ERROR_LOG_FILE_PATH, maxBytes=MAX_LOG_FILE_SIZE, backupCount=BACKUP_COUNT)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(LOG_FORMAT))
error_logger = logging.getLogger("error")
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# === Shared Stream Logger ===
stream_handler = StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# === Dynamic Loggers by Range Name ===
loggers: Dict[str, logging.Logger] = {}

# === Shared HTTP Session ===
session = requests.Session()
session.headers.update(HEADERS)
session.verify = False

def get_range_logger(name: str) -> logging.Logger:
    if name not in loggers:
        logger = logging.getLogger(name)

        file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=MAX_LOG_FILE_SIZE, backupCount=BACKUP_COUNT)  
        file_handler.setLevel(logging.INFO)      
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

        log_queue = Queue()
        queue_handler = QueueHandler(log_queue)
        listener = QueueListener(log_queue, error_handler, file_handler)
        listener.start()

        logger.addHandler(queue_handler)
        logger.addHandler(stream_handler)   # Shared stream log
        logger.setLevel(LOG_LEVEL)
        logger.propagate = False
        loggers[name] = logger

    return loggers[name]

# === Job Progress Tracker ===
class JobProgressTracker:
    def __init__(self, total_jobs):
        self.total_jobs = total_jobs
        self.current_job = 0
        self.start_time = time.time()
        self.lock = threading.Lock()

    def next_job_number(self):
        with self.lock:
            self.current_job += 1
            return self.current_job

    def estimate_remaining(self):
        duration = time.time() - self.start_time
        avg_duration = duration / self.current_job
        remaining_jobs = self.total_jobs - self.current_job
        return timedelta(seconds=remaining_jobs * avg_duration)

# === Time Interval Generator ===
class TimeIntervalGenerator:
    def __init__(self, id, name, start, end, interval, port, prepend_name):
        self.logger = logging.getLogger(name)
        self.id = id
        self.name = name
        self.start = start
        self.end = end
        self.current = end  # Start from the END and move backward
        self.interval = timedelta(minutes=interval)
        self.port = port
        self.lock = threading.Lock()
        self.total_jobs = int(((end - start).total_seconds()) // (interval * 60))
        self.progress_tracker = JobProgressTracker(self.total_jobs)
        self.prepend_name = prepend_name

    def next_interval(self) -> Optional[Tuple[datetime, datetime, int, str, str, int, int, bool]]:
        with self.lock:
            if self.current <= self.start:
                return None  # Reached or passed the lower bound
            
            interval_end = self.current
            interval_start = max(self.start, interval_end - self.interval)
            self.current = interval_start  # Move pointer back for next call

            job_number = self.progress_tracker.next_job_number()

            if job_number % 5 == 0:
                self.logger.info(f"!!! Estimated time remaining: {self.progress_tracker.estimate_remaining()} !!!")

            return (
                interval_start,
                interval_end,
                job_number,
                self.id,
                self.name,
                self.total_jobs,
                self.port,
                self.prepend_name,
            )
        
# === Submit Search ===
def submit_search(id, start_time, stop_time):
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")
    url = BASE_URL + SEARCH_ENDPOINT
    query = QUERY.format(id=id, start_time=start_time_str, stop_time=stop_time_str)
    params = {"query_expression": query}

    response = session.post(url, params=params)
    response.raise_for_status()
    return response.json().get("search_id")

# === Get Search Status ===
def get_search_status(search_id):
    url = BASE_URL + STATUS_ENDPOINT.format(search_id=search_id)
    response = session.get(url)
    response.raise_for_status()
    res_json = response.json()
    return res_json.get("status"), res_json.get("progress"), res_json.get("record_count"), res_json.get("query_string")

# === Get Search Results ===
def get_search_results(search_id):
    url = BASE_URL + RESULTS_ENDPOINT.format(search_id=search_id)
    response = session.get(url)
    response.raise_for_status()
    return response.json().get("events")

# === Worker Function ===
def get_events(start_time: datetime, end_time: datetime, job_number: int, id: str, name: str, total_jobs: int, port: int, prepend_name: bool):
    logger = get_range_logger(name)
    log_prefix = f"Job {job_number} of {total_jobs} ({int(job_number/total_jobs * 100)}%)"
    logger.info(f"{log_prefix}: Searching '{name}' on interval {start_time} to {end_time}")
    
    try:
        search_id = submit_search(id, start_time, end_time)

        if not search_id:
            logger.error(f"{log_prefix}: No search ID returned")
            return

        while True:
            status, progress, record_count, query = get_search_status(search_id)

            if status == "COMPLETED":
                logger.info(f"{log_prefix}: Search completed with {record_count} events")
                
                if record_count == 0:
                    return
                
                events = get_search_results(search_id)

                if not events:
                    logger.error(f"Job {job_number}: No events returned")
                    return

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(('127.0.0.1', port))
                    if prepend_name:
                        payload = [f"{name} {event['payload']}" for event in events]
                    else:
                        payload = [event['payload'] for event in events]

                    sock.sendall("".join(payload).encode('utf-8'))
                break
            
            elif status == "ERROR":
                logger.error(f"{log_prefix}: Search failed with query {query}")
                break
            
            elif status == "CANCELED":
                logger.error(f"{log_prefix}: Search canceled with query {query}")
                break
            
            else:
                logger.info(f"{log_prefix}: Search in status {status} with progress {progress}, retrying in 5s")
                time.sleep(5)

    except requests.RequestException as e:
        logger.exception(f"{log_prefix}: Error during execution — Reason: {e}")

# === Worker Thread Loop ===
def round_robin_worker(generators, exhausted, exhausted_lock):
    while True:
        all_exhausted = True

        for i, generator in enumerate(generators):
            with exhausted_lock:
                if i in exhausted:
                    continue

            interval = generator.next_interval()
            if interval:
                get_events(*interval)
                all_exhausted = False
            else:
                with exhausted_lock:
                    exhausted.add(i)

        if all_exhausted:
            break

# === CSV Reader ===
def load_data_from_csv(csv_path: str):
    expected_format = "%Y-%m-%d %H:%M:%S"
    input = []
    csv_path = Path(csv_path)

    if csv_path.exists() and csv_path.is_file():
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    id = row['id']
                    name = row['name']
                    start = datetime.strptime(row['start_time'], expected_format)
                    end = datetime.strptime(row['end_time'], expected_format)
                    interval = int(row['interval_minutes'])
                    port = int(row['port'])
                    prepend_name = row['prepend_name'].lower() in ['t', 'true', '1', 'y', 'yes']

                    if end <= start:
                        raise ValueError(f"End time must be after start time in range: {name}")

                    input.append((id, name, start, end, interval, port, prepend_name))
                except (ValueError, KeyError) as e:
                    error_logger.error(f"Skipping invalid row in CSV: {row} — Reason: {e}")
    else:
        error_logger.error(f"Invalid CSV input path: {csv_path}")

    return input

# === Entry Point ===
def run_workers_from_csv(csv_file_path: str, worker_count: int):
    generators = [TimeIntervalGenerator(*args) for args in load_data_from_csv(csv_file_path)]

    if not generators:
        error_logger.error("No valid generators found.")
        return

    exhausted = set()
    exhausted_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for _ in range(worker_count):
            executor.submit(round_robin_worker, generators, exhausted, exhausted_lock)

# === Main ===
if __name__ == "__main__":
    run_workers_from_csv(INPUT_CSV, worker_count=WORKER_COUNT)
