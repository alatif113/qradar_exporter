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
ENDPOINT1 = "https://api.example.com/submit"
ENDPOINT2 = "https://api.example.com/status"

# === Setup log directory ===
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(LOG_DIR, exist_ok=True)

# === Shared Error Logger ===
error_logger = logging.getLogger("errors")
error_logger.setLevel(logging.ERROR)
error_handler = RotatingFileHandler(os.path.join(LOG_DIR, "errors.log"), maxBytes=5 * 1024 * 1024, backupCount=10)
error_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(threadName)s] %(name)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
error_logger.addHandler(error_handler)
error_logger.propagate = False

# === Dynamic Loggers by Range Name ===
loggers: Dict[str, logging.Logger] = {}

def get_range_logger(devicetype_name: str) -> logging.Logger:
    if devicetype_name not in loggers:
        logger = logging.getLogger(devicetype_name)
        logger.setLevel(logging.INFO)

        file_handler = RotatingFileHandler(os.path.join(LOG_DIR, f"{range_name}.log"), maxBytes=5 * 1024 * 1024, backupCount=10)        
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        logger.addHandler(file_handler)
        logger.addHandler(error_handler)  # Shared error log
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

# === Worker Function ===
def func(start: datetime, end: datetime, job_number: int, devicetype_id: str, devicetype_name: str, total_jobs: int):
    logger = get_range_logger(devicetype_name)
    logger.info(f"Starting job {job_number} of {total_jobs} for '{devicetype_name}' on interval {start} to {end}")

    try:
        response = requests.post(ENDPOINT1, json={"startTime": start.isoformat(), "endTime": end.isoformat(), "rangeId": devicetype_id})
        response.raise_for_status()
        job_id = response.json().get("id")

        if not job_id:
            logger.error(f"Job {job_number} [{start} - {end}]: No job ID returned")
            return

        logger.info(f"Job {job_number} [{start} - {end}]: Submitted with ID {job_id}")

        while True:
            poll_response = requests.get(ENDPOINT2, params={"id": job_id})
            poll_response.raise_for_status()
            status_json = poll_response.json()
            status = status_json.get("status")

            if status == "Complete":
                logger.info(f"Job {job_number} [{start} - {end}]: Job {job_id} complete")
                filename = f"{devicetype_name}_job{job_id}_{start.isoformat()}_{end.isoformat()}.json".replace(":", "-")
                with open(filename, "w") as f:
                    f.write(poll_response.text)
                logger.info(f"Job {job_number} [{start} - {end}]: Results written to {filename}")
                break
            elif status == "Failed":
                logger.warning(f"Job {job_number} [{start} - {end}]: Job {job_id} failed")
                break
            else:
                logger.info(f"Job {job_number} [{start} - {end}]: Status = {status}, retrying in 5s")
                time.sleep(5)

    except requests.RequestException:
        logger.exception(f"Job {job_number} [{start} - {end}]: Error during execution")

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
                error_logger.error(f"Skipping invalid row in CSV: {row} — Reason: {e}")
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
