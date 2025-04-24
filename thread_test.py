import threading
from typing import Optional, List, Tuple, Dict

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
    def __init__(self, id):
        self.id = id
        self.lock = threading.Lock()
        self.total_jobs = 5
        self.interval = 1
        self.progress_tracker = JobProgressTracker(self.total_jobs)

    def next_interval(self):
        with self.lock:
            if self.current >= self.end:
                return None
            start = self.current
            end = min(start + self.interval, self.end)
            self.current = end
            job_number = self.progress_tracker.next_job_number()
            return (start, end, job_number, self.id, self.total_jobs)
        
def func(self, start, end, job_number, id, total_jobs):
    print(start, end, job_number, id, total_jobs)
        
def worker(generators: List[TimeIntervalGenerator]):
    while True:
        for generator in generators:
            interval = generator.next_interval()
            if interval:
                func(*interval)
                break
        else:
            return  # No more intervals in any generator

def run_workers():
    ranges = [1, 2, 3]
    generators = [TimeIntervalGenerator() for id in ranges]

    threads = []
    for i in range(4):
        t = threading.Thread(target=worker, args=(generators,), name=f"Worker-{i+1}")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    run_workers()