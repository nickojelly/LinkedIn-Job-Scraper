from scripts.create_db import create_tables
from scripts.database_scripts import insert_job_postings
from scripts.fetch import JobSearchRetriever
import sqlite3
import time
from collections import deque
import random

# Initialize database connection
conn = sqlite3.connect('linkedin_jobs.db')
cursor = conn.cursor()

# Create tables if they don't exist
create_tables(conn, cursor)

# Initialize JobSearchRetriever
job_searcher = JobSearchRetriever(batch_size=1_000, num_threads=20, sessions_per_thread=75)

# Initialize variables for adaptive sleep
sleep_times = deque(maxlen=5)
first_run = True
sleep_factor = 3
start_id = 3961155832 # Adjust this starting ID as needed
start_id = 3966613013

while True:
    jobs, jobs_searched, rate_limit_hit = next(job_searcher.continuous_job_search(start_id))
    print(f"Jobs searched: {jobs_searched}, rate limit hit: {rate_limit_hit}, jobs: {len(jobs)}")

    if jobs:
        # Check which jobs are new
        job_ids = list(jobs.keys())
        placeholders = ','.join(['?' for _ in job_ids])
        query = f"SELECT job_id FROM jobs WHERE job_id IN ({placeholders})"
        cursor.execute(query, job_ids)
        existing_jobs = set(row[0] for row in cursor.fetchall())
        
        new_jobs = {job_id: job_info for job_id, job_info in jobs.items() if job_id not in existing_jobs}

        # Insert new jobs into the database
        insert_job_postings(new_jobs, conn, cursor)

        # Calculate statistics
        total_non_sponsored = len([job for job in jobs.values() if not job['sponsored']])
        new_non_sponsored = len([job for job in new_jobs.values() if not job['sponsored']])

        print(f'{len(new_jobs)}/{len(jobs)} NEW RESULTS | {new_non_sponsored}/{total_non_sponsored} NEW NON-PROMOTED RESULTS')
    else:
        print("No valid jobs found in this batch.")

    print(f'Searched {jobs_searched} job IDs')

    # Adjust sleep time
    if not first_run:
        seconds_per_job = sleep_factor / max(len(new_jobs), 1)
        sleep_factor = min(seconds_per_job * total_non_sponsored * 0.75, 200)
    first_run = False

    # Handle rate limiting
    if rate_limit_hit:
        print(f'Rate limit hit. Sleeping for {job_searcher.rate_limit_delay} seconds...')
        time.sleep(10)
    else:
        sleep_time = min(200, sleep_factor)
        jitter = random.uniform(-5, 5)  # Add some randomness to the sleep time
        sleep_time = max(1, sleep_time + jitter)  # Ensure sleep time is at least 1 second
        print(f'Sleeping for {sleep_time:.2f} seconds...')
        time.sleep(sleep_time)

    # Prepare for next iteration
    start_id += job_searcher.batch_size
    print(f'Resuming with start_id: {start_id}')
    # break

# Close the database connection when done
conn.close()
