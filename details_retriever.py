from scripts.create_db import create_tables
from scripts.database_scripts import insert_data
from scripts.fetch import JobDetailRetriever
import sqlite3
from scripts.helpers import clean_job_postings
import time
import random
import timeit

SLEEP_TIME = 60
MAX_UPDATES = 3000

conn = sqlite3.connect('linkedin_jobs.db')
cursor = conn.cursor()

create_tables(conn, cursor)

job_detail_retriever = JobDetailRetriever(num_threads=12)

def get_scraping_progress():
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total_jobs = cursor.fetchone()[0]
    print(f"Total jobs: {total_jobs}")
    
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE scraped = 1")
    scraped_jobs = cursor.fetchone()[0]
    print(f"Scraped jobs: {scraped_jobs}")
    
    if total_jobs > 0:
        percentage = (scraped_jobs / total_jobs) * 100
    else:
        percentage = 0
    
    return percentage

while True:
    start_time = timeit.default_timer()
    query = "SELECT job_id FROM jobs WHERE scraped = 0 LIMIT ?"
    cursor.execute(query, (MAX_UPDATES,))
    result = [r[0] for r in cursor.fetchall()]

    if not result:
        print("No more unscraped jobs. Waiting before checking again...")
        time.sleep(SLEEP_TIME)
        continue

    details = job_detail_retriever.get_job_details(result)
    print(f'{len(details)} job details retrieved')
    
    cleaned_details = clean_job_postings(details)
    print(f'{len(cleaned_details)} job details cleaned')
    
    insert_data(cleaned_details, conn, cursor)
    print(f'UPDATED {len(cleaned_details)} VALUES IN DB')

    # Calculate and print the scraping progress
    progress = get_scraping_progress()
    print(f'Current scraping progress: {progress:.2f}%')
    finish_time = timeit.default_timer()
    print(f'Time taken: {finish_time - start_time:.2f} seconds, scraped per second: {len(cleaned_details) / (finish_time - start_time):.2f}')

    print(f'Sleeping For {SLEEP_TIME} Seconds...')
    time.sleep(SLEEP_TIME)
    print('Resuming...')
