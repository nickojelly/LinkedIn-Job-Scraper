import random
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import pandas as pd

from scripts.helpers import strip_val, get_value_by_path


BROWSER = 'edge'

def create_session(email, password):
    if BROWSER == 'chrome':
        driver = webdriver.Chrome()
    elif BROWSER == 'edge':
        driver = webdriver.Edge()

    driver.get('https://www.linkedin.com/checkpoint/rm/sign-in-another-account')
    time.sleep(1)
    driver.find_element(By.ID, 'username').send_keys(email)
    driver.find_element(By.ID, 'password').send_keys(password)
    driver.find_element(By.XPATH, '//*[@id="organic-div"]/form/div[3]/button').click()
    time.sleep(1)
    input('Press ENTER after a successful login for "{}": '.format(email))
    driver.get('https://www.linkedin.com/jobs/search/?')
    time.sleep(1)
    cookies = driver.get_cookies()
    driver.quit()
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])
    return session

def get_logins(method):
    logins = pd.read_csv('logins.csv')
    logins = logins[logins['method'] == method]
    emails = logins['emails'].tolist()
    passwords = logins['passwords'].tolist()
    return emails, passwords

import requests
import time
import random
import logging
from logging.handlers import RotatingFileHandler

import requests
import time
import random
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
import json
from scripts.proxies import proxies
from tqdm import tqdm

class RateLimitException(Exception):
    time.sleep(20)
    pass

class JobSearchRetriever:
    def __init__(self, country_filter=None, batch_size=100, num_threads=5, sessions_per_thread=100, log_file='linkedin_scraper.log'):
        self.country_filter = country_filter
        self.batch_size = batch_size
        self.num_threads = num_threads
        self.sessions_per_thread = sessions_per_thread
        
        self.job_exists_link = "https://www.linkedin.com/jobs/view/{}/"
        self.job_details_link = "https://www.linkedin.com/voyager/api/jobs/jobPostings/{}?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65"
        
        self.rate_limit_delay = 10  # Initial delay in seconds
        self.jobs_searched = 0
        self.rate_limit_hit = False

        # Set up logging
        self.logger = logging.getLogger('LinkedInScraper')
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG to see all messages
        handler = RotatingFileHandler(log_file, maxBytes=100*1024*1024, backupCount=5, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        # Thread-local storage for sessions
        self.thread_local = local()

        self.proxies = proxies

        self.user_agents = self.generate_random_user_agents(num_agents=1000)

    def generate_random_user_agents(self, num_agents=100):
        browsers = [
            'Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'
        ]
        
        os_list = [
            'Windows NT 10.0; Win64; x64',
            'Windows NT 6.1; Win64; x64',
            'Macintosh; Intel Mac OS X 10_15_7',
            'Macintosh; Intel Mac OS X 10_14_6',
            'X11; Linux x86_64'
        ]
        
        webkits = [
            'AppleWebKit/537.36 (KHTML, like Gecko)',
            'AppleWebKit/605.1.15 (KHTML, like Gecko)',
            'AppleWebKit/537.36 (KHTML, like Gecko)',
            'AppleWebKit/537.36 (KHTML, like Gecko)',
            'AppleWebKit/537.36 (KHTML, like Gecko)'
        ]
        
        versions = [
            '91.0.4472.124', '89.0.4389.82', '88.0.4324.150', '87.0.4280.141', '86.0.4240.198',
            '85.0.4183.121', '84.0.4147.135', '83.0.4103.116', '81.0.4044.138', '80.0.3987.149',
            '79.0.3945.130', '78.0.3904.108', '77.0.3865.120', '76.0.3809.132', '75.0.3770.142',
            '74.0.3729.169', '73.0.3683.103', '72.0.3626.121', '71.0.3578.98', '70.0.3538.110'
        ]
        
        user_agents = []
        
        for _ in range(num_agents):
            browser = random.choice(browsers)
            os = random.choice(os_list)
            webkit = random.choice(webkits)
            version = random.choice(versions)
            
            if browser == 'Chrome':
                user_agent = f'Mozilla/5.0 ({os}) {webkit} Chrome/{version} Safari/537.36'
            elif browser == 'Firefox':
                user_agent = f'Mozilla/5.0 ({os}; rv:{version.split(".")[0]}.0) Gecko/20100101 Firefox/{version.split(".")[0]}.0'
            elif browser == 'Safari':
                user_agent = f'Mozilla/5.0 ({os}) {webkit} Version/{version.split(".")[0]}.0 Safari/537.36'
            elif browser == 'Edge':
                user_agent = f'Mozilla/5.0 ({os}) {webkit} Chrome/{version} Safari/537.36 Edge/{version}'
            elif browser == 'Opera':
                user_agent = f'Mozilla/5.0 ({os}) {webkit} Chrome/{version} Safari/537.36 OPR/{version}'
            
            user_agents.append(user_agent)
        
        return user_agents

    def extract_country_code(self, job_data):
        country_urn = job_data.get('data', {}).get('country')
        if country_urn:
            return country_urn.split(':')[-1].upper()
        return None

    def create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Authority': 'www.linkedin.com',
            'Method': 'GET',
            'Path': '/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List(sortBy-%3EDD,resultType-%3EJOBS)&origin=JOB_SEARCH_PAGE_JOB_FILTER&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start=0&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)',
            'Scheme': 'https',
            'Accept': 'application/vnd.linkedin.normalized+json+2.1',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'X-Li-Track': '{"clientVersion":"1.13.5589","mpVersion":"1.13.5589","osName":"web","timezoneOffset":-7,"timezone":"America/Los_Angeles","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":360,"displayHeight":800}'
        })
        return session

    def get_thread_sessions(self):
        if not hasattr(self.thread_local, 'sessions'):
            self.thread_local.sessions = [self.create_session() for _ in range(self.sessions_per_thread)]
            self.thread_local.session_index = 0
        return self.thread_local.sessions

    def handle_rate_limit(self):
        self.logger.warning(f"Rate limit encountered after searching {self.jobs_searched} jobs.")
        self.logger.info(f"Waiting for {self.rate_limit_delay} seconds.")
        time.sleep(5)
        self.rate_limit_delay *= 2  # Exponential backoff
        self.rate_limit_hit = True

    def get_job_details(self, job_id):
        sessions = self.get_thread_sessions()
        session = sessions[self.thread_local.session_index]
        self.thread_local.session_index = (self.thread_local.session_index + 1) % len(sessions)
        
        proxy = random.choice(self.proxies)
        url = self.job_details_link.format(job_id)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.get(url, proxies=proxy, timeout=10)
                self.jobs_searched += 1

                if response.status_code == 429 or response.status_code == 999:
                    self.logger.warning(f"Rate limit hit for job ID {job_id}")
                    raise RateLimitException()
                
                if response.status_code == 200:
                    job_data = response.json()
                    job_info = {
                        'job_id': job_id,
                        'title': job_data.get('data', {}).get('title', 'NA'),
                        'country': job_data.get('data', {}).get('formattedLocation', 'NA').split(',')[-1].strip(),
                        'country_code': self.extract_country_code(job_data),
                        'sponsored': job_data.get('data', {}).get('sponsored', False)
                    }
                    self.logger.info(f"Valid job found: {job_info}")
                    return job_info
                else:
                    self.logger.warning(f"Unexpected status code {response.status_code} for job ID {job_id}")
                    return None

            except RateLimitException:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.random()
                    self.logger.warning(f"Rate limit hit. Waiting {wait_time} seconds before retry.")
                    time.sleep(wait_time)
                else:
                    self.handle_rate_limit()
                    return {'job_id': job_id, 'valid': False, 'rate_limited': True}

            except json.JSONDecodeError:
                self.logger.error(f"Failed to parse JSON for job ID {job_id}")
                return None
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching details for job ID {job_id}: {str(e)}")
                return None

            time.sleep(random.uniform(1, 3))  # Random delay between retries

        return None

    def get_jobs(self, start_id):
        valid_jobs = {}
        jobs_searched = 0
        rate_limit_hit = False

        def process_job(job_id):
            nonlocal jobs_searched
            job_info = self.get_job_details(job_id)
            jobs_searched += 1
            return job_id, job_info

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_job = {executor.submit(process_job, job_id): job_id 
                             for job_id in range(start_id, start_id + self.batch_size)}
            for future in as_completed(future_to_job):
                job_id, job_info = future.result()
                if job_info and job_info.get('rate_limited', False):
                    rate_limit_hit = True
                    break
                if job_info:
                    valid_jobs[job_id] = job_info

        self.logger.info(f"Jobs searched: {jobs_searched}, valid jobs found: {len(valid_jobs)}, rate limit hit: {rate_limit_hit}")
        return valid_jobs, jobs_searched, rate_limit_hit

    def continuous_job_search(self, start_id):
        current_id = start_id
        while True:
            jobs, jobs_searched, rate_limit_hit = self.get_jobs(current_id)
            yield jobs, jobs_searched, rate_limit_hit
            current_id += self.batch_size
            if rate_limit_hit:
                self.logger.warning(f"Rate limit hit. Sleeping for {self.rate_limit_delay} seconds...")
                time.sleep(random.uniform(10, 20))
                self.rate_limit_delay = max(60, self.rate_limit_delay * 2)  # Increase delay, but not less than 60 seconds
            else:
                time.sleep(random.uniform(10, 20))
            
import threading
class JobDetailRetriever:
    def __init__(self,num_threads=5, log_file='job_retriever.log'):
        self.error_count = 0
        self.job_details_link = "https://www.linkedin.com/voyager/api/jobs/jobPostings/{}?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65"
        emails, passwords = get_logins('details')
        self.emails = emails
        self.sessions = [create_session(email, password) for email, password in list(zip(emails, passwords))*5]
        self.session_index = 0
        self.variable_paths = pd.read_csv('json_paths/data_variables.csv')
        self.num_threads = num_threads
        self.thread_local = threading.local()

        #Logging
        self.logger = logging.getLogger('JobDetailRetriever')
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.headers = [{
            'Authority': 'www.linkedin.com',
            'Method': 'GET',
            'Path': '/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List(sortBy-%3EDD,resultType-%3EJOBS)&origin=JOB_SEARCH_PAGE_JOB_FILTER&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start=0&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)',
            'Scheme': 'https',
            'Accept': 'application/vnd.linkedin.normalized+json+2.1',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': "; ".join([f"{key}={value}" for key, value in session.cookies.items()]),
            'Csrf-Token': session.cookies.get('JSESSIONID').strip('"'),
            # 'TE': 'Trailers',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            # 'X-Li-Track': '{"clientVersion":"1.12.7990","mpVersion":"1.12.7990","osName":"web","timezoneOffset":-7,"timezone":"America/Los_Angeles","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":1920,"displayHeight":1080}'
            'X-Li-Track': '{"clientVersion":"1.13.5589","mpVersion":"1.13.5589","osName":"web","timezoneOffset":-7,"timezone":"America/Los_Angeles","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":360,"displayHeight":800}'
        } for session in self.sessions]

        self.proxies = proxies


    def get_session(self):
        if not hasattr(self.thread_local, 'session'):
            self.thread_local.session = random.choice(self.sessions)
        return self.thread_local.session

    def get_job_detail(self, job_id):
        session = self.get_session()
        proxy = random.choice(self.proxies)
        headers = random.choice(self.headers)
        url = self.job_details_link.format(job_id)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers, proxies=proxy, timeout=10)
                self.logger.info(f"Job {job_id} response: {response.status_code}")
                if response.status_code == 200:
                    return job_id, response.json()
                elif response.status_code in [429, 999]:
                    self.logger.warning(f"Job {job_id} rate limit exceeded")
                    time.sleep(20)
                    raise RateLimitException()
                elif response.status_code == 400:
                    self.logger.error(f"Bad request for job ID {job_id}. Response: {response.text}")
                    return job_id, -1
                else:
                    self.logger.warning(f"Unexpected status code {response.status_code} for job ID {job_id}")
                    return job_id, -1
            except (RateLimitException, requests.exceptions.RequestException) as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to retrieve job {job_id} after {max_retries} attempts")
                    return job_id, -1
                time.sleep((2 ** attempt) + random.random())

        return job_id, -1

    def get_job_details(self, job_ids):
        job_details = {}
        with tqdm(total=len(job_ids), desc="Retrieving job details") as pbar:
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                future_to_job = {executor.submit(self.get_job_detail, job_id): job_id for job_id in job_ids}
                for future in as_completed(future_to_job):
                    job_id, result = future.result()
                    job_details[job_id] = result
                    if result == -1:
                        self.error_count += 1
                        if self.error_count == len(job_ids)-10:
                            self.logger.error('Too many errors')
                            raise Exception('Too many errors')
                    else:
                        self.error_count = 0
                    self.logger.info(f'Job {job_id} done')
                    pbar.update(1)
        return job_details




# https://proxy2.webshare.io/register?

