from collections import defaultdict
import queue

import time

from bs4 import BeautifulSoup
import requests

import threading

# 1) To do : handle bad requests
# 2) To do : Span multiple processes for each job category.
# 3) In case of job already existing ignore it 
# 4) Add auutomatic summmarization for job descriptions.

class Scrapper:
    
    def __init__(self, scraps_needed = 10):
        
        self.main_site = "https://www.simplyhired.com"
        
        self.database = defaultdict(dict)
        self.unique_jobs = set()

        self.scraps_needed = scraps_needed

        self.jobs_Q = queue.Queue()
        self.sentinel = object()


    # TO DO POINT 2 :
    def tmp(self):
        # 1. Will need a list of Qs for each process we create.
        # Each process will have it's seperate threads and a seperate Q as a result.
        # 2. Multiple processes.
        # 3. Split the required scraps_needed / number of processes 
        # e.g. We want 100 urls scraped for 20 cats
        # Option 1) 5 jobs for each cat or 
        # Option 2) 100 for each
        # Option 3) time limit them
        # Option 3) let them run for a fixed amount of time or instead of demanding a number.
        """
        top_20_job_boards = self.get_job_boards()
        all_qs = [ queue.Queue() for _ in range(len(top_20_job_boards)) ]
        scrape_job_board(self, theads_needed = 10, ) # This funcions must have a Q parameter. !sentinels!
        processes = [scrape_job_board(self, theads_needed = 10, Q) for Q in Qs] 
        """
        return
    

    # Initialize the scrapper. Uses a producer consumer model with a Q.
    # Job boards refers to the category of the jobs. Browse - > All jobs - > e.g. Receptionist/Manager/Sales
    def scrape_job_board(self, theads_needed = 10):
        
        top_20_job_boards = self.get_job_boards()
        # We will take only 1 category of jobs. The first category in top#20
        random_job_board = top_20_job_boards[0]
        category, url = random_job_board[0], random_job_board[1]

        sentinels_needed = theads_needed
        producer_thread = threading.Thread(target = self.producer_jobs, args=(category, url, sentinels_needed))
        consumer_threads = [threading.Thread(target = self.consumer_jobs) for _ in range(sentinels_needed)]

        # Start the threads
        producer_thread.start()
        for consumer_thread in consumer_threads : consumer_thread.start()
            
        # Wait for them to finish. And exit gracefully.
        producer_thread.join()
        for consumer_thread in consumer_threads : consumer_thread.join()
        self.jobs_Q.join()
        
    
    # Collects the urls of the job postings in the current page(11) puts them in the Q and then proceeds to the next page.
    # Terminates when all the required jobs are collected or when there are no more job postings.
    def producer_jobs(self, category, url, sentinels):
    
        job_postings, next_page = self.get_jobs_in_page( url )
        while( self.scraps_needed > 0 and next_page):

            for job in job_postings: 
                self.jobs_Q.put([category, job])
                self.scraps_needed -= 1
            job_postings, next_page = self.get_jobs_in_page( next_page )

        for _ in range(sentinels):
            self.jobs_Q.put(self.sentinel)
        
        
    def consumer_jobs(self):
        
        while( True ):
            item = self.jobs_Q.get()
            if item is self.sentinel:
                self.jobs_Q.task_done()
                break
            category, url = item[0], item[1]
            self.scrape_job_create_entry(category, url)
            # TO DO POINT 3 : If the job exists already we can add a self.scraps_needed += 1  
            # Mark the element as consumed (releases the lock on the Q)
            self.jobs_Q.task_done()


    # TO DO POINT 1 : handle bad requests
    def handle_request(self):
        return
    

    # Returns the [category of the job board, url] of all the job_boards we will be searching. Currently finds the top# 20.
    def get_job_boards(self):

        page = requests.get(self.main_site + "/browse-jobs/titles")
        soup = BeautifulSoup(page.text, 'html.parser')
        
        job_boards = soup.find_all('div', attrs={'class' : 'item-list simple-list'})[0]
        job_boards = [[job_board.text, self.main_site + job_board['href']] for job_board in job_boards.find_all('a')]
        
        return job_boards
    

    # Returns all the urls of the jobs in the current page and the url for the next page.
    def get_jobs_in_page(self, url):

        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        urls = soup.find('ul', attrs={'role': 'list', 'id' : 'job-list'})
        urls = [self.main_site + url.find('a')["href"] for url in urls.find_all('h3')]
        
        next_page_url = soup.find('nav', attrs={'role': 'navigation'}).find_all('a')[-1]['href']
        
        return urls, next_page_url
        

    # Scrapes all the necessary information of the current job posting.
    # Adds it to the appropriate category.
    # Assumes there are no 2 same job postings. Else updates the old one.
    def scrape_job_create_entry(self, category, url):

        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        main_title_of_job = soup.title.text
        job_title = main_title_of_job#.split('|')[0]
        
        job_type = soup.find_all('span', attrs={'data-testid': 'detailText'})
        company = job_type[0].text
        if len(job_type) > 3 : 
            salary = job_type[3].text
            pay_type = job_type[2].text.split('|')
        else : 
            salary = None  
            if len(job_type) < 2 : 
                pay_type = None
            else:
                pay_type = job_type[2].text.split('|')
           
        job_qualifications = soup.find_all('span', attrs={'data-testid': 'viewJobQualificationItem'})
        qualifications = [item.text for item in job_qualifications]
        
        description = soup.find_all('div', attrs={'data-testid': 'viewJobBodyJobFullDescriptionContent'})[0].text
        
        link = soup.find('a', attrs={'data-testid': 'viewJobHeaderApplyButtonSERP'})
        apply_Link = self.main_site + link["data-mdref"]
        
        entry = {
            "jobTitle" : job_title, 
            "employerName" : company, 
            "compensation" : salary, 
            "jobTypes" : pay_type, 
            "qualifications" : qualifications, 
            "jobDescriptionHtml" : description, 
            "applyUrl" : apply_Link 
        }
        
        self.unique_jobs.add(main_title_of_job)
        self.database[category][main_title_of_job] = entry


if __name__ == '__main__':
    # 22 - > 14.27sec
    # 110 - > Must take(71.35) and took (~ 50)
    new_scrapper = Scrapper(110)

    start_time = time.time()
    new_scrapper.scrape_job_board()
    end_time = time.time()

    nice = lambda x : x + (len("jobDescriptionHtml") - len(x) + 2 ) * ' ' + " :    "
    
    for category in new_scrapper.database:

        print("\nCurrent category is : ", category, '\n', 27*'~',"\n\n")
        jobs = new_scrapper.database[category]

        for job in jobs:
            
            print(job)
            current_job_information = jobs[job]
            for info in current_job_information:
                if info in { "jobTypes", "qualifications"}:
                    print(nice(info), ", ".join(current_job_information[info]), '\n')
                else:
                    print(nice(info), current_job_information[info], '\n')
            print(27 * '~', '\n')

        print(27 * '~', '\n')
    
    print("Elapsed time: %.2f seconds " % (end_time - start_time) )
    print("TOTAL SCRAPED : ", len(new_scrapper.unique_jobs))
    print("Names : ")
    print(new_scrapper.unique_jobs)
  