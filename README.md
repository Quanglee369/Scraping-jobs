# Overview (TL;DR)

This is and end to end project from scraping data-related jobs, processing, relabelling using AI and push data into SQL tables, visualizing data using Google Looker Studio. As my pain point all about having to switch between multiple website look for jobs. Thus I created my own job board workflow that not only give me way more jobs that I could not find but also filter out non-relevant ones.

# Tech Stack

## **Python (with the following libraries)**

   * requests, httpx, aiohttp for pulling data from websites' apis
   * playwright, playwright-stealth to scrape data using browser session for websites that have no exposed internal APIs
   * langchain-groq, langchain_openai to relabel job titles for higher accuracy
   * pandas, numpy, scikit-learn to clean, transform and filter out duplicate job postings, prepare job data for SQL database update
   * sqlalchemy, psycopg2-binary, pydantic use to define table schema and interact with postgresql database with ORM

## **Supabase (Postgresql) to store data**

   * Design database with star schema to handle the analytical purposes
   * Utilizing cron jobs to check for expired jobs

## **Google Looker Studio**

   * Design a dashboard for continuous monitor of job martkets metrics
   * A seperate page dedicate for monitoring the efficiency of the workflow for continuous improvement


# Details for each teach stack

## **Python**
Websites that data will be extracted from are careerviet, vietnamworks and itviec as I find their platform cover a good amount of data-relevant job that I'm targeting for this project. I create my python ETL workflow with 3 standard layers. 

###Extracting layer
  * Careerviet and vietnamworks expose their internal API so I use that to extract clean Json data from the website using aiohttp for asynchronous request which speed up the process
  * Itviec do not expose their internal API, plus their website is dynamic loading so I apply stealth playwright to scrape the data and avoid getting flagged
  * All this tasks are done with ```asyncio (run asynchronous request)```, intercept (abort loading unecessary resource when scraping) and asyncio.semaphore at 20 request to prevent overloading the API



