# Overview (TL;DR)

This is and End-to-End project from scraping data-related jobs, processing, AI relabelling and push data into SQL tables of **Supabase**  using **Python** and **Github Action**, visualizing data using **Google Looker Studio**. As my pain point is all about having to switch between multiple websites looking for jobs. Thus I created my own job board that not only give me way more jobs that I could not find but also filter out non-relevant ones.

# Tech Stack

## **Python (with the following libraries)**

   * ```requests```, ```httpx```, ```aiohttp``` for pulling data from websites' apis
   * ```playwright```, ```playwright-stealth``` to scrape data using browser session for websites that have no exposed internal APIs
   * ```langchain-groq```,```langchain_openai``` to relabel job titles for higher accuracy using free AI model from **Groq** and **OpenRouter**
   * ```pandas```, ```numpy```, ```scikit-learn``` to clean, transform and filter out duplicate job postings, prepare job data for SQL database update
   * ```sqlalchemy```, ```psycopg2-binary```, ```pydantic``` use to define table schema and interact with postgresql database with ORM

## **Supabase (Postgresql) to store data**

   * Design database with star schema to handle the analytical purposes
     
!("database_schema.png")

   * Utilizing cron jobs to check for expired jobs

## **Google Looker Studio**

   * Design a dashboard for continuous monitor of job martkets metrics
   * A seperate page dedicate for monitoring the efficiency of the workflow for continuous improvement


# Details for each teach stack

## **Python**
Websites that data will be extracted from are careerviet, vietnamworks and itviec as I find their platform cover a good amount of data-relevant job that I'm targeting for this project. I create my python ETL workflow with 3 standard layers. 

### Extracting layer
  * Careerviet and vietnamworks expose their internal API so I use that to extract clean Json data from the website using ```aiohttp``` for asynchronous request which speed up the process.
  * Itviec do not expose their internal API, plus their website is dynamic loading so I apply ```stealth playwright``` to scrape the data and avoid getting flagged. As scraping through browser sessions, I need to find a way to create a unique ID for Itviec's jobs, thus i use ```hashlib``` to hash job link (which should be unique by default) and make it my primary key. 
  * All this tasks are done with ```asyncio``` (run asynchronous request), intercept function (abort loading unecessary resource when scraping) and ```asyncio.semaphore``` at 20 request to prevent overloading the API. Exponential backoff retry strategy also applied to ensure the code roburstness

### Tranforming layer
  * All of the data will be filter first using simple duplicate removal
  * After that data will be tranform and input into AI models to relabel, as sometime job naming can be very inconsistent and using fix keyword without some degree of sentiment will result in a loss of valuable job postings.
  * I primarily use ```langchain-groq``` with model **meta-llama/llama-4-scout-17b-16e-instruct** as they provide a decent model with generous free tier, fall-back options using ```langchain_openai``` together with exponential backoff retry strategy also applied to ensure **100% uptime**.
  * Although have been trimmed down, the amount of data can still be a hug workload for free AI models, thus I use early labeling base on fix keyword that signal the exact relevant jobs (i.e Data Analyst, Data Engineer,...)
  * Finally the data is sent to AI models for labeling, to be more certain, I also chunking data into 30 records each with 25s delay each records ensuring AI model is not overloaded or mistaken due to the large amount of input.
  * Finally, I remove the duplicate jobs (cross-platform) as a company can post same position many website, I use ```cosine_similarity``` at the threshold of 0.75 (due to ```employer_name and```, ```job_title``` can be inconsistent between websites) to filter out similar posting base on a pair of ```job_title``` and ```employer_name```

### Loading layer
  * For the final step, I use ```sqlalchemy.orm``` to define table schemas and interact with SQL database more efficiently with pythonic syntax
  * First step is about updating the dimensional data with ```on_update_do_nothing``` as it is expect to have duplicate values from time to time
  * After that I query the newly updated dimensional data, mapping with the fact tables to get related ids and update the 2 fact tables 

