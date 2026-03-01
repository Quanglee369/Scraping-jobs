import asyncio
import logging
import httpx
import re
import hashlib
import json
import time
import pandas as pd
from datetime import timedelta
from itertools import chain
from playwright.async_api import async_playwright
from playwright_stealth import Stealth


# [FUNCTION] Fetch JD from each link, only apply to Careerviet as they do not have skill section in the job headers
# Use asyncio.Semaphore at 20 requests each to prevent getting flagged
async def fetch_jd_careerviet(client, item, sem = asyncio.Semaphore(20)):
  # Replace locale as the default locale in api is "en" which is not accessible
  link = item.get('job_link').replace('https://careerviet.vn/en/', 'https://careerviet.vn/vi/')
  job_id = item.get('job_id')
  async with sem:
    try:
      response = await client.get(link, timeout = 10)
      # Filter the JD from html response (converted to text)
      if '__next_f.push([1,"{\\"@context\\"' in response.text:
        clean_text = response.text.split('__next_f.push([1,"{\\"@context\\')[1].split('Ngành nghề:')[0].split('description')[1]
        return {job_id: clean_text}

    except Exception as e:
      logging.error(f'[fetch_jd_careerviet] Error when fetching link {link}, detail: {e}')
      print(f'[fetch_jd_careerviet] Error when fetching link {link}, detail: {e}')
      return None

# [FUNCTION] Create list of tasks to use asyncio for concurency of fetch_jd_careerviet
async def get_all_jobs(data):
  async with httpx.AsyncClient() as client:
    task = [fetch_jd_careerviet(client, item) for item in data]
    results = await asyncio.gather(*task)
    if results:
      return results
    else:
      return None

# [FUNCTION] Get job headers based on position and platform
async def fetch_job_headers(session, keyword: str, platform: str):

# Default number of maximum return is 200
  payload_careerviet = {
    'keyword': keyword,
    'limit': 200
}

  payload_vietnamworks = {
      "query": keyword,
      "hitsPerPage": 200
  }

  url_careerviet = 'https://internal-api.careerviet.vn/api/v1/js/jsk/jobs/public'
  url_vietnamworks = 'https://ms.vietnamworks.com/job-search/v1.0/search'

  mapping_dict = {'careerviet': [payload_careerviet, url_careerviet],
                  'vietnamworks': [payload_vietnamworks, url_vietnamworks]}

  clean_platform = platform.lower().strip()
  # Check to ensure platform name is valid
  if clean_platform not in mapping_dict.keys():
    return print(f'[fetch_job_headers] Keyword: {clean_platform} does not match any of the list {mapping_dict.keys()}')

  # For vietnamworks api, using POST not GET
  try:
    if clean_platform == 'vietnamworks':
      async with session.post(url = mapping_dict.get(clean_platform)[1], json = mapping_dict.get(clean_platform)[0]) as Response:
        if Response.status == 200:
          data = await Response.json()
          return {clean_platform: data.get('data', '')}
    else:
      async with session.get(url = mapping_dict.get(clean_platform)[1], params = mapping_dict.get(clean_platform)[0]) as Response:
        if Response.status == 200:
          data = await Response.json()
          return {clean_platform: data.get('data', '')}
  except Exception as e:
    logging.error(f'[fetch_job_headers] Can not get job headers for position {keyword}, Error: {e}, Status Code: {Response.status_code}')
  return None

# The following section is specifically built for scraping ITViec as they don't expose their API (Using Playwright Stealth)
# [FUNCTION] Eliminate loading unnecessary resources (image, stylesheet, font, etc.)
async def apply_speedup(page):
    async def intercept(route):
        if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", intercept)

# [FUNCTION] Extract jobs information from a job card in ITviet website
async def extract_card_data(card):
  data = await card.eval_on_selector(
          "h3[data-search--job-selection-target='jobTitle']",
          "el => ({ title: el.innerText, url: el.getAttribute('data-url') })"
      )
  # Search for elements that contain specific data
  company_task = card.query_selector("a.text-rich-grey")
  location_task = card.query_selector("div[title]")
  date_task = card.query_selector(".small-text.text-dark-grey")
  skills_task = card.query_selector_all(".itag")

  company_el, location_el, date_el, skill_elements = await asyncio.gather(
        company_task, location_task, date_task, skills_task
    )

  # Get company name
  company = await company_el.inner_text() if company_el else "N/A"

  # Get location
  location = await location_el.get_attribute("title") if location_el else "N/A"

  # Get skills
  skills = await asyncio.gather(*[s.inner_text() for s in skill_elements])

  # Get date
  rawtext = await date_el.inner_text() if date_el else "N/A"
  rawtext = rawtext.replace('\n', ' ').strip()
  days_match = re.search(r'\d+', rawtext)
  current_date = pd.Timestamp.now().date()
  if days_match and 'hour' in rawtext.lower():
      actual_hours = int(days_match.group())
      postingdate = (current_date- timedelta(hours = actual_hours))

  elif days_match and 'day' in rawtext.lower():
      actual_day = int(days_match.group())
      postingdate = (current_date - timedelta(days = actual_day))

  elif days_match and 'minute' in rawtext.lower():
      actual_min = int(days_match.group())
      postingdate = (current_date - timedelta(minutes = actual_min))

  else:
      postingdate = 'N/A'

  # Prepare data for hashing for job id
  skill_for_hash = ''.join(skills)

  components = [
    str(data['url']).strip().lower()
    ]

  combined_string = '|'.join(components)

  job_id_hash = hashlib.md5(combined_string.encode('utf-8')).hexdigest()

  return {
        "job_id": job_id_hash,
        "job_title": data['title'].strip(),
        "job_link": data['url'],
        "posting_date": postingdate,
        "company": company.strip(),
        'location': location.strip(),
        "skills": list(skills)
    }

# [FUNCTION] Extract job data from each page
async def extract_page_data_itviec(page):
    job_list = []
    # Wait for at least an instance of .job card appear to make sure data is fully loaded
    await page.wait_for_selector(".job-card", timeout=10000)
    cards = await page.query_selector_all(".job-card")

    # Run asyncio to extract data more efficiently
    job_list = await asyncio.gather(*[extract_card_data(card) for card in cards])
    return list(job_list)

# [FUNCTION] Scraping itviec website based on keyword
async def run_itviec_scraper(keyword: str):
    stealth = Stealth()
    all_jobs = []
    clean_keyword = keyword.strip().lower().replace(' ', '-')

    # Initialize Playwright Stealth
    async with stealth.use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)
        # Configure viewport to mimic user behavior
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()

        # Apply speed up function
        await apply_speedup(page)

        url = f"https://itviec.com/it-jobs/{clean_keyword}"
        print(f"[run_itviec_scraper] Accessing: {url}")
        await page.goto(url, wait_until="domcontentloaded")

        # Loop through all page until there is no page left for the keyword
        page_count = 1
        while True:
            print(f"[run_itviec_scraper] Processing page {page_count}...")
            jobs_on_page = await extract_page_data_itviec(page)
            all_jobs.extend(jobs_on_page)
            print(f"[run_itviec_scraper] Processed {len(jobs_on_page)} job(s).")

            # Save current page number
            current_page_el = await page.query_selector(".page.current")
            if not current_page_el:
                break
            old_page_num = await current_page_el.inner_text()

            # Move to next page
            next_button = await page.query_selector('a[rel="next"]')
            if next_button:
                await next_button.click()
                page_count += 1

                # Check if moved to next page is successful by comparing current page number with old page number
                try:
                    await page.wait_for_function(
                        f"document.querySelector('.page.current').innerText !== '{old_page_num}'",
                        timeout=7000
                    )
                except Exception:
                    print("[run_itviec_scraper] Timeout ! There might not be any page left to process.")
                    break
            else:
                print("[run_itviec_scraper] Complete.")
                break

        await browser.close()
        print(f"\n[run_itviec_scraper] A total of : {len(all_jobs)} job(s) have been processed.")
        return all_jobs


# [FUNTION] function to process a list of dicts that have key as job_id and value as job_title
def process_all_at_once(list_of_dicts):
    formatted_input = json.dumps(list_of_dicts, ensure_ascii=False, indent=2)
    max_retries = 2

    for attempt in range(max_retries+1):
      try:
        result = chain.invoke({"input_data": formatted_input})
        if not result or not hasattr(result, 'jobs') or len(list_of_dicts) != len(result.jobs):
          raise ValueError('Incomplete outputs !')

        return [{job.job_id : job.job_label for job in result.jobs}]
      except Exception as e:
        logging.warning(f'[process_all_at_once] Atempt {attempt + 1} failed. Error: {e}')

        if attempt < max_retries:
          time.sleep(5**attempt)
          continue
        else:
          logging.error(f'[process_all_at_once] Unable to retrieve result from AI, error: {e}')
          return {}

# [FUNCTION] Chunking request sending to AI model with delay of 30 second prevent overloading the model
def chunking(data, min_size = 20):
  batch = []
  total = len(data)
  i = 0

  while i < total:
    batch.append(data[i: i + min_size])
    i += min_size

  return batch
