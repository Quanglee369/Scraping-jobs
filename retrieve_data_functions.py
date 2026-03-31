import asyncio
import logging
import re
import hashlib
import random
import json
import time
from selectolax.parser import HTMLParser
from datetime import timedelta, datetime
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from master_config import html_scraping_dict, api_scraping_dict, jd_dict_selector
from typing import List, Literal, Union, Dict, Any
import aiohttp
import os


async def fetch_jd(session: aiohttp.ClientSession, item: Dict, platform: str, sem: asyncio.Semaphore)-> Dict[str, str]:
    """Get job description info from links
    Args:
      session: aiohttp session
      item (dict): a dict that contain info of a job
      platform (str): name of the available platform
      sem: asyncio.semaphore at 10 request each time to prevent api ban

    Return:
      final_jd_data (dict): a dict with key as job id and value as job description
    """

    # Extract configuration based on platform
    config = jd_dict_selector.get(platform)
    if not config:
      logging.error(f'[fetch_jd] Plaform {platform} is not in the list {list(jd_dict_selector.keys())}')
      return {}
    
    if not item:
      logging.error(f'[fetch_jd] Input data empty or invalid, data len: {len(item)}')
      return {}

    # Logic for CareerViet specific locale replacement
    job_link_name = config.get('job_link_header')
    job_desc = config.get('job_desc')
    header = config.get('headers')
    retry = 0
    response = None

    while retry < 4:
      async with sem:
        try:
            job_id = item.get('job_id')
            job_link = item.get(job_link_name)
            if platform == 'careerviet':
              job_link = job_link.replace('https://careerviet.vn/en/', 'https://careerviet.vn/vi/')
            # Perform the request
            response = await session.get(job_link, headers=header, timeout=10)

            if response.status == 200:
              break

            elif response.status == 429:
              logging.error(f"[fetch_jd] Error, too many request for platform {platform} link: {job_link} retry for {retry + 1} time")
              await asyncio.sleep(2**retry)
              retry += 1
              continue

            elif response.status == 404:
              return {}

            elif response.status != 200:
              logging.error(f"[fetch_jd] Error, status code: {response.status} for platform: {platform} link: {job_link}")
              return {job_id: ''}

        except Exception as e:
            logging.error(f'[fetch_jd] Error when fetching link {job_link}, detail: {e}')
            return {}

    if response is None or response.status != 200:
        logging.error(f'[fetch_jd] Error when fetching link {job_link}')
        return {job_id: ''}
        
    # Filter the JD from html response
    jd_text = await response.text()
    jdhtml = HTMLParser(jd_text)
    clean_text = ''.join([i.text(strip=True) for i in  jdhtml.css(job_desc)])

    return {job_id: clean_text.strip()}

async def fetch_jd_mult(session: aiohttp.ClientSession, data: Dict[str, List]) -> Dict[str, List]:
  """This function is used to handle batch process of the fetch_jd function
  Args:
    session: aiohttp session
    data (dict): a dict with key as platform name and value as list of dict that contain job information

  Return:
    final_data (dict): a dict with key as platform name and value as list of dict contain job description and its job id
  """
  final_data = {}
  sem= asyncio.Semaphore(10)
  for key, value in data.items():
    task = [fetch_jd(session = session, item = single_val, platform = key, sem= sem) for single_val in value]
    extracted_jd = await asyncio.gather(*task)
    if key not in final_data.keys():
      final_data[key] =  extracted_jd
    else:
      final_data[key].extend(extracted_jd)
  return final_data

async def fetch_job_headers(session: aiohttp.ClientSession, keyword: str, page_num: int,platform: str) -> Dict[str, List]:
    """Scrap job information directly from website api
    Args:
      session: aiohttp session
      keyword (str): name of the search keyword such as Data Analyst, Data Engineer, etc.
      platform (str): name of the available platform
      page_num (int): number of page

    Return:
      A dict with key as platform name and value as list of dict contain job data from website apis
    """

    clean_platform = platform.lower().strip()

    # Check to ensure platform name is valid
    if clean_platform not in api_scraping_dict:
        logging.error(f'[fetch_job_headers] Keyword: {clean_platform} does not match any of the list {list(api_scraping_dict)}')
        return {}

    config = api_scraping_dict[clean_platform]
    url = config.get('url')
    payload = config.get('payload')
    params = {}
    for key, value in payload.items():
      if isinstance(value, str):
        params[key] = value.format(keyword = keyword, page_num = page_num)
      else:
        params[key] = value
    header = config.get('header')
    retry = 1
    data = {}

    if clean_platform == 'monster':
      proxy_url = os.environ.get('PROXY_URL')
    else:
      proxy_url = None

    while retry < 4:
      try:
          # For vietnamworks api, using POST not GET
          if clean_platform == 'vietnamworks':
              async with session.post(url=url, json=params, headers=header) as response:
                  if response.status == 200:
                    data = await response.json()
                    break
                  elif response.status == 429:
                    logging.warning(f'[fetch_job_headers] Too many request, proceed to retry {retry} time(s)')
                    await asyncio.sleep(2 ** retry)
                    retry += 1
                    continue
                  elif response.status != 200:
                    logging.error(f'[fetch_job_headers] Can not get job data from api of platform: {platform}')
                    return {}
                  else:
                    logging.error(f'[fetch_job_headers] Can not get job data from api of platform: {platform}')
                    break

          else:
              async with session.get(url=url, params=params, headers=header,  proxy = proxy_url) as response:
                  if response.status == 200:
                      data = await response.json()
                      break
                      
                  elif response.status == 429:
                    logging.warning(f'[fetch_job_headers] Too many request, proceed to retry {retry} time(s)')
                    await asyncio.sleep(2 ** retry)
                    retry += 1
                    continue
                      
                  elif response.status != 200:
                    logging.error(f'[fetch_job_headers] Can not get job data from api of platform: {platform}, status: {response.status}')
                    return {}
                      
                  else:
                    logging.error(f'[fetch_job_headers] Can not get job data from api of platform: {platform}, status: {response.status}')
                    break

      except Exception as e:
          print(f'[fetch_job_headers] Can not get job headers for position {keyword}, Error: {e}')
          return {}

    if data and isinstance(data, dict):
      return {clean_platform: data.get('data', [])}

    return data

async def html_scraping(keyword: str, platform: str, page_num: int, session: aiohttp.ClientSession, sem:  asyncio.Semaphore) -> List[Dict[str, Any]]:
    """Scrap job informations from html data
    Args:
      keyword (str): name of the search keyword such as Data Analyst, Data Engineer, etc.
      platform (str): name of the available platform
      page_num (int): number of page
      session: aiohttp session
      sem: asyncio.semaphore at 10 request each time to prevent api ban

    Return:
      final_data (list of dict): list of dict with key as platform name and value as a dict contain job information
    """

    # 1. Config Retrieval & Validation
    current_config = html_scraping_dict.get(platform)
    if not current_config:
        print(f"Error: {platform} not found in config.")
        return []

    # 2. Variable Preparation
    url_template = current_config.get('url', '')
    url = url_template.format(keyword=keyword)

    raw_payload = current_config.get('payload', {})
    params = {}
    if isinstance(raw_payload, dict):
        for k, v in raw_payload.items():
            params[k] = str(v).format(keyword=keyword, page_num=page_num)

    domain = current_config.get('domain', '')
    selectors = current_config.get('selector', {})
    headers = current_config.get('header', {})

    final_data = {platform: []}
    html_data = None  # 1. Initialize as None at the top
    retry = 1

    # 3. Network Request
    while retry < 4:
      async with sem:
        try:
          await asyncio.sleep(random.uniform(1.5, 4.0))
          async with session.get(url, params=params, headers=headers, timeout=15) as response:
              if response.status == 200:
                html_data = await response.text()
                break

              elif response.status == 429:
                logging.warning(f"[html_scraping] Too much request, retry {retry} time")
                retry += 1
                await asyncio.sleep(2**retry)
                continue

              elif response.status != 200:
                logging.error(f"[html_scraping] Failed to fetch {platform}: Status {response.status}, Keyword: {keyword}, Page_num: {page_num}")
                return final_data

              else:
                logging.error(f"[html_scraping] Failed to fetch {platform}: Status {response.status}, Keyword: {keyword}, Page_num: {page_num}")
                break

        except Exception as e:
            logging.error(f"Connection error for {platform}: {e}")
            return final_data

    if not html_data:
        return final_data

    # 4. Parsing Logic
    parser = HTMLParser(html_data)
    container_selector = selectors.get('container')

    if not container_selector:
        return []

    for card in parser.css(container_selector):
        # HELPER: Safe extraction to prevent NoneType crashes
        def get_text(sel_key: str) -> str:
            sel = selectors.get(sel_key)
            if not sel or sel == "None":
                return "None"
            node = card.css_first(sel)
            return node.text(strip=True) if node else "None"

        def get_attr(sel_key: str, attr: str) -> str:
            sel = selectors.get(sel_key)
            if not sel or sel == "None":
                return "None"
            node = card.css_first(sel)
            return node.attributes.get(attr, "None") if node else "None"

        # Field Extraction
        title = get_text('job_title')
        employer = get_text('emp_name')
        location = get_text('location_name')

        # Link Logic (Handle relative vs absolute)
        raw_href = get_attr('job_link', 'href')
        full_link = raw_href if raw_href.startswith('http') or raw_href == "None" else f"{domain}{raw_href}"

        # Created On Logic (Special Case: Attributes)
        if platform == 'job123':
            # Job123 stores date in data-time on the card itself
            created = card.attributes.get('data-time', "None")
        elif platform == 'careerlink':
            # Careerlink stores it in an attribute of a specific span
            created = get_attr('created_on', 'data-datetime')
        else:
            created = get_text('created_on')

        # Job Description Logic
        description = get_text('job_desc')

        final_data[platform].append({
            'job_id': hashlib.md5(full_link.encode('utf-8')).hexdigest(),
            'job_title': title,
            'emp_name': employer,
            'location_name': location,
            'job_link': full_link,
            'created_on': created,
            'job_desc': description
        })

    return final_data

async def process_none_student_job( item: Dict, session: aiohttp.ClientSession, sem: asyncio.Semaphore)-> Dict:
  """Get location, created date specifically for student job site
  Args:
    item (dict): a dict containing job info for a job
    session: aiohttp session
    sem: asyncio.semaphore at 10 request each time to prevent api ban

  Returns:
    jd_data_text (dict): a dict containing job info for a job

  """
  lat = jd_dict_selector.get('studentjob').get('lat')
  header =  jd_dict_selector.get('studentjob').get('headers')
  created_on = jd_dict_selector.get('studentjob').get('created_on')
  link = item.get('job_link')

  jd_data_text = None

  retry = 1
  while retry < 4:
    async with sem:
      try:
        async with await session.get(link, headers = header, timeout = 10) as jd_data:
          if jd_data.status == 200:
            jd_data_text = await jd_data.text()
            break

          elif jd_data.status == 429:
            logging.warning(f'[process_none_student_job] Too much request for link: {link} proceed to retry: {retry} time(s)')
            retry += 1
            await asyncio.sleep(2**retry)
            continue

          elif jd_data.status != 200:
            logging.error(f'[process_none_student_job] Error when retrieving data for link: {link} status code: {jd_data.status}')
            return item
      except Exception as e:
        logging.error(f"[process_none_student_job] Connection error: unable to access link: {link} error: {e}")
        return item

  if not jd_data_text:
        return item

  parser = HTMLParser(jd_data_text)
  for i in parser.css(created_on):
    text_data = i.text(strip=True)
    if "/" in text_data and len(text_data) == 10:
      item['created_on'] = text_data
      break

  try:
      nodes = parser.css(lat)
      if len(nodes) >= 2:
          lat_extracted = nodes[0].attributes.get('value', 0)
          lon_extracted = nodes[1].attributes.get('value', 0)
          if not lat_extracted or lon_extracted:
            item['location_name'] = 'None'
          else:
            lat_val = float(lat_extracted)
            lon_val = float(lon_extracted)
            item['location_name'] = rg.search((lat_val, lon_val))[0]['admin1']
  except Exception as e:
      logging.error(f"[process_none_student_job] Geocoding failed for {link}: {e}")

  return item

async def process_none_student_job_mult(session, data: Dict[str, List])-> Dict[str, List]:
  """This function is use to handle batch process of the function process_none_student_job
  Args:
    session: aiohttp session
    data (dict of list): dict of list that contain info of jobs

  Returns:
    final_result (dict of list): dict of list that contain info of jobs

  """
  final_result = {}
  sem = asyncio.Semaphore(10)
  for key, value in data.items():
    task = [process_none_student_job(session = session, item = i, sem = sem) for i in value]
    extracted_data = await asyncio.gather(*task)
    if key not in final_result.keys():
      final_result[key] = extracted_data
    else:
      final_result[key].append(extracted_data)
  return final_result

# The following section is specifically built for scraping ITViec as they don't expose their API (Using Playwright Stealth)
# [FUNCTION] Eliminate loading unnecessary resources (image, stylesheet, font, etc.)
async def apply_speedup(page):
    async def intercept(route):
        try:
            if page.is_closed():
                return
            
            # Allow essential document and scripts for fingerprinting/human-like behavior
            if route.request.resource_type in ["document", "script"]:
                await route.continue_()
                return 
            
            # Block heavy non-essentials to prevent hanging requests [cite: 2026-01-09]
            if route.request.resource_type in ["image", "media", "font"]:
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            pass
            
    # FIX: unroute_all supports 'behavior', unroute does not
    await page.unroute_all(behavior='ignoreErrors') 
    await page.route("**/*", intercept)

# [FUNCTION] Extract jobs information from a job card in ITviet website
async def extract_card_data(card):
  data = await card.eval_on_selector(
          "h3[data-search--job-selection-target='jobTitle']",
          "el => ({ title: el.innerText, url: el.getAttribute('data-url') })"
      )
  # Search for elements that contain specific data
  company_task = card.query_selector("a.text-rich-grey")
  location_task = card.query_selector("div.text-rich-grey[title]")
  date_task = card.query_selector(".small-text.text-dark-grey")
  skills_task = card.query_selector_all(".itag")
  skills_tooltip_task = card.query_selector("div[data-bs-original-title]")

  company_el, location_el, date_el, skill_elements, skill_tooltip = await asyncio.gather(
        company_task, location_task, date_task, skills_task, skills_tooltip_task
    )

  # Get company name
  company = await company_el.inner_text() if company_el else "N/A"

  # Get location
  location = await location_el.get_attribute("title") if location_el else "N/A"

  # Get skills
  skills = await asyncio.gather(*[s.inner_text() for s in skill_elements])
  
  # Get skills in tooltip
  skill_tooltip = await skill_tooltip.get_attribute("data-bs-original-title") if skill_tooltip else ""
  skill_tooltip_cleaned = [x.strip() for x in skill_tooltip.split(",")] if skill_tooltip else []

  # Combine skills prevent duplicate and clear out trash data
  final_skill = list(set(skills + skill_tooltip_cleaned))
  final_skill =  [s for s in final_skill if not s.startswith('+')]
  
  # Get date
  rawtext = await date_el.inner_text() if date_el else "N/A"
  rawtext = rawtext.replace('\n', ' ').strip()
  days_match = re.search(r'\d+', rawtext)
  current_date = datetime.now()
  if days_match and 'hour' in rawtext.lower():
      actual_hours = int(days_match.group())
      postingdate = (current_date- timedelta(hours = actual_hours)).date()

  elif days_match and 'day' in rawtext.lower():
      actual_day = int(days_match.group())
      postingdate = (current_date - timedelta(days = actual_day)).date()

  elif days_match and 'minute' in rawtext.lower():
      actual_min = int(days_match.group())
      postingdate = (current_date - timedelta(minutes = actual_min)).date()

  else:
      postingdate = current_date.date()

  # Prepare data for hashing for job id
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
        "skills": final_skill
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

    browser = None
    context = None
    page = None

    try:
        # Initialize Playwright Stealth
        async with stealth.use_async(async_playwright()) as p:
              browser = await p.chromium.launch(
                 headless=True,
                 args=["--disable-blink-features=AutomationControlled",
                       "--no-sandbox",
                       "--disable-gpu",
                       "--disable-dev-shm-usage",
                       "--disable-extensions"])
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
                  logging.debug(f"[run_itviec_scraper] Processing page {page_count}...")
                  jobs_on_page = await extract_page_data_itviec(page)
                  all_jobs.extend(jobs_on_page)
                  logging.debug(f"[run_itviec_scraper] Processed {len(jobs_on_page)} job(s).")
  
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
                          logging.warning("[run_itviec_scraper] Timeout ! There might not be any page left to process.")
                          break
                  else:
                      logging.debug("[run_itviec_scraper] Complete.")
                      try:
                          await page.unroute_all(behavior='ignoreErrors')
                      except:
                          pass
                      break
                      
    except Exception as e:
       logging.error(f'[run_itviec_scraper] Error: {e}')
    finally:
      if page and not page.is_closed():
        try:
          await page.unroute_all(behavior='ignoreErrors')
        except Exception as e:
          pass
      if browser:
        await asyncio.sleep(0.5)
        await browser.close()
                    
    logging.debug(f"\n[run_itviec_scraper] A total of : {len(all_jobs)} job(s) have been processed.")
    return all_jobs

def process_all_at_once(list_of_dicts):
    formatted_input = json.dumps(list_of_dicts, ensure_ascii=False, indent=2)
    max_retries = 2

    models = ['arcee-ai/trinity-mini:free',
          'openai/gpt-oss-20b:free',
          'google/gemma-3-27b-it:free',
          'openai/gpt-oss-120b:free',
          'meta-llama/llama-3.3-70b-instruct:free',
          'arcee-ai/trinity-large-preview:free']

    # Initialize Groq LLM to relabel job title
    llm = ChatGroq(
        model_name="meta-llama/llama-4-scout-17b-16e-instruct",
        temperature=0,
        api_key= os.environ.get('GROQ_API_KEY')
    ).bind(response_format = {"type": "json_object"})

    for i in models:
       subllm = ChatOpenAI(
        api_key = os.environ.get('OPEN_ROUTER_API_KEY'),
        model_name=i,
        temperature = 0
       ).bind(response_format = {'type': 'json_object'})

       llm = llm.with_fallbacks([subllm])

    # Define structure of data and format that the AI model should output
    class JobCategorization(BaseModel):
        job_id: Union[str, int] = Field(description="The unique identifier for the job post provided in the input")
        job_label: Literal[
            "Data Analyst",
            "Data Engineer",
            "Data Scientist",
            "Other Data Job",
            "None"
        ] = Field(description="Best fit category.")

    # Define the final output structure as it is expect to process multiple title as once
    class JobCategorizationList(BaseModel):
      """A collection of categorized job postings."""
      jobs: List[JobCategorization] = Field(description = "All categorized jobs")

    # Define the expected JSON structure
    parser = JsonOutputParser(pydantic_object=JobCategorizationList)

    # Create Prompt to prevent error in categorizing titles
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Bạn là chuyên gia phân loại job dữ liệu.
    Chỉ xuất dữ liệu qua hàm JobCategorizationList
    KHÔNG giải thích, KHÔNG thêm text ngoài JSON.

    Hãy tuân thủ cấu trúc sau:
    {{format_instructions}}

    CHỈ SỬ DỤNG các nhãn sau: "Data Analyst", "Data Engineer", "Data Scientist", "Other Data Job", "None".
    KHÔNG ĐƯỢC SỬ DỤNG nhãn khác trong bất cứ trường hợp nào

    QUY TẮC:
    1. "Data Scientist": AI/ML Engineer, Computer Vision, NLP, LLM. (Trừ Manager/Head), Thị giác máy tính.
    2. "Data Engineer": Data/Database Engineer, Architect, SQL Dev, DBA hệ thống.
    3. "Data Analyst": BI, Data Analyst.
    LƯU Ý RIÊNG CHO BA: 'Business Analyst' CHỈ được chọn là "Data Analyst" nếu tiêu đề có chứa ÍT NHẤT MỘT công cụ kỹ thuật: SQL, Tableau, PowerBI, Python, hoặc R. Nếu tiêu đề chỉ có chữ "Phân tích" hoặc "Analytics" chung chung mà không có công cụ, hãy xếp vào "None".
    4. "Other Data Job": Manager/Head/Director Data, Data Governance, Quality, Reviewer, Coordinator.
    5. "None": BA (Business Analyst) đơn thuần hoặc job không liên quan dữ liệu. Các vị trí thuần developer như full stack, .NET, Java developers nếu không có chữ 'data' hoặc 'dữ liệu' hoặc 'AI' thì đều là None

    VÍ DỤ OUTPUT MẪU (TUÂN THỦ TUYỆT ĐỐI):
    {{
      "jobs": [
        {{ "job_id": "id_1", "job_label": "Data Scientist" }},
        {{ "job_id": "id_2", "job_label": "Data Engineer" }}
      ]
    }}"""),
        ("user", "Hãy phân loại danh sách công việc sau, đảm bảo output đầy đủ và đúng định dạng JSON:\n{input_data}")
    ])

    # Create the chain that guarantees JSON output
    chain = prompt | llm | parser

    for attempt in range(max_retries+1):
      try:
        result = chain.invoke({"input_data": formatted_input,
                               "format_instructions": parser.get_format_instructions()})
        data = result.get('jobs')
        if not data or len(list_of_dicts) != len(data):
          raise ValueError('Incomplete outputs !')

        return data
      except Exception as e:
        logging.warning(f'[process_all_at_once] Atempt {attempt + 1} failed. Error: {e}')

        if attempt < max_retries:
          if '429' in str(e) or 'rate limit' in str(e).lower():
            logging.info(f'[process_all_at_once] Retrying after hitting rate limit. Attempt {attempt + 1} of {max_retries}.')
            time.sleep(20) # Wait 20 seconds before retrying
          else:
            time.sleep(5**attempt)
          continue
        else:
          logging.error(f'[process_all_at_once] Unable to retrieve result from AI, error: {e}')
          return [{'job_id': i['job_id'], 'job_label': 'None'} for i in list_of_dicts]

def sending_data_ai(total_ai_input: List[Dict]) -> Dict:
  """Sending data in batches to AI for relabeling (Base delay each batch is 30s)
  Args:
    total_ai_input (list of dicts): list of dicts with key as job id and value as job title

  Returns:
    labeled_data (dict): a dict with key as job id and data as clean label from AI model
  """
  def chunking(data: List[Dict], max_size: int = 30) -> List[List[Dict[str, str]]]:
    """Seperate data into chunks
    Args:
      data (list of dicts): list of dicts with key as job id and value as job title
      max_size (int): maximum size for each batch

    Returns:
      batch (list of lists): list contain many smaller lists that has maximum size as 30 records each
    """
    batch = []
    total = len(data)
    i = 0

    while i < total:
      batch.append(data[i: i + max_size])
      i += max_size

    return batch

  chunked_request = chunking(total_ai_input)
  labeled_data = []
  for i in chunked_request:
    labeled_data.extend(process_all_at_once(i))
    time.sleep(30)
  labeled_data_dict = {i.get('job_id'): i.get('job_label') for i in labeled_data}
  return labeled_data
