{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/Quanglee369/Scraping-jobs/blob/testing-branch/Extracting_new_website.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "import requests\n",
        "import json\n",
        "import pandas as pd\n",
        "import random\n",
        "import asyncio\n",
        "from datetime import timedelta, datetime\n",
        "import hashlib\n",
        "import aiohttp\n",
        "import re\n",
        "from selectolax.parser import HTMLParser\n",
        "from bs4 import BeautifulSoup\n",
        "from master_config import html_scraping_dict, api_scraping_dict, jd_dict_selector, all_skills, en_keywords, vn_keywords, mh_words,nega_keyword\n",
        "from typing import List, Union, Dict, Any\n",
        "import hashlib\n",
        "import reverse_geocoder as rg\n"
      ],
      "metadata": {
        "id": "zHpFLggqMkIF"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "!pip install selectolax\n",
        "!pip install reverse_geocoder"
      ],
      "metadata": {
        "id": "wf2XXkl9TnNa"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "#Standardize Fetch JD function"
      ],
      "metadata": {
        "id": "oVIcGdC54ypg"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "async def fetch_jd(session: aiohttp.ClientSession, item: Dict, platform: str, sem=asyncio.Semaphore(10))-> Dict:\n",
        "    \"\"\"Get job description info from links\n",
        "    Args:\n",
        "      session: aiohttp session\n",
        "      item (dict): a dict that contain info of a job\n",
        "      platform (str): name of the available platform\n",
        "      sem: asyncio.semaphore at 10 request each time to prevent api ban\n",
        "\n",
        "    Return:\n",
        "      final_jd_data (dict): a dict with key as job id and value as job description\n",
        "    \"\"\"\n",
        "\n",
        "    # Extract configuration based on platform\n",
        "    config = jd_dict_selector.get(platform)\n",
        "    if not config:\n",
        "      print(f'[fetch_jd] Plaform {platform} is not in the list {list(jd_dict_selector.keys())}')\n",
        "      return None\n",
        "    if not item:\n",
        "      print(f'[fetch_jd] Input data empty or invalid, data len: {len(item)}')\n",
        "      return {}\n",
        "\n",
        "    # Logic for CareerViet specific locale replacement\n",
        "    job_link_name = config.get('job_link_header')\n",
        "    job_desc = config.get('job_desc')\n",
        "    header = config.get('headers')\n",
        "    retry = 1\n",
        "\n",
        "    while retry < 4:\n",
        "      async with sem:\n",
        "        try:\n",
        "            job_id = item.get('job_id')\n",
        "            job_link = item.get(job_link_name)\n",
        "            if platform == 'careerviet':\n",
        "              job_link = job_link.replace('https://careerviet.vn/en/', 'https://careerviet.vn/vi/')\n",
        "            # Perform the request\n",
        "            response = await session.get(job_link, headers=header, timeout=10)\n",
        "\n",
        "            if response.status == 200:\n",
        "              break\n",
        "\n",
        "            elif response.status == 429:\n",
        "              print(f\"Error, too many request for platform {platform} link: {job_link} retry for {retry} time\")\n",
        "              await asyncio.sleep(2**retry)\n",
        "              retry += 1\n",
        "              continue\n",
        "\n",
        "            elif response.status != 200:\n",
        "                print(f\"Error, status code: {response.status} for platform: {platform} link: {job_link}\")\n",
        "                return {job_id: ''}\n",
        "\n",
        "        except Exception as e:\n",
        "            print(f'[fetch_jd_careerviet] Error when fetching link {job_link}, detail: {e}')\n",
        "            return {}\n",
        "\n",
        "    # Filter the JD from html response\n",
        "    jd_text = await response.text()\n",
        "    jdhtml = HTMLParser(jd_text)\n",
        "\n",
        "    clean_text = ''\n",
        "    # Iterate through the selectors to extract text\n",
        "    for i in jdhtml.css(job_desc):\n",
        "          clean_text += i.text(strip=True) + \" \"\n",
        "\n",
        "    return {job_id: clean_text.strip()}\n",
        "\n",
        "async def fetch_jd_mult(session: aiohttp.ClientSession, data: Dict[str, List]) -> Dict[str, List]:\n",
        "  \"\"\"This function is used to handle batch process of the fetch_jd function\n",
        "  Args:\n",
        "    session: aiohttp session\n",
        "    data (dict): a dict with key as platform name and value as list of dict that contain job information\n",
        "\n",
        "  Return:\n",
        "    final_data (dict): a dict with key as platform name and value as list of dict contain job description and its job id\n",
        "  \"\"\"\n",
        "  final_data = {}\n",
        "  for key, value in data.items():\n",
        "    task = [fetch_jd(session = session, item = single_val, platform = key) for single_val in value]\n",
        "    extracted_jd = await asyncio.gather(*task)\n",
        "    if key not in final_data.keys():\n",
        "      final_data[key] =  extracted_jd\n",
        "    else:\n",
        "      final_data[key].extend(extracted_jd)\n",
        "  return final_data"
      ],
      "metadata": {
        "id": "jc6XGy394gRP"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Standardize Fetch Job Headers Function (merge the total dict in the config file)"
      ],
      "metadata": {
        "id": "B-jdP7dQ4jNt"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "async def fetch_job_headers(session: aiohttp.ClientSession, keyword: str, page_num: int,platform: str) -> Dict[str, List]:\n",
        "    \"\"\"Scrap job information directly from website api\n",
        "    Args:\n",
        "      session: aiohttp session\n",
        "      keyword (str): name of the search keyword such as Data Analyst, Data Engineer, etc.\n",
        "      platform (str): name of the available platform\n",
        "      page_num (int): number of page\n",
        "\n",
        "    Return:\n",
        "      A dict with key as platform name and value as list of dict contain job data from website apis\n",
        "    \"\"\"\n",
        "\n",
        "    clean_platform = platform.lower().strip()\n",
        "\n",
        "    # Check to ensure platform name is valid\n",
        "    if clean_platform not in api_scraping_dict:\n",
        "        print(f'[fetch_job_headers] Keyword: {clean_platform} does not match any of the list {list(api_scraping_dict)}')\n",
        "        return None\n",
        "\n",
        "    config = api_scraping_dict[clean_platform]\n",
        "    url = config.get('url')\n",
        "    payload = config.get('payload')\n",
        "    params = {}\n",
        "    for key, value in payload.items():\n",
        "      if isinstance(value, str):\n",
        "        params[key] = value.format(keyword = keyword, page_num = page_num)\n",
        "      else:\n",
        "        params[key] = value\n",
        "    header = config.get('header')\n",
        "    retry = 1\n",
        "    data = {}\n",
        "\n",
        "    while retry < 4:\n",
        "      try:\n",
        "          # For vietnamworks api, using POST not GET\n",
        "          if clean_platform == 'vietnamworks':\n",
        "              async with session.post(url=url, json=params, headers=header) as response:\n",
        "                  if response.status == 200:\n",
        "                    data = await response.json()\n",
        "                    break\n",
        "                  elif response.status == 429:\n",
        "                    print(f'[fetch_job_headers] Too many request, proceed to retry {retry} time(s)')\n",
        "                    await asyncio.sleep(2 ** retry)\n",
        "                    retry += 1\n",
        "                    continue\n",
        "                  else:\n",
        "                    print(f'[fetch_job_headers] Can not get job data from api of platform: {platform}')\n",
        "                    break\n",
        "\n",
        "          else:\n",
        "              async with session.get(url=url, params=params, headers=header) as response:\n",
        "                  if response.status == 200:\n",
        "                      data = await response.json()\n",
        "                      break\n",
        "                  elif response.status == 429:\n",
        "                    print(f'[fetch_job_headers] Too many request, proceed to retry {retry} time(s)')\n",
        "                    await asyncio.sleep(2 ** retry)\n",
        "                    retry += 1\n",
        "                    continue\n",
        "                  else:\n",
        "                    print(f'[fetch_job_headers] Can not get job data from api of platform: {platform}')\n",
        "                    break\n",
        "\n",
        "      except Exception as e:\n",
        "          print(f'[fetch_job_headers] Can not get job headers for position {keyword}, Error: {e}')\n",
        "          return {}\n",
        "\n",
        "    if data and isinstance(data, dict):\n",
        "      return {clean_platform: data.get('data', [])}\n",
        "\n",
        "    return data"
      ],
      "metadata": {
        "id": "bFUmBoiy4Hjk"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Standardize Html Scraping function"
      ],
      "metadata": {
        "id": "2MUOP5Gw43N4"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "async def html_scraping(keyword: str, platform: str, page_num: int, session: aiohttp.ClientSession, sem =  asyncio.Semaphore(10)) -> List[Dict[str, Any]]:\n",
        "    \"\"\"Scrap job informations from html data\n",
        "    Args:\n",
        "      keyword (str): name of the search keyword such as Data Analyst, Data Engineer, etc.\n",
        "      platform (str): name of the available platform\n",
        "      page_num (int): number of page\n",
        "      session: aiohttp session\n",
        "      sem: asyncio.semaphore at 10 request each time to prevent api ban\n",
        "\n",
        "    Return:\n",
        "      final_data (list of dict): list of dict with key as platform name and value as a dict contain job information\n",
        "    \"\"\"\n",
        "\n",
        "    # 1. Config Retrieval & Validation\n",
        "    current_config = html_scraping_dict.get(platform)\n",
        "    if not current_config:\n",
        "        print(f\"Error: {platform} not found in config.\")\n",
        "        return []\n",
        "\n",
        "    # 2. Variable Preparation\n",
        "    url_template = current_config.get('url', '')\n",
        "    url = url_template.format(keyword=keyword)\n",
        "\n",
        "    raw_payload = current_config.get('payload', {})\n",
        "    params = {}\n",
        "    if isinstance(raw_payload, dict):\n",
        "        for k, v in raw_payload.items():\n",
        "            params[k] = str(v).format(keyword=keyword, page_num=page_num)\n",
        "\n",
        "    domain = current_config.get('domain', '')\n",
        "    selectors = current_config.get('selector', {})\n",
        "    headers = current_config.get('header', {})\n",
        "\n",
        "    final_data = {platform: []}\n",
        "    html_data = None  # 1. Initialize as None at the top\n",
        "    retry = 1\n",
        "\n",
        "    # 3. Network Request\n",
        "    while retry < 4:\n",
        "      async with sem:\n",
        "        try:\n",
        "          await asyncio.sleep(random.uniform(1.5, 4.0))\n",
        "          async with session.get(url, params=params, headers=headers, timeout=15) as response:\n",
        "              if response.status == 200:\n",
        "                html_data = await response.text()\n",
        "                break\n",
        "\n",
        "              elif response.status == 429:\n",
        "                print(f\"Too much request, retry {retry} time\")\n",
        "                retry += 1\n",
        "                await asyncio.sleep(2**retry)\n",
        "                continue\n",
        "\n",
        "              else:\n",
        "                print(f\"Failed to fetch {platform}: Status {response.status}, Keyword: {keyword}, Page_num: {page_num}\")\n",
        "                break\n",
        "\n",
        "        except Exception as e:\n",
        "            print(f\"Connection error for {platform}: {e}\")\n",
        "            return {platform: []}\n",
        "\n",
        "    if not html_data:\n",
        "        return final_data\n",
        "\n",
        "    # 4. Parsing Logic\n",
        "    parser = HTMLParser(html_data)\n",
        "    container_selector = selectors.get('container')\n",
        "\n",
        "    if not container_selector:\n",
        "        return []\n",
        "\n",
        "    for card in parser.css(container_selector):\n",
        "        # HELPER: Safe extraction to prevent NoneType crashes\n",
        "        def get_text(sel_key: str) -> str:\n",
        "            sel = selectors.get(sel_key)\n",
        "            if not sel or sel == \"None\":\n",
        "                return \"None\"\n",
        "            node = card.css_first(sel)\n",
        "            return node.text(strip=True) if node else \"None\"\n",
        "\n",
        "        def get_attr(sel_key: str, attr: str) -> str:\n",
        "            sel = selectors.get(sel_key)\n",
        "            if not sel or sel == \"None\":\n",
        "                return \"None\"\n",
        "            node = card.css_first(sel)\n",
        "            return node.attributes.get(attr, \"None\") if node else \"None\"\n",
        "\n",
        "        # Field Extraction\n",
        "        title = get_text('job_title')\n",
        "        employer = get_text('emp_name')\n",
        "        location = get_text('location_name')\n",
        "\n",
        "        # Link Logic (Handle relative vs absolute)\n",
        "        raw_href = get_attr('job_link', 'href')\n",
        "        full_link = raw_href if raw_href.startswith('http') or raw_href == \"None\" else f\"{domain}{raw_href}\"\n",
        "\n",
        "        # Created On Logic (Special Case: Attributes)\n",
        "        if platform == 'job123':\n",
        "            # Job123 stores date in data-time on the card itself\n",
        "            created = card.attributes.get('data-time', \"None\")\n",
        "        elif platform == 'careerlink':\n",
        "            # Careerlink stores it in an attribute of a specific span\n",
        "            created = get_attr('created_on', 'data-datetime')\n",
        "        else:\n",
        "            created = get_text('created_on')\n",
        "\n",
        "        # Job Description Logic\n",
        "        description = get_text('job_desc')\n",
        "\n",
        "        final_data[platform].append({\n",
        "            'job_id': hashlib.md5(full_link.encode('utf-8')).hexdigest(),\n",
        "            'job_title': title,\n",
        "            'emp_name': employer,\n",
        "            'location_name': location,\n",
        "            'job_link': full_link,\n",
        "            'created_on': created,\n",
        "            'job_desc': description\n",
        "        })\n",
        "\n",
        "    return final_data"
      ],
      "metadata": {
        "id": "nXAvbmnZzHgq"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Put the keywords list inside config file for filter_relevant"
      ],
      "metadata": {
        "id": "2Hj12sY9f5As"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def filter_relevant(item: List[Dict[str, any]], platform: str) -> List[Dict]:\n",
        "  \"\"\"Remove none-relevant jobs\n",
        "  Args:\n",
        "    data (list of dicts): List of dict with jobs information\n",
        "    platform (str): name of the platform, must exist in this list (careerviet, vietnamworks, itviec)\n",
        "\n",
        "  Returns:\n",
        "    filtered_job (list of dicts): The filtered list contain dicts with job information\n",
        "  \"\"\"\n",
        "\n",
        "  # Each platform have a different way of naming key values so createing a dict to mapping based on platform of choice\n",
        "  keys_for_platforms = {\n",
        "      'careerviet': ['job_title', 'job_id'],\n",
        "      'vietnamworks': ['jobTitle', 'jobId'],\n",
        "      'monster': ['cleanedJobTitle', 'jobId'],\n",
        "      'itviec': ['job_title', 'job_id'],\n",
        "      'topdev': ['title', 'job_id'],\n",
        "      'other': ['job_title', 'job_id']\n",
        "  }\n",
        "\n",
        "  # Check if platform is valid and exist in the predefined list\n",
        "  clean_platform = platform.strip().replace(' ', '').lower()\n",
        "\n",
        "  if clean_platform not in keys_for_platforms.keys():\n",
        "    print(f'Platform: {clean_platform} is not in the list: {keys_for_platforms.keys()} proceed with the option other')\n",
        "    clean_platform = 'other'\n",
        "\n",
        "  # Check if data is valid\n",
        "  if not item:\n",
        "    print('[filter_relevant] Data invalid !')\n",
        "    return []\n",
        "\n",
        "  job_title = keys_for_platforms.get(clean_platform)[0]\n",
        "\n",
        "  # Compile the regular expression (regex) pattern for higher efficiency in the loop\n",
        "  all_keywords = en_keywords + vn_keywords\n",
        "  pos_pattern = re.compile(r'\\b(' + '|'.join(all_keywords) + r')\\b', re.IGNORECASE)\n",
        "  neg_pattern = re.compile(r'\\b(' + '|'.join(nega_keyword) + r')\\b', re.IGNORECASE)\n",
        "  mh_pattern = re.compile(r'\\b(' + '|'.join(mh_words) + r')\\b', re.IGNORECASE)\n",
        "\n",
        "  # Processing data that match the predefined pattern\n",
        "  filtered_job = []\n",
        "\n",
        "  try:\n",
        "    for i in item:\n",
        "            raw_title = i.get(job_title)\n",
        "            if not raw_title:\n",
        "              return print(f'[filter_relevent] if the word other is used, this mean there is a platform with invalid keyword, if maybe there is not data at all')\n",
        "\n",
        "            # 1. Fix the \"Cake\" formatting (CamelCase to Spaces)\n",
        "            normalized_title = re.sub(r'([a-z])([A-Z])', r'\\1 \\2', raw_title)\n",
        "\n",
        "            # 2. Run your filters on the normalized title\n",
        "            is_must_have = mh_pattern.search(normalized_title)\n",
        "            is_positive = pos_pattern.search(normalized_title)\n",
        "            is_negative = neg_pattern.search(normalized_title)\n",
        "\n",
        "            if is_must_have and is_positive and not is_negative:\n",
        "                filtered_job.append(i)\n",
        "    return filtered_job\n",
        "\n",
        "  except Exception as e:\n",
        "    print(f'[filter_relevant] Unable to clean data and prepare for AI input, error detail: {e}')\n",
        "    return []\n",
        "\n",
        "def filter_relevant_mult(data: List[Dict[str, any]]) -> Dict[str, List]:\n",
        "  \"\"\"\n",
        "  This function is use to process batch data for filter_relevant function\n",
        "  Args:\n",
        "    data (list of dicts): list of dict containing platform name as key and job information as value\n",
        "\n",
        "  Return:\n",
        "    filtered_data (dict): a dict contain key as platform name and value as filtered value returned from filter_relevant function\n",
        "  \"\"\"\n",
        "  filtered_data = {}\n",
        "  for i in data:\n",
        "    for key, value in i.items():\n",
        "      if key not in filtered_data.keys():\n",
        "        filtered_data[key] = filter_relevant(item = value, platform = key)\n",
        "      else:\n",
        "        filtered_data[key].extend(filter_relevant(item = value, platform = key))\n",
        "  return filtered_data"
      ],
      "metadata": {
        "id": "ylNdsLRmMnV_"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "def remove_duplicate(item: List[Dict], platform: str, exist_job_id: pd.DataFrame) -> List[Dict]:\n",
        "  \"\"\" Remove duplicate jobs\n",
        "  args:\n",
        "    item (list of dicts): list of dict with jobs information\n",
        "    platform (str): name of the available platform\n",
        "    exist_job_id (dataframe): dataframe of job_id already exist in the database\n",
        "\n",
        "  returns:\n",
        "    cleaned (list of dict): list of dict with filtered jobs information\n",
        "    input_ai (list of dict): list of dict with job_id and job_title for AI labeling\n",
        "  \"\"\"\n",
        "\n",
        "  # Create a set of seen_id to remove the duplicates currently in the data\n",
        "  # Create a set of exist_id to remove the duplicate when compare with the database, use to reduce AI model payload\n",
        "  seen_id = set()\n",
        "  exist_id = set(exist_job_id['job_id'].astype(str)) if exist_job_id is not None and not exist_job_id.empty else set()\n",
        "\n",
        "  keys_for_platforms = {\n",
        "      'careerviet': ['job_title', 'job_id'],\n",
        "      'vietnamworks': ['jobTitle', 'jobId'],\n",
        "      'itviec': ['job_title', 'job_id'],\n",
        "      'monster': ['cleanedJobTitle', 'jobId'],\n",
        "      'topdev': ['title', 'job_id'],\n",
        "      'other': ['job_title', 'job_id']\n",
        "  }\n",
        "\n",
        "  # Intialize dict for labeling to reduce AI payload\n",
        "  label_mapping = {\n",
        "    \"dataanalyst\": \"Data Analyst\",\n",
        "    \"dataengineer\": \"Data Engineer\",\n",
        "    \"datascientist\": \"Data Scientist\",\n",
        "    \"aiengineer\": \"Data Scientist\",\n",
        "    \"mlengineer\": \"Data Scientist\"\n",
        "  }\n",
        "\n",
        "  # Check if platform is valid and exist in the predefined list\n",
        "  clean_platform = platform.strip().replace(' ', '').lower()\n",
        "\n",
        "  if clean_platform not in keys_for_platforms.keys():\n",
        "    print(f'[remove_duplicate] platform: {clean_platform} is not in the list: {keys_for_platforms.keys()} proceed with the option other')\n",
        "    clean_platform = 'other'\n",
        "\n",
        "  # Check if data is valid\n",
        "  if not item:\n",
        "    print('[remove_duplicate] Invalid job data')\n",
        "    return [], []\n",
        "  cleaned = []\n",
        "\n",
        "  job_title = keys_for_platforms.get(clean_platform)[0]\n",
        "  job_id = keys_for_platforms.get(clean_platform)[1]\n",
        "\n",
        "  pattern = re.compile('|'.join(re.escape(k) for k in label_mapping.keys()))\n",
        "\n",
        "  # Loop through each job, only return the one that is not yet exist in the list\n",
        "\n",
        "  for i in item:\n",
        "    raw_id = i.get(job_id)\n",
        "    if not raw_id:\n",
        "      return print('[remove_duplicate] if the word other is used, this mean there is a platform with invalid keyword, if maybe there is not data at all')\n",
        "    job_id_inner = str(raw_id) if raw_id is not None else None\n",
        "    if not job_id_inner or job_id_inner in seen_id:\n",
        "      continue\n",
        "\n",
        "    seen_id.add(job_id_inner)\n",
        "\n",
        "    # Also label job with explicit keyword to reduce AI payload\n",
        "    match_label = pattern.search(i.get(job_title).lower().replace(' ', ''))\n",
        "\n",
        "    if match_label:\n",
        "      i['label'] = label_mapping[match_label.group(0)]\n",
        "    else:\n",
        "      i['label'] = ''\n",
        "    cleaned.append(i)\n",
        "\n",
        "  input_ai = [{'job_id': i.get(job_id), 'job_title': i.get(job_title)} for i in cleaned if i.get('label') == '' and str(i.get(job_id)) not in exist_id]\n",
        "  return cleaned, input_ai\n",
        "\n",
        "def remove_duplicate_multi(data: Dict[str, List], exist_job_id: pd.DataFrame) -> Dict[str, List]:\n",
        "  \"\"\"This function is used for handle batch process of remove_duplicate function\n",
        "  Args:\n",
        "    data (dict): a dict contain key as platform namd and value as list of dict containing job info\n",
        "    exist_job_id (dataframe): dataframe of already exist job id in the database\n",
        "\n",
        "  Return:\n",
        "    nondupdata (dict): a dict with key as platform name and value as list of dicts contain job information\n",
        "    input_ai (dict): a dict with key as platform name and value as list of dicts contain job id and job title for AI relabel\n",
        "  \"\"\"\n",
        "  nondupdata = {}\n",
        "  input_ai = {}\n",
        "\n",
        "  for key, value in data.items():\n",
        "    filtered_data = remove_duplicate(item = value, platform=key, exist_job_id= exist_job_id)\n",
        "    if key not in nondupdata.keys() and key not in input_ai.keys():\n",
        "      nondupdata[key] = filtered_data[0]\n",
        "      input_ai[key] = filtered_data[1]\n",
        "    else:\n",
        "      nondupdata[key].extend(filtered_data[0])\n",
        "      input_ai[key].extend(filtered_data[1])\n",
        "  return nondupdata, input_ai"
      ],
      "metadata": {
        "id": "pWPABeNaNYKr"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Put the skill list inside config file for extract_skills_from_jd"
      ],
      "metadata": {
        "id": "_yvQRHB4fqyV"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def extract_skills_from_jd(job_list: List[Dict[str, str]]) -> List[Dict[str, List]]:\n",
        "  \"\"\"Extract skills from Job Descriptions\n",
        "  Args:\n",
        "    job_list (list of dict): list of dict with Job Id and Job Descriptions to extract skills\n",
        "\n",
        "  Returns:\n",
        "    results (list of dict): list of dict with Job Id and skill names as list\n",
        "  \"\"\"\n",
        "  # Descending sort order so this prioritize longer word over shorter one (example: Power BI -  matching Power first before looking at BI)\n",
        "  all_skills.sort(key=len, reverse=True)\n",
        "  pattern = re.compile(r'\\b(' + '|'.join(map(re.escape, all_skills)) + r')\\b', re.IGNORECASE)\n",
        "\n",
        "  results = []\n",
        "  if not job_list or not isinstance(job_list,list):\n",
        "    return results\n",
        "\n",
        "  for job_entry in job_list:\n",
        "      if not job_entry:\n",
        "        continue\n",
        "      for job_id, jd in job_entry.items():\n",
        "           if not jd or jd == 'None':\n",
        "              results.append({'job_id': job_id, 'skills': []})\n",
        "              continue\n",
        "\n",
        "           # Find all matching key word in jd\n",
        "           found_skills = pattern.findall(jd)\n",
        "\n",
        "           # Standardize by removing duplicate and normalizing the words\n",
        "           unique_skills = sorted(list(set([s.strip() for s in found_skills])))\n",
        "\n",
        "           results.append({'job_id': job_id, 'skills': unique_skills})\n",
        "\n",
        "  return results\n",
        "\n",
        "\n",
        "def extract_skills_from_jd_mult(data: Dict[str, List[Dict[str, str]]])-> Dict[str, List[Dict[str, List]]]:\n",
        "  \"\"\"This function is use to handle batch process of the function\n",
        "  Args:\n",
        "    data (dict of list): dict of list with key as platform name, the list contain dicts with key as job id and value as job desc\n",
        "\n",
        "  Returns:\n",
        "    final_result (dict of list): dict of list with key as platform name, the list contain dict with key as job id and value as skill list\n",
        "  \"\"\"\n",
        "  final_result = {}\n",
        "  for key, value in data.items():\n",
        "    if key not in final_result.keys():\n",
        "      final_result[key] = extract_skills_from_jd(job_list = value)\n",
        "    else:\n",
        "      final_result[key].extend(extract_skills_from_jd(job_list = value))\n",
        "  return final_result"
      ],
      "metadata": {
        "id": "DHdlwFy_Oq65"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "async def process_none_student_job( item: Dict, session: aiohttp.ClientSession,sem = asyncio.Semaphore (10))-> Dict:\n",
        "  \"\"\"Get location, created date specifically for student job site\n",
        "  Args:\n",
        "    item (dict): a dict containing job info for a job\n",
        "    session: aiohttp session\n",
        "    sem: asyncio.semaphore at 10 request each time to prevent api ban\n",
        "\n",
        "  Returns:\n",
        "    jd_data_text (dict): a dict containing job info for a job\n",
        "\n",
        "  \"\"\"\n",
        "  lat = jd_dict_selector.get('studentjob').get('lat')\n",
        "  header =  jd_dict_selector.get('studentjob').get('headers')\n",
        "  created_on = jd_dict_selector.get('studentjob').get('created_on')\n",
        "  link = item.get('job_link')\n",
        "\n",
        "  jd_data_text = None\n",
        "\n",
        "  retry = 1\n",
        "  while retry < 4:\n",
        "    async with sem:\n",
        "      try:\n",
        "        async with await session.get(link, headers = header, timeout = 10) as jd_data:\n",
        "          if jd_data.status == 200:\n",
        "            jd_data_text = await jd_data.text()\n",
        "            break\n",
        "\n",
        "          elif jd_data.status == 429:\n",
        "            print(f'Too much request for link: {link} proceed to retry: {retry} time(s)')\n",
        "            retry += 1\n",
        "            await asyncio.sleep(2**retry)\n",
        "            continue\n",
        "\n",
        "          elif jd_data.status != 200:\n",
        "            print(f'Error when retrieving data for link: {link} status code: {jd_data.status}')\n",
        "            return item\n",
        "      except Exception as e:\n",
        "        print(f\"Connection error: unable to access link: {link} error: {e}\")\n",
        "        return item\n",
        "\n",
        "  if not jd_data_text:\n",
        "        return item\n",
        "\n",
        "  parser = HTMLParser(jd_data_text)\n",
        "  for i in parser.css(created_on):\n",
        "    text_data = i.text(strip=True)\n",
        "    if \"/\" in text_data and len(text_data) == 10:\n",
        "      item['created_on'] = text_data\n",
        "      break\n",
        "\n",
        "  try:\n",
        "      nodes = parser.css(lat)\n",
        "      if len(nodes) >= 2:\n",
        "          lat_val = float(nodes[0].attributes.get('value', 0))\n",
        "          lon_val = float(nodes[1].attributes.get('value', 0))\n",
        "          # Perform offline reverse geocoding\n",
        "          item['location_name'] = rg.search((lat_val, lon_val))[0]['admin1']\n",
        "  except Exception as e:\n",
        "      print(f\"Geocoding failed for {link}: {e}\")\n",
        "\n",
        "  return item\n",
        "\n",
        "async def process_none_student_job_mult(session, data: Dict[str, List])-> Dict[str, List]:\n",
        "  \"\"\"This function is use to handle batch process of the function process_none_student_job\n",
        "  Args:\n",
        "    session: aiohttp session\n",
        "    data (dict of list): dict of list that contain info of jobs\n",
        "\n",
        "  Returns:\n",
        "    final_result (dict of list): dict of list that contain info of jobs\n",
        "\n",
        "  \"\"\"\n",
        "  final_result = {}\n",
        "  for key, value in data.items():\n",
        "    task = [process_none_student_job(session = session, item = i) for i in value]\n",
        "    extracted_data = await asyncio.gather(*task)\n",
        "    if key not in final_result.keys():\n",
        "      final_result[key] = extracted_data\n",
        "    else:\n",
        "      final_result[key].append(extracted_data)\n",
        "  return final_result\n"
      ],
      "metadata": {
        "id": "aKaWRUJPNtFm"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Get data from apis\n",
        "api_site = list(i for i in api_scraping_dict.keys() if i not in ('topdev'))\n",
        "keyword_list = ['data analyst', 'data engineer', 'data scientist', 'ai', 'sql']\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  task = [fetch_job_headers(session = session, keyword = keyword, platform=i, page_num = 1) for i in api_site for keyword in keyword_list]\n",
        "  api_data = await asyncio.gather(*task)"
      ],
      "metadata": {
        "id": "LG2HRTo_98Se"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Get data from apis for topdev\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  task_topdev = [fetch_job_headers(session = session, keyword = keyword, platform='topdev', page_num = num) for i in api_site for keyword in keyword_list for num in range(1, 11)]\n",
        "  api_data_topdev = await asyncio.gather(*task_topdev)"
      ],
      "metadata": {
        "id": "e0LMUzxvehsg"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Get api data for topdev website\n",
        "api_data_topdev_noblank = {}\n",
        "for i in api_data_topdev:\n",
        "  for key, value in i.items():\n",
        "    if key not in api_data_topdev_noblank.keys():\n",
        "      api_data_topdev_noblank[key] = value\n",
        "    else:\n",
        "      api_data_topdev_noblank[key].extend(value)\n",
        "\n",
        "for i in api_data_topdev_noblank.get('topdev'):\n",
        "  i['job_id'] = hashlib.md5(i.get('detail_url').encode('utf-8')).hexdigest()"
      ],
      "metadata": {
        "id": "qz8DyNyPeohU"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "filtered_topdev = filter_relevant(api_data_topdev_noblank.get('topdev'), platform = 'topdev')"
      ],
      "metadata": {
        "id": "Vip-7HmVnfdk"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "filtered_data = filter_relevant_mult(api_data)"
      ],
      "metadata": {
        "id": "vcQ8jUOe_Ios"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "cleaned_data, input_ai = remove_duplicate_multi(filtered_data, exist_job_id= pd.DataFrame())"
      ],
      "metadata": {
        "id": "DsDzZOPbX6w4"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "cleaned_data_topdev, input_ai_topdev = remove_duplicate(filtered_topdev, platform = 'topdev',exist_job_id = pd.DataFrame())"
      ],
      "metadata": {
        "id": "E52xYmLxs5DP"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "cv_data = {key : value for key, value in cleaned_data.items() if key == 'careerviet'}\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  cv_data_jd = await fetch_jd_mult(session = session, data = cv_data)\n",
        "  all_skill_cv = extract_skills_from_jd_mult(data = cv_data_jd)"
      ],
      "metadata": {
        "id": "uNwXpXcU9-2A"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "job_site = list(i for i in html_scraping_dict.keys())\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  task =  [html_scraping(session=session, keyword= keyword, platform= site, page_num=i) for keyword in keyword_list for site in job_site for i in range(1, 21)]\n",
        "  html_scrap_data = await asyncio.gather(*task)"
      ],
      "metadata": {
        "id": "zuVswB2J5qfU"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "final_data_html = {}\n",
        "\n",
        "for entry in html_scrap_data:\n",
        "  for platform, job_list in entry.items():\n",
        "    if platform not in final_data_html:\n",
        "      final_data_html[platform] = []\n",
        "\n",
        "    if isinstance(job_list, list):\n",
        "      for i in job_list:\n",
        "        if isinstance(i, list):\n",
        "          final_data_html[platform].extend(i)\n",
        "        elif isinstance(i, dict):\n",
        "          final_data_html[platform].append(i)\n",
        "    elif isinstance(job_list, dict):\n",
        "      final_data_html[platform].append(job_list)"
      ],
      "metadata": {
        "id": "AAkivXVWceYD"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "filtered_data_html = filter_relevant_mult(data = [final_data_html])"
      ],
      "metadata": {
        "id": "yjUsVZyxr6QS"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "cleaned_data_html, input_ai_html = remove_duplicate_multi(data = filtered_data_html, exist_job_id=pd.DataFrame())"
      ],
      "metadata": {
        "id": "2rxa8P2GtN-w"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "async with aiohttp.ClientSession() as session:\n",
        "  jd_data_html = await fetch_jd_mult(session = session, data=cleaned_data_html)"
      ],
      "metadata": {
        "id": "RWXfdkRqohkQ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "all_skill_html = extract_skills_from_jd_mult(jd_data_html)"
      ],
      "metadata": {
        "id": "al2uji8Shr2b"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "test_link = cleaned_data_html.get('studentjob')[100].get('job_link')\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  test_link_re = await session.get(test_link, headers = header)\n",
        "  test_link_raw = await test_link_re.text()"
      ],
      "metadata": {
        "id": "dCGcx_ymyIuZ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "total_html_data = []\n",
        "\n",
        "for key, value in cleaned_data_html.items():\n",
        "  if key != 'studentjob':\n",
        "    total_html_data.extend(value)\n"
      ],
      "metadata": {
        "id": "6u8YNVgR1Zyo"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "pd.DataFrame(total_html_data)['created_on'].unique()"
      ],
      "metadata": {
        "id": "pF-hOHLuLqdl"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "lB-WJMXEz-tM"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "student_job_none = {key: value for key, value in  cleaned_data_html.items() if key == 'studentjob'}\n",
        "async with aiohttp.ClientSession() as session:\n",
        "  student_job_fill = await process_none_student_job_mult(session = session, data=student_job_none)"
      ],
      "metadata": {
        "id": "4yvdDBl89iO8"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "async with aiohttp.ClientSession() as session:\n",
        "  test_text = await session.get('https://studentjob.vn/viec-lam/data-analyst-middle-senior-job817694')\n",
        "  text_text_raw = await test_text.text()"
      ],
      "metadata": {
        "id": "7aUosflE_3FX"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "pd.DataFrame(student_job_fill.get('studentjob'))['created_on'].value_counts()"
      ],
      "metadata": {
        "id": "6tewcxhqApHu"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "for i in parser.css(lat):\n",
        "  print(i.attributes)"
      ],
      "metadata": {
        "id": "0sCV6DKKBRQU"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "lat"
      ],
      "metadata": {
        "id": "nACKzTpHBAki"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "\n",
        "def get_province_offline(lat, lon):\n",
        "    results = rg.search((lat, lon)) # Returns a list\n",
        "    # 'admin1' usually corresponds to the Province/State\n",
        "    return results[0]['admin1']\n",
        "\n",
        "print(get_province_offline(10.8199135, 106.6987697))"
      ],
      "metadata": {
        "id": "csV6m_ng60g3"
      },
      "execution_count": null,
      "outputs": []
    }
  ],
  "metadata": {
    "colab": {
      "provenance": [],
      "include_colab_link": true
    },
    "kernelspec": {
      "display_name": "Python 3",
      "name": "python3"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 0
}