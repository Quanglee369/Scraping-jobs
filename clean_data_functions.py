import re
import logging
import copy
import ast
import dateparser
import pytz
from datetime import datetime
import pandas as pd
from master_config import all_skills, en_keywords, vn_keywords, mh_words,nega_keyword, keys_for_platforms, api_data_cols
from sqlalchemy import text
from typing import List, Union, Dict, Any


def filter_relevant(item: List[Dict[str, Any]], platform: str) -> List[Dict]:
  """Remove none-relevant jobs
  Args:
    data (list of dicts): List of dict with jobs information
    platform (str): name of the platform, must exist in this list (careerviet, vietnamworks, itviec)

  Returns:
    filtered_job (list of dicts): The filtered list contain dicts with job information
  """

  # Check if platform is valid and exist in the predefined list
  clean_platform = platform.strip().replace(' ', '').lower()

  if clean_platform not in keys_for_platforms.keys():
    print(f'[filter_relevant] Platform: {clean_platform} is not in the list: {keys_for_platforms.keys()} proceed with the option other')
    clean_platform = 'other'

  # Check if data is valid
  if not item:
    logging.error('[filter_relevant] Data invalid !')
    return []

  job_title = keys_for_platforms.get(clean_platform)[0]

  # Compile the regular expression (regex) pattern for higher efficiency in the loop
  all_keywords = en_keywords + vn_keywords
  pos_pattern = re.compile(r'\b(' + '|'.join(all_keywords) + r')\b', re.IGNORECASE)
  neg_pattern = re.compile(r'\b(' + '|'.join(nega_keyword) + r')\b', re.IGNORECASE)
  mh_pattern = re.compile(r'\b(' + '|'.join(mh_words) + r')\b', re.IGNORECASE)

  # Processing data that match the predefined pattern
  filtered_job = []

  try:
    for i in item:
            raw_title = i.get(job_title)
            if not raw_title:
              logging.warning(f'[filter_relevent] if the word other is used, this mean there is a platform with invalid keyword, if maybe there is no data at all')
              continue

            # 1. Fix the "Cake" formatting (CamelCase to Spaces)
            normalized_title = re.sub(r'([a-z])([A-Z])', r'\1 \2', raw_title)

            # 2. Run your filters on the normalized title
            is_must_have = mh_pattern.search(normalized_title)
            is_positive = pos_pattern.search(normalized_title)
            is_negative = neg_pattern.search(normalized_title)

            if is_must_have and is_positive and not is_negative:
                filtered_job.append(i)
    return filtered_job

  except Exception as e:
    logging.error(f'[filter_relevant] Unable to clean data and prepare for AI input, error detail: {e}')
    return []

def filter_relevant_mult(data: List[Dict[str, Any]]) -> Dict[str, List]:
  """
  This function is use to process batch data for filter_relevant function
  Args:
    data (list of dicts): list of dict containing platform name as key and job information as value

  Return:
    filtered_data (dict): a dict contain key as platform name and value as filtered value returned from filter_relevant function
  """
  filtered_data = {}
  for i in data:
    for key, value in i.items():
      if key not in filtered_data.keys():
        filtered_data[key] = filter_relevant(item = value, platform = key)
      else:
        filtered_data[key].extend(filter_relevant(item = value, platform = key))
  return filtered_data

def remove_duplicate(item: List[Dict], platform: str, exist_job_id: pd.DataFrame) -> List[Dict]:
  """Remove duplicate jobs
  Args:
    item (list of dicts): list of dict with jobs information
    platform (str): name of the available platform
    exist_job_id (dataframe): dataframe of job_id already exist in the database

  Returns:
    cleaned (list of dict): list of dict with filtered jobs information
    input_ai (list of dict): list of dict with job_id and job_title for AI labeling
  """

  # Create a set of seen_id to remove the duplicates currently in the data
  # Create a set of exist_id to remove the duplicate when compare with the database, use to reduce AI model payload
  seen_id = set()
  exist_id = set(exist_job_id['job_id'].astype(str)) if exist_job_id is not None and not exist_job_id.empty else set()


  # Intialize dict for labeling to reduce AI payload
  label_mapping = {
    "dataanalys": "Data Analyst",
    "dataengineer": "Data Engineer",
    "databaseadmin": "Data Engineer",
    "databaseengineer": "Data Engineer",
    "datascientist": "Data Scientist",
    "aiengineer": "Data Scientist",
    "mlengineer": "Data Scientist",
    "aidevelop": "Data Scientist",
    "aiarchitect": "Data Scientist",
    "quảnlýdữliệu": "Data Engineer",
    "quảntrịdữliệu": "Data Engineer",
    "dataarchitect": "Data Engineer"
  }

  # Check if platform is valid and exist in the predefined list
  clean_platform = platform.strip().replace(' ', '').lower()

  if clean_platform not in keys_for_platforms.keys():
    print(f'[remove_duplicate] platform: {clean_platform} is not in the list: {keys_for_platforms.keys()} proceed with the option other')
    clean_platform = 'other'

  # Check if data is valid
  if not item:
    print('[remove_duplicate] Invalid job data')
    return [], []
  cleaned = []

  job_title = keys_for_platforms.get(clean_platform)[0]
  job_id = keys_for_platforms.get(clean_platform)[1]

  pattern = re.compile('|'.join(re.escape(k) for k in label_mapping.keys()))

  # Loop through each job, only return the one that is not yet exist in the list

  for i in item:
    raw_id = i.get(job_id)
    if not raw_id:
      logging.warning('[remove_duplicate] if the word other is used, this mean there is a platform with invalid keyword, if maybe there is no data at all')
      continue
    job_id_inner = str(raw_id) if raw_id is not None else None
    if not job_id_inner or job_id_inner in seen_id:
      continue

    seen_id.add(job_id_inner)

    # Also label job with explicit keyword to reduce AI payload
    match_label = pattern.search(i.get(job_title).lower().replace(' ', ''))

    if match_label:
      i['label'] = label_mapping[match_label.group(0)]
    else:
      i['label'] = ''
    cleaned.append(i)

  input_ai = [{'job_id': i.get(job_id), 'job_title': i.get(job_title)} for i in cleaned if i.get('label') == '' and str(i.get(job_id)) not in exist_id]
  return cleaned, input_ai

def remove_duplicate_multi(data: Dict[str, List], exist_job_id: pd.DataFrame) -> Dict[str, List]:
  """This function is used for handle batch process of remove_duplicate function
  Args:
    data (dict): a dict contain key as platform namd and value as list of dict containing job info
    exist_job_id (dataframe): dataframe of already exist job id in the database
  Return:
    nondupdata (dict): a dict with key as platform name and value as list of dicts contain job information
    input_ai (dict): a dict with key as platform name and value as list of dicts contain job id and job title for AI relabel
  """
  nondupdata = {}
  input_ai = {}

  for key, value in data.items():
    filtered_data = remove_duplicate(item = value, platform=key, exist_job_id= exist_job_id)
    if key not in nondupdata.keys() and key not in input_ai.keys():
      nondupdata[key] = filtered_data[0]
      input_ai[key] = filtered_data[1]
    else:
      nondupdata[key].extend(filtered_data[0])
      input_ai[key].extend(filtered_data[1])
  return nondupdata, input_ai

def extract_skills_from_jd(job_list: List[Dict[str, str]]) -> List[Dict[str, List]]:
  """Extract skills from Job Descriptions
  Args:
    job_list (list of dict): list of dict with Job Id and Job Descriptions to extract skills

  Returns:
    results (list of dict): list of dict with Job Id and skill names as list
  """
  # Descending sort order so this prioritize longer word over shorter one (example: Power BI -  matching Power first before looking at BI)
  all_skills.sort(key=len, reverse=True)
  pattern = re.compile(r'\b(' + '|'.join(map(re.escape, all_skills)) + r')\b', re.IGNORECASE)

  results = []
  if not job_list or not isinstance(job_list,list):
    return results

  for job_entry in job_list:
      if not job_entry:
        continue
      for job_id, jd in job_entry.items():
           if not jd or jd == 'None':
             results.append({'job_id': job_id, 'skills': []})
             continue

           # Find all matching key word in jd
           found_skills = pattern.findall(jd)

           # Standardize by removing duplicate and normalizing the words
           unique_skills = sorted(list(set([s.strip() for s in found_skills])))

           results.append({'job_id': job_id, 'skills': unique_skills})

  return results

def extract_skills_from_jd_mult(data: Dict[str, List[Dict[str, str]]])-> Dict[str, List[Dict[str, List]]]:
  """This function is use to handle batch process of the function
  Args:
    data (dict of list): dict of list with key as platform name, the list contain dicts with key as job id and value as job desc

  Returns:
    final_result (dict of list): dict of list with key as platform name, the list contain dict with key as job id and value as skill list
  """
  final_result = {}
  for key, value in data.items():
    if key not in final_result.keys():
      final_result[key] = extract_skills_from_jd(job_list = value)
    else:
      final_result[key].extend(extract_skills_from_jd(job_list = value))
  return final_result

def fill_label(data: List[Dict], label_data: Dict) -> List[Dict]:
  """Fill label results from AI output
  Args:
    data (list of dict): list of dict with job information
    label_data (dict): a dict with job id as key and job label result from AI as value
  Returns:
    temp_date (list of dict): list of dict with job information (labeled)
  """

  if not data:
    print(f'[fill_label] data invalid')
    return []

  if not label_data:
    print(f'[fill_label] label data invalid for proceed with blank label')
    label_data = {}

  temp_data = copy.deepcopy(data)
  for i in temp_data:
    job_id = i.get('job_id')
    label = i.get('label')
    if job_id in label_data:
      i['label'] = label_data.get(job_id)
    elif label == '':
      i['label'] = 'None'
  return temp_data

def fast_remove_accents(text: str)-> str:
    """Remove accents and convert Vietnamese into ASCII-friendly format
    args:
      text (str): String need to be processed
    returns:
      str: cleaned text
    """
    if not isinstance(text, str):
        return ""

    # Using a dictionary to ensure 1-to-1 mapping
    map_dict = {
        "àáảãạăằắẳẵặâầấẩẫậ": "aaaaaaaaaaaaaaaaa",
        "èéẻẽẹêềếểễệ": "eeeeeeeeeee",
        "ìíỉĩị": "iiiii",
        "òóỏõọôồốổỗộơờớởỡợ": "ooooooooooooooooo",
        "ùúủũụưừứửữự": "uuuuuuuuuuu",
        "ỳýỷỹỵ": "yyyyy",
        "đ": "d",
        "ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬ": "AAAAAAAAAAAAAAAAA",
        "ÈÉẺẼẸÊỀẾỂỄỆ": "EEEEEEEEEEE",
        "ÌÍỈĨỊ": "IIIII",
        "ÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢ": "OOOOOOOOOOOOOOOOO",
        "ÙÚỦŨỤƯỪỨỬỮỰ": "UUUUUUUUUUU",
        "ỲÝỶỸỴ": "YYYYY",
        "Đ": "D"
    }

    # Create strings from dictionary keys and values
    intab = "".join(map_dict.keys())
    outtab = "".join(map_dict.values())

    trantab = str.maketrans(intab, outtab)
    return text.translate(trantab).strip()

def location_norm(loc: Any, pattern) -> str:
  """Convert location name into string, ASCII friendly format
  Args:
    loc (any): can be list of string contain location name
    pattern = regex pattern to match 

  Returns:
    location name (string): cleaned location name as string
  """
  if not loc:
    return 'Other'
  
  if isinstance(loc, list):
    loc = fast_remove_accents(', '.join(loc).strip())
  elif isinstance(loc, str):
    loc = fast_remove_accents(loc)
  else:
     return 'Other'
    
  result = pattern.findall(loc.strip())
  unique_results = list(dict.fromkeys(result))

  if len(unique_results) > 1:
    return(', '.join(unique_results))
  elif len(unique_results) == 1:
    return(unique_results[0])
  else:
    return('Other')

def prep_data_dim(data: pd.DataFrame, collist: List) -> List[pd.DataFrame]:
  """Prepare data to update dim tables in database
  args:
    data (dataframe): clean dataframe of job data need to be processed
    collist (list): list of column need to extract from dataframe
  results:
    list of dataframes: list of dataframes respective to the collist
  """
  if data.empty or not collist or len(collist) == 0:
    logging.error('[prep_data_dim] Input data or columns list is not valid')
    return None
  results = []
  for i in collist:
    unique_vals = data[i].dropna().unique()
    if i in ['date_view', 'last_seen']:
      results.append(pd.DataFrame(unique_vals, columns=['actual_date']))
    else:
      results.append(pd.DataFrame(unique_vals, columns = [i]))
  return results

def universal_date_cleaner(date_val: Any, job_link: str)-> datetime:
  """Convert date from multipe format
  Args:
    date_val (any): date data in various format from string to unix time (float)
    job_link (str): link of the postings
  Returns:
    return converted datetime data
  """

  vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
 
  none_date = datetime(1990, 10, 10, tzinfo=pytz.UTC)
  dmy_sites = {'studentjob.vn', 'topdev.vn'}

  # Check if any of the DMY sites are in the link
  if any(site in str(job_link).lower() for site in dmy_sites):
      date_order = 'DMY'
  else:
      date_order = 'YMD'

  if pd.isna(date_val) or date_val == 'None':
      return none_date

  if isinstance(date_val, (int, float)):
      unit = 'ms' if len(str(int(date_val))) == 13 else 's'
      return pd.to_datetime(date_val, unit=unit, utc=True)

  settings = {'RELATIVE_BASE': datetime.now(vn_tz),
              'DATE_ORDER': date_order
              }

  dt = dateparser.parse(str(date_val), settings= settings)
  if not dt:
      return none_date

  if dt.tzinfo is None:
      return vn_tz.localize(dt).astimezone(pytz.UTC)

  return dt.astimezone(pytz.UTC)

def merge_df_master(data: List[Any]) -> pd.DataFrame:
  """Merge all dicts and lists into a unify dataframe
  Args:
    data (list): a list contain all final data, could be dict or list

  Returns:
    df (dataframe): a dataframe of unify all extracted data
  """
  df_list = []
  standard_cols = api_data_cols.get('other')
  df = pd.DataFrame(columns =  standard_cols)
  for i in data:
    try:
      if isinstance(i, dict):
        for key, value in i.items():
          required_cols_dict = api_data_cols.get(key)
          if not required_cols_dict:
            print(f'[merge_df_master] keyword {key} not in api_data_cols keys: {list(api_data_cols.keys())} proceed with other option')
            required_cols_dict = api_data_cols.get('other')

          temp_df = pd.DataFrame(value).loc[:, required_cols_dict]
          temp_df.columns = standard_cols
          df_list.append(temp_df)
      else:
          temp_df = pd.DataFrame(i).loc[:, standard_cols]
          df_list.append(temp_df)
    except Exception as e:
      print(f'Unable to process data, error, {e}')

  df = pd.concat(df_list, axis=0, ignore_index=True)

  return df

def safe_literal_eval(val):
    try:
        if isinstance(val, str) and val.startswith('['):
            return ast.literal_eval(val)
        return val
    except:
        return []
    
def sync_fact_job_postings(df: pd.DataFrame, engine)-> None:
    """Update fact_job_postings table in database
    Args:
      df (dataframe): dataframe of job information
      engine (sqlalchemy object): engine to connect and interact with database
    Returns:
      None
    """
    if df.empty:
        return

    target_table = "fact_job_postings"
    staging_table = f"staging_{target_table}"

    # 1. Safety: Drop staging if it exists from a previous failed run
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

    # 2. Push to Staging with Batching (Fast)
    try:
        df.to_sql(
            staging_table,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        print('[sync_fact_job_postings] Successfully created staging table')

    except Exception as e:
        logging.error(f"[sync_fact_job_postings] Error in creating staging table, error detail: {e}")
        return

    # 3. Native MERGE (Handling Updates & Inserts)
    try:
        merge_sql = text(f"""
            MERGE INTO {target_table} AS target
            USING {staging_table} AS source
            ON target.job_id = source.job_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.is_expired = source.is_expired,
                    target.last_seen_id = source.last_seen_id
            WHEN NOT MATCHED THEN
                INSERT (job_id, job_title, is_expired, job_link, last_seen_id, location_id, label_id, emp_id, created_on_id)
                VALUES (source.job_id, source.job_title, source.is_expired, source.job_link, source.last_seen_id,
                        source.location_id, source.label_id, source.emp_id, source.created_on_id)
        """)

        with engine.begin() as conn:
            conn.execute(merge_sql)
            conn.execute(text(f"DROP TABLE {staging_table}"))

        print(f"[sync_fact_job_postings] Successfully synced {target_table}")

    except Exception as e:
        logging.error(f'[sync_fact_job_postings] Error in merging with the main table, error detail: {e}')
        return

def sync_fact_skill_fast(df: pd.DataFrame, engine)-> None:
    """Update fact_skill table in database
    Args:
      df (dataframe): dataframe of job information
      engine (sqlalchemy object): engine to connect and interact with database

    Returns:
      None
    """
    if df.empty:
        return

    target_table = "fact_skill"
    staging_table = f"staging_{target_table}"


    # 1. Clear staging for safety
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

    # 2. Bulk upload to staging
    try:
        df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')
        print('[sync_fact_skill_fast] Success updating staging table')
    except Exception as e:
        logging.error(f"[sync_fact_skill_fast] Error when updating staging table, error detail: {e}")
        return
    # 3. Fast "Not Exists" Insert
    # This query only inserts rows where the combination of skill_id AND job_id doesn't exist yet
    try:
        insert_sql = text(f"""
            INSERT INTO {target_table} (skill_id, job_id)
            SELECT s.skill_id, s.job_id
            FROM {staging_table} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {target_table} t
                WHERE t.skill_id = s.skill_id
                AND t.job_id = s.job_id
            )
        """)

        with engine.begin() as conn:
            conn.execute(insert_sql)
            conn.execute(text(f"DROP TABLE {staging_table}"))

        print(f"[sync_fact_skill_fast] Successfully synced {target_table}")
    except Exception as e:
        logging.error(f"[sync_fact_skill_fast] Error in updating main database, error detail: {e}")
        return

def databricks_hybrid_upsert(df: pd.DataFrame, target_table: str, unique_key: str, columns_to_update: List, engine) -> None:
    """Update dim tables using upsert logic into databricks
    Args:
      df (dataframe): The pandas DataFrame (e.g., location_dim_df)
      target_table (str): The table name in Databricks
      unique_key (str): The column to match on (e.g., 'location_name')
      columns_to_update (list): List of columns to update if match found (excluding the ID)
    """
    if df.empty or len(columns_to_update) < 0:
        raise ValueError('dataframe is empty or list of updated column is emtpy')

    staging_table = f"staging_{target_table}"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

    # 1. Push to Staging
    try:
        df.to_sql(staging_table, con=engine, if_exists='replace', index=False, method='multi')
        print(f"[databricks_hybrid_upsert] Success updating {staging_table}")
    except Exception as e:
        logging.error(f"[databricks_hybrid_upsert] Failed to update staging table, error detail: {e}")
        return
    # 2. Build the MERGE query
    # Logic: Match on unique_key. If exists, update metadata. If not, insert.
    all_cols = set(columns_to_update + [unique_key])
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"source.{col}" for col in all_cols])

    try:
        # Check if the only column we are "updating" is the unique key itself
        if len(columns_to_update) == 1 and columns_to_update[0] == unique_key:
            # INSERT ONLY (No UPDATE needed since the values are identical)
            merge_sql = text(f"""
                MERGE INTO {target_table} AS target
                USING {staging_table} AS source
                ON target.{unique_key} = source.{unique_key}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
        else:
            # STANDARD UPSERT
            update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns_to_update])
            merge_sql = text(f"""
                MERGE INTO {target_table} AS target
                USING {staging_table} AS source
                ON target.{unique_key} = source.{unique_key}
                WHEN MATCHED THEN
                    UPDATE SET {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals})
            """)

        with engine.begin() as conn:
            conn.execute(merge_sql)
            conn.execute(text(f"DROP TABLE {staging_table}"))

        print(f"[databricks_hybrid_upsert] Successfully synced {target_table}")
    except Exception as e:
        logging.error(f"[databricks_hybrid_upsert] Fail to update main databse, error detail: {e}")
        return

