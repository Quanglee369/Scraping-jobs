import re
import logging
import copy
import ast
import pandas as pd
from sqlalchemy import text

# [FUNCTION] Filter out relevant job header, as the primary focus is data related job (data engineer, data scientist and dat analyst)
def filter_relevant(data, platform: str):
  """Remove none-relevant jobs
  Args:
    data (list of dicts): List of dict with jobs information
    platform (str): name of the platform, must exist in this list (careerviet, vietnamworks, itviec)
  
  Returns:
    list of dicts: The filtered list 
  """

  # Define keywords must be present in the job title 'mh_words' and keywords should be present 'en_keywords' + 'vn_keywords'
  en_keywords = ['intelligence', 'bi', 'developer','head','insights', 'processing', 'mining', 'reporting', 'modeling', 'model','expert', 'computer vision', 'analyst', 'analytics', 'analyse', 'engineering','engineer', 'database', 'governance', 'administrator', 'science', 'scientist', 'architect']
  vn_keywords = ['xử lý', 'khai thác', 'thống kê', 'quản trị', 'quản lý', 'kỹ thuật', 'thị giác máy tính', 'khoa học']
  mh_words = ['data', 'phân tích', 'database', 'dữ liệu', 'ai', 'computer vision', 'thị giác máy tính', 'modeling', 'model', 'sql', 'intelligence']

  # Also define keywords that should not be present
  nega_keyword = ['entry', 'kiểm toán','auditor', 'tester', 'qc','security', 'bảo mật', 'ib', 'sale', 'sales', 'full stack', 'backend', 'frontend', 'java engineer', 'spring boot', 'back-end', 'front-end', 'full-stack', 'web', 'software', 'android', 'ios', 'mobile', 'qa', 'business']

  # Each platform have a different way of naming key values so createing a dict to mapping based on platform of choice
  keys_for_platforms = {
      'careerviet': ['job_title', 'job_id'],
      'vietnamworks': ['jobTitle', 'jobId'],
      'itviec': ['job_title', 'job_id']
  }

  # Check if platform is valid and exist in the predefined list
  clean_platform = platform.strip().replace(' ', '').lower()

  if clean_platform not in keys_for_platforms.keys():
    return logging.error(f'Platform: {clean_platform} is not in the list: {keys_for_platforms.keys()}')

  # Check if data is valid
  if not data:
    logging.error('[filter_relevant] Data invalid !')

  job_title = keys_for_platforms.get(clean_platform)[0]

  # Compile the regular expression (regex) pattern for higher efficiency in the loop
  all_keywords = en_keywords + vn_keywords
  pos_pattern = re.compile(r'\b(' + '|'.join(all_keywords) + r')\b', re.IGNORECASE)
  neg_pattern = re.compile(r'\b(' + '|'.join(nega_keyword) + r')\b', re.IGNORECASE)
  mh_pattern = re.compile(r'\b(' + '|'.join(mh_words) + r')\b', re.IGNORECASE)

  # Processing data that match the predefined pattern
  filtered_job = []
 
  try:
    filtered_job =[
        item for item in data
        if mh_pattern.search(item.get(job_title, '')) and
        pos_pattern.search(item.get(job_title, '')) and
        not neg_pattern.search(item.get(job_title, ''))
    ]

    return filtered_job

  except Exception as e:
    logging.error(f'[filter_relevant] Unable to clean data and prepare for AI input, error detail: {e}')
    return [], []

# [FUNCTION] Remove duplicated job based on id as job can be duplicated when searching for closely related position (example: Data Analysis might have duplicated job with Data Scientist)
# Also label explicit title (title contain keywords like 'Data Analyst', ...)

def remove_duplicate(job, platform: str, exist_job_id):
  """ Remove duplicate jobs
  args:
    job (list of dicts): list of dict with jobs information
    platform (str): name of the platform, must exist in this list (careerviet, vietnamworks, itviec)
    exist_job_id (dataframe): dataframne of job_id already exist in the database

  returns:
    cleaned (list of dict): list of dict with filtered jobs information
    input_ai (list of dict): list of dict with job_id and job_title for AI labeling
  """

  # Create a set of seen_id to remove the duplicates currently in the data
  # Create a set of exist_id to remove the duplicate when compare with the database, use to reduce AI model payload
  seen_id = set()
  exist_id = set(exist_job_id['job_id'].astype(str)) if exist_job_id is not None and not exist_job_id.empty else set()

  keys_for_platforms = {
      'careerviet': ['job_title', 'job_id'],
      'vietnamworks': ['jobTitle', 'jobId'],
      'itviec': ['job_title', 'job_id']
  }

  # Intialize dict for labeling to reduce AI payload
  label_mapping = {
    "dataanalyst": "Data Analyst",
    "dataengineer": "Data Engineer",
    "datascientist": "Data Scientist",
    "aiengineer": "Data Scientist",
    "mlengineer": "Data Scientist"
  }

  # Check if platform is valid and exist in the predefined list
  clean_platform = platform.strip().replace(' ', '').lower()

  if clean_platform not in keys_for_platforms.keys():
    return logging.error(f'[remove_duplicate] platform: {clean_platform} is not in the list: {keys_for_platforms.keys()}')

  # Check if data is valid
  if not job:
    logging.error('[remove_duplicate] Invalid job data')
    return [], []
  cleaned = []

  job_title = keys_for_platforms.get(clean_platform)[0]
  job_id = keys_for_platforms.get(clean_platform)[1]

  pattern = re.compile('|'.join(re.escape(k) for k in label_mapping.keys()))

  # Loop through each job, only return the one that is not yet exist in the list

  for i in job:
    raw_id = i.get(job_id)
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

# [FUNCTION] Function to extract skills from JD
def extract_skills_from_jd(job_list):
  """Extract skills from Job Descriptions
  Args:
    job_list (list of dict): list of dict with Job Id and Job Descriptions to extract skills

  Returns:
    results (list of dict): list of dict with Job Id and skill names
  """
  # Define all keywords for skills need to be matched
  all_skills = [
      # Programming & Scripting
      "Python", "SQL", "R", "Java", "Scala", "C++", "C#", "VBA", "JavaScript",
      "TypeScript", "HTML", "CSS", "Bash", "Shell Scripting", "Go", "Julia", "SAS", "MATLAB",

      # Databases
      "MySQL", "PostgreSQL", "Oracle Database", "Microsoft SQL Server", "MongoDB",
      "Redis", "Cassandra", "DynamoDB", "MariaDB", "DB2", "Netezza", "Elasticsearch",
      "Neo4j", "ClickHouse", "Vector Databases",

      # Cloud & Infrastructure
      "AWS", "Microsoft Azure", "GCP", "S3", "EC2", "Lambda", "Glue", "Athena",
      "Redshift", "Kinesis", "Azure Data Factory", "Azure Synapse Analytics",
      "Azure Data Lake", "Azure Blob Storage", "Google BigQuery",
      "Google Cloud Storage", "Firebase",

      # Big Data & Frameworks
      "Apache Spark", "PySpark", "Hadoop", "HDFS", "MapReduce", "Apache Kafka",
      "Apache Flink", "Apache Storm", "Hive", "Presto", "Trino", "Databricks",
      "Delta Lake", "Snowflake",

      # ETL & Orchestration Tools
      "Apache Airflow", "dbt", "SSIS", "Talend", "Informatica", "Pentaho",
      "Oracle Data Integrator", "ODI", "Luigi", "Prefect", "Nifi",

      # BI & Visualization Tools
      "Power BI", "Tableau", "QlikView", "QlikSense", "Looker", "Google Looker Studio",
      "Metabase", "Superset", "Excel", "Power Query", "DAX", "Cognos", "MicroStrategy",
      "SAP Analytics Cloud", "Grafana", "Kibana",

      # AI, ML & NLP Libraries
      "Scikit-learn", "TensorFlow", "PyTorch", "Keras", "XGBoost", "LightGBM",
      "CatBoost", "Fastai", "Hugging Face", "Transformers", "OpenCV", "LangChain",
      "LlamaIndex", "BERT", "GPT", "Computer Vision"

      # Deployment & DevOps
      "MLflow", "Kubeflow", "Triton Inference Server", "TensorFlow Serving",
      "TorchServe", "RayServe", "Docker", "Kubernetes", "Jenkins", "GitLab CI",
      "GitHub Actions", "Terraform", "Ansible",

      # Technical Protocols
      "Rest API", "GraphQL"
  ]

  # Descending sort order so this prioritize longer word over shorter one (example: Power BI -  matching Power first before looking at BI)
  all_skills.sort(key=len, reverse=True)
  pattern = re.compile(r'\b(' + '|'.join(map(re.escape, all_skills)) + r')\b', re.IGNORECASE)

  results = []
  if not job_list or not isinstance(job_list,list):
    return results
    
  for job_entry in job_list:
      if not job_entry:
        return results
      for job_id, jd in job_entry.items():
           if not jd:
              results.append({'job_id': job_id, 'skills': []})
              continue

           # Find all matching key word in jd
           found_skills = pattern.findall(jd)

           # Standardize by removing duplicate and normalizing the words
           unique_skills = sorted(list(set([s.strip() for s in found_skills])))

           results.append({'job_id': job_id, 'skills': unique_skills})

  return results

# [FUNCTION] Fill in the label result from AI
def fill_label(data, label_data, platform: str):
  """Fill label results from AI output
  args:
    data (list of dict): list of dict with job information
    label_data (list of dict): list of dict with job id and job label result from AI
    platform (str): name of the platform, must exist in this list (careerviet, vietnamworks, itviec)
  returns:
    temp_date (list of dict): list of dict with job information (labeled)
  """
  keys_for_platforms = {
      'careerviet': ['job_title', 'job_id'],
      'vietnamworks': ['jobTitle', 'jobId'],
      'itviec': ['job_title', 'job_id']
  }

  clean_platform = platform.replace(' ', '').lower()
  
  if clean_platform not in keys_for_platforms.keys():
    logging.error(f'[fill_label] platform is not in the list : {keys_for_platforms.keys()}')
    return []
  
  if not data:
    logging.warning(f'[fill_label] data invalid for {clean_platform}')
    return []
    
  if not label_data:
    logging.warning(f'[fill_label] label data invalid for {clean_platform} proceed with blank label')
    label_data = {}

  job_id = keys_for_platforms.get(clean_platform)[1]

  temp_data = copy.deepcopy(data)
  for i in temp_data:
    if i.get(job_id) in label_data:
      i['label'] = label_data.get(i.get(job_id))
    elif i.get('label') == '':
      i['label'] = 'None'
  return temp_data

# [FUNCTION] Remove accents, convert Vietnamese into ASCII format
def fast_remove_accents(text):
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

# [FUNCTION] Prepare data as a list of dict with unique value to update dim tables
def prep_data_dim(data, collist):
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

# [FUNCTION] Convert the string representation back to a Python list
def safe_literal_eval(val):
    try:
        if isinstance(val, str) and val.startswith('['):
            return ast.literal_eval(val)
        return val
    except:
        return []
    
def sync_fact_job_postings(df, engine):
    """Update fact_job_postings table in database
    args:
      df (dataframe): dataframe of job information
      engine (sqlalchemy object): engine to connect and interact with database
    returns:
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

def sync_fact_skill_fast(df, engine):
    """Update fact_skill table in database
    args:
      df (dataframe): dataframe of job information
      engine (sqlalchemy object): engine to connect and interact with database
    returns:
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

def databricks_hybrid_upsert(df, target_table, unique_key, columns_to_update, engine):
    """
    df: The pandas DataFrame (e.g., location_dim_df)
    target_table: The table name in Databricks
    unique_key: The column to match on (e.g., 'location_name')
    columns_to_update: List of columns to update if match found (excluding the ID)
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

