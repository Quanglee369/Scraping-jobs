import re
import logging
import copy
import ast

# [FUNCTION] Filter out relevant job header, as the primary focus is data related job (data engineer, data scientist and dat analyst)
def filter_relevant(data, platform: str):

  # Define keywords must be present in the job title 'mh_words' and keywords should be present 'en_keywords' + 'vn_keywords'
  en_keywords = ['intelligence', 'bi', 'developer','head','insights', 'processing', 'mining', 'reporting', 'modeling', 'model','expert', 'computer vision', 'analyst', 'analytics', 'analyse', 'engineering','engineer', 'database', 'governance', 'administrator', 'science', 'scientist', 'architect']
  vn_keywords = ['xử lý', 'khai thác', 'thống kê', 'quản trị', 'quản lý', 'kỹ thuật', 'thị giác máy tính', 'khoa học']
  mh_words = ['data', 'phân tích', 'database', 'dữ liệu', 'ai', 'computer vision', 'thị giác máy tính', 'modeling', 'model', 'sql', 'intelligence']

  # Also define keywords that should not be present
  nega_keyword = ['entry', 'kiểm toán','auditor','security', 'bảo mật', 'ib', 'sale', 'sales', 'full stack', 'backend', 'frontend', 'back-end', 'front-end', 'full-stack', 'web', 'software', 'soft ware', 'business']

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
    raise ValueError('[filter_relevant] Data invalid !')


  job_title = keys_for_platforms.get(clean_platform)[0]
  job_id = keys_for_platforms.get(clean_platform)[1]

  # Compile the regular expression (regex) pattern for higher efficiency in the loop
  all_keywords = en_keywords + vn_keywords
  pos_pattern = re.compile(r'\b(' + '|'.join(all_keywords) + r')\b', re.IGNORECASE)
  neg_pattern = re.compile(r'\b(' + '|'.join(nega_keyword) + r')\b', re.IGNORECASE)
  mh_pattern = re.compile(r'\b(' + '|'.join(mh_words) + r')\b', re.IGNORECASE)

  # Processing data that match the predefined pattern, list 'filtered_job' is for data analysis while 'input_ai' is used to feed to AI for relabeling
  filtered_job = []
  input_ai = []

  try:
    filtered_job =[
        item for item in data
        if mh_pattern.search(item.get(job_title, '')) and
        pos_pattern.search(item.get(job_title, '')) and
        not neg_pattern.search(item.get(job_title, ''))
    ]

    return filtered_job

  except Exception as e:
    logging.error(f'[filter_relevant] Unable to clean data and prepare for AI input, error: {e}')
    return [], []

# [FUNCTION] Remove duplicated job based on id as job can be duplicated when searching for closely related position (example: Data Analysis might have duplicated job with Data Scientist)
# Also label explicit title (title contain keywords like 'Data Analyst', ...)
def remove_duplicate(job, platform: str):
  # Each platform have a different way of naming key values so createing a dict to mapping based on platform of choice
  seen_id = set()
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
    logging.error('[remove_duplicate] No job data to remove duplicate !')
    return [], []
  cleaned = []

  job_title = keys_for_platforms.get(clean_platform)[0]
  job_id = keys_for_platforms.get(clean_platform)[1]

  pattern = re.compile('|'.join(re.escape(k) for k in label_mapping.keys()))

  # Loop through each job, only return the one that is not yet exist in the list
  # Also label job with explicit keyword to reduce AI payload
  for i in job:
    job_id_inner = i.get(job_id)
    if not job_id_inner or job_id_inner in seen_id:
      continue

    seen_id.add(job_id_inner)

    match_label = pattern.search(i.get(job_title).lower().replace(' ', ''))

    if match_label:
      i['label'] = label_mapping[match_label.group(0)]
    else:
      i['label'] = ''
    cleaned.append(i)

  input_ai = [{'job_id': i.get(job_id), 'job_title': i.get(job_title)} for i in cleaned if i.get('label') == '']
  return cleaned, input_ai

# [FUNCTION] Function to extract skills from JD
def extract_skills_from_jd(job_list):
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
  for job_entry in job_list:
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
  keys_for_platforms = {
      'careerviet': ['job_title', 'job_id'],
      'vietnamworks': ['jobTitle', 'jobId'],
      'itviec': ['job_title', 'job_id']
  }

  clean_platform = platform.replace(' ', '').lower()
  if clean_platform not in keys_for_platforms.keys():
    return print(f'[fill_label] platform is not in the list : {keys_for_platforms.keys()}')

  if not data or not label_data:
    logging.error('[fill_label] data invalid')
    return []

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
  if data.empty or not collist or len(collist) == 0:
    logging.error('[prep_data_dim] Input data or columns list is not valid')
    return None
  results = []

  for i in collist:
    unique_vals = data[i].dropna().unique()
    if i in ['date_view', 'last_seen']:
      results.append([{'actual_date': x} for x in unique_vals])
    else:
      results.append([{i: x} for x in unique_vals if x.strip() != '' and x.lower() != 'nan'])
  return results

# [FUNCTION] Convert the string representation back to a Python list
def safe_literal_eval(val):
    try:
        if isinstance(val, str) and val.startswith('['):
            return ast.literal_eval(val)
        return val
    except:
        return []

