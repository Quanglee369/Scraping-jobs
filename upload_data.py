import os
import logging
import re
import pandas as pd
from datetime import datetime
import numpy as np
from master_config import province_map
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import text, create_engine
from sklearn.metrics.pairwise import cosine_similarity
from clean_data_functions import prep_data_dim, fast_remove_accents, location_norm, databricks_hybrid_upsert, sync_fact_job_postings, sync_fact_skill_fast, universal_date_cleaner

# ==========================================
# SECTION 1: SETUP & DATA LOADING
# ==========================================
# Configure logging
logging.basicConfig(
    level = logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_debug.log", mode='a'),
        logging.StreamHandler()
    ],
    force = True
)

# Check if master data exist, if not create dummy dataframe to avoid error in the next step of data cleaning
master_path = os.path.exists('df_master.csv')
skill_path = os.path.exists('df_skills.csv')

if master_path and skill_path:
    df_master = pd.read_csv('df_master.csv')
    df_skills = pd.read_csv('df_skills.csv')
    print('[check_master_data] df_master.csv, df_skills.csv exist, begin to check for duplicate data')

elif not skill_path and master_path:
    df_master = pd.read_csv('df_master.csv')
    df_skills = pd.DataFrame(columns=['job_id', 'skill_raw'])
    logging.error('[check_skill_data] df_skills.csv does not exist, create dummy dataframe')
        
else:
    df_master = pd.DataFrame(columns=['job_id', 'job_title', 'date_view', 'emp_raw', 'location_name', 'label_name', 'job_link'])
    df_skills = pd.DataFrame(columns=['job_id', 'skill_raw'])
    print('[check_master_data] df_master.csv and df_skills.csv does not exist, create dummy dataframe for both')


# ==========================================
# SECTION 2: DEDUPLICATION (Cosine Similarity)
# ==========================================
# Combination of jobtitle and company name (emp_name)
compare_text = (df_master['job_title'].str.lower().str.replace(' ', '') + " " +
df_master['emp_raw'].str.lower().str.replace(' ', ''))

# Check for cosine similarity to filter out if one company post the same job in different websites
vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3,3))
tfidf_matrix = vectorizer.fit_transform(compare_text)
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# Filter out if the combination of job title and company name has similarity higher than 0.7
key = set()
drop_index = set()
for i in range(len(cosine_sim)):
  for u in np.where(cosine_sim[i] > 0.75)[0]:
    if i != u and u not in key: # Only add if the duplicate value not in the base case
      key.add(int(i))
      drop_index.add(int(u))

# Drop duplicate jobs
df_master.drop(index = list(drop_index), axis = 0, inplace= True)

job_id_master = list(df_master['job_id'])
df_skills = df_skills[df_skills['job_id'].isin(job_id_master)]
df_skills['skill_raw'] = df_skills['skill_raw'].str.strip()


# ==========================================
# SECTION 3: DATA NORMALIZATION
# ==========================================
# Convert date into ISO8601 format
df_master['date_view'] = (
    pd.to_datetime(df_master.apply(lambda row: universal_date_cleaner(date_val = row['date_view'], job_link = row['job_link']), axis=1), utc=True)
    .dt.tz_convert('Asia/Ho_Chi_Minh')
    .dt.tz_localize(None)
)
df_master['date_view'] = df_master['date_view'].dt.date

# Standardize location name
all_location = [loc.replace(' ', r'\s?') for loc in province_map]
pattern = re.compile('|'.join(all_location), re.IGNORECASE)

df_master['location_name'] = (df_master['location_name'].apply(lambda x: location_norm(x, pattern = pattern)))

# Create additional column to check if last_seen not equal today then the job is expired -> set is_expire to True
df_master['last_seen'] = datetime.today().date()
df_master['is_expired'] = False
df_master['job_link'] = df_master['job_link'].apply(lambda x: str(x).replace('https://careerviet.vn/en', 'https://careerviet.vn/vi'))
df_master.dropna(subset = ['job_link'], inplace=True)
df_skills = df_skills[df_skills['job_id'].isin(list(df_master['job_id']))]

# ==========================================
# SECTION 4: DIMENSIONAL PREP & DB CONNECTION
# ==========================================
# Explode and seperate skills data for storage efficiency
# Initiate sessions and connect to supabase database
url = os.getenv('DATABRICKS_DB_URL')
engine = create_engine(url, connect_args={"base_parameters": {"query_timeout": 30}})

# Prepare data to input into databrick
list_of_dim_cols = ['location_name', 'date_view', 'last_seen', 'label_name', 'emp_raw']
results = prep_data_dim(data=df_master, collist=list_of_dim_cols)
location_name = results[0]
date_view = results[1]
last_seen = results[2]
label = results[3]
emp_name = results[4]

list_of_dim_cols_skills = ['skill_raw']
result_skills = prep_data_dim(data=df_skills, collist=list_of_dim_cols_skills)
skills = result_skills[0]

# Test the connection
try:
    engine.connect()
    print('[testing_database_connection] Connection Successful')
except Exception as e:
    print(f'[testing_database_connection] Error when connecting to the database {e}')


# ==========================================
# SECTION 5: DIMENSION UPSERTS
# ==========================================
# Create list of tasks to update dimensional data into the tables
databricks_hybrid_upsert(df = location_name, target_table='location_dim', unique_key='location_name', columns_to_update=['location_name'], engine= engine)
databricks_hybrid_upsert(df = date_view, target_table='date_dim', unique_key='actual_date', columns_to_update=['actual_date'], engine= engine)
databricks_hybrid_upsert(df = last_seen, target_table='date_dim', unique_key='actual_date', columns_to_update=['actual_date'], engine= engine)
databricks_hybrid_upsert(df = label, target_table='label_dim', unique_key='label_name', columns_to_update=['label_name'], engine= engine)
databricks_hybrid_upsert(df = emp_name, target_table='emp_dim', unique_key='emp_raw', columns_to_update=['emp_raw'], engine= engine)
databricks_hybrid_upsert(df = skills, target_table='skill_dim', unique_key='skill_raw', columns_to_update=['skill_raw'], engine= engine)


# ==========================================
# SECTION 6: FACT TABLE MAPPING & SYNC
# ==========================================
# Prepare data for mapping and get ids from the newly updated dimensional tables
date_dim_dict = pd.read_sql_query("Select * from date_dim", engine)
emp_dim_dict = pd.read_sql_query("Select * from emp_dim", engine)
location_dim_dict = pd.read_sql_query("Select * from location_dim", engine)
label_dim_dict = pd.read_sql_query("Select * from label_dim", engine)
skill_dim_dict = pd.read_sql_query("Select * from skill_dim", engine)
fact_job_dict = pd.read_sql_query("Select * from fact_job_postings", engine)
unique_exist_id = set(list(fact_job_dict['job_id']))

# Map the data for both fact_job_posting and fact_skill table
update_fact_job_postings = (df_master
    .merge(date_dim_dict, how='left', left_on='date_view', right_on='actual_date')
    .drop(columns=['actual_date'])
    .rename(columns= {'date_id': 'created_on_id'})
    .merge(emp_dim_dict, how='left', on='emp_raw')
    .merge(location_dim_dict, how='left', on='location_name')
    .merge(label_dim_dict, how='left', on = 'label_name')
    .merge(date_dim_dict, left_on='last_seen', right_on='actual_date')
    .rename(columns={'date_id': 'last_seen_id'})
    # Drop all the 'raw' columns used for joining in one go
    .drop(columns=[
        'date_view', 'actual_date', 'last_seen','emp_raw',
        'emp_cleaned', 'location_name', 'label_name'
    ])
)

update_fact_skill = (df_skills
                .merge(skill_dim_dict, on = 'skill_raw', how='left')
                .drop(columns=['skill_cleaned', 'skill_raw'])
                .reset_index(drop=True)
)

update_fact_skill = update_fact_skill[~update_fact_skill['job_id'].isin(unique_exist_id)]

# Check column types - look for 'float64' where it should be 'int'
logging.warning(f"DATAFRAME TYPES:\n{update_fact_job_postings.dtypes}")

# Check for NaNs in label_id (can be due to exceed AI limit) that turn Integers into Floats, fill nan with 0
if update_fact_job_postings['label_id'].isnull().any():
  nan_count = update_fact_job_postings['label_id'].isnull().sum()
  logging.error(f"⚠️ Column label_id contains {nan_count} NaN values! Proceed to fill na")

update_fact_job_postings['label_id'] = update_fact_job_postings['label_id'].fillna(5).astype(int)

# DEFENSIVE DROP: Prevent Databricks crash if any skills fail to map
if update_fact_skill['skill_id'].isnull().any():
    nan_skills = update_fact_skill['skill_id'].isnull().sum()
    logging.warning(f"⚠️ Dropping {nan_skills} unmapped skills to prevent Databricks crash.")
    update_fact_skill = update_fact_skill.dropna(subset=['skill_id']).reset_index(drop=True)

# Force Integer type to match the Databricks schema
update_fact_skill['skill_id'] = update_fact_skill['skill_id'].astype(int)

# Begin to update fact_job_postings table
sync_fact_job_postings(update_fact_job_postings, engine= engine)

# Begin to update fact_skill table
sync_fact_skill_fast(update_fact_skill, engine= engine)